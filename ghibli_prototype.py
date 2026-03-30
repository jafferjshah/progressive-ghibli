"""
Ghibli Art Generator — AWS Prototype (moto)

v3: S3 + DynamoDB + SQS — Decoupled Processing

In v2, the upload and processing happened in the same flow — one function
called the next directly. But what if 1000 Priyas upload selfies at once?
The worker can't keep up, and requests get dropped.

SQS solves this: the upload step puts a message on a queue, and the worker
polls at its own pace. Messages wait safely in the queue — nothing is lost.
This is the "decoupling" pattern.

New in this version:
  - SQS queue 'ghibli-jobs-queue' between upload and processing
  - S3 upload event simulated as SQS message
  - Worker polls SQS, processes, then deletes the message
  - Before/after queue depth verification at every step

Run with:
    poetry run python ghibli_prototype.py

No AWS credentials needed — everything is mocked via moto.
"""

from moto import mock_aws
import boto3
import json
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REGION = "ap-south-1"  # Mumbai — Priya's region, data stays close to users
RAW_BUCKET = "ghibli-raw-uploads"
OUTPUT_BUCKET = "ghibli-output"
JOBS_TABLE = "jobs-metadata"
QUEUE_NAME = "ghibli-jobs-queue"
USER_EMAIL = "priya@example.com"
FILENAME = "priya-selfie.jpg"
FAKE_IMAGE = b"\xff\xd8\xff\xe0" + b"\x00" * 1000  # Fake JPEG header + padding


def banner(step: int, title: str):
    """Print a visible step header."""
    print(f"\n{'=' * 60}")
    print(f"STEP {step}: {title}")
    print(f"{'=' * 60}")


def show_bucket_contents(s3, bucket: str, label: str):
    """List all objects in a bucket — used for before/after verification."""
    response = s3.list_objects_v2(Bucket=bucket)
    objects = response.get("Contents", [])
    if not objects:
        print(f"  [{label}] Bucket '{bucket}': (empty)")
    else:
        print(f"  [{label}] Bucket '{bucket}':")
        for obj in objects:
            print(f"    - {obj['Key']}  ({obj['Size']} bytes)")


def show_job_record(dynamo, job_id: str, label: str):
    """Fetch and display a DynamoDB job record — used for before/after verification."""
    response = dynamo.get_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
    )
    item = response.get("Item")
    if not item:
        print(f"  [{label}] DynamoDB job '{job_id}': (not found)")
    else:
        print(f"  [{label}] DynamoDB job '{job_id}':")
        for key, val in sorted(item.items()):
            actual = list(val.values())[0]
            print(f"    {key}: {actual}")


def show_queue_depth(sqs, queue_url: str, label: str):
    """Show how many messages are waiting in the queue — before/after verification."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = attrs["ApproximateNumberOfMessages"]
    in_flight = attrs["ApproximateNumberOfMessagesNotVisible"]
    print(f"  [{label}] Queue '{QUEUE_NAME}': {visible} waiting, {in_flight} in-flight")


# ---------------------------------------------------------------------------
# STEP 1: Create Infrastructure — S3 + DynamoDB + SQS
# ---------------------------------------------------------------------------
def step1_create_infrastructure(s3, dynamo, sqs) -> str:
    banner(1, "INFRASTRUCTURE SETUP — S3 + DynamoDB + SQS")

    # Before
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    existing_queues = sqs.list_queues().get("QueueUrls", [])
    print(f"  [BEFORE] Buckets: {[b['Name'] for b in existing_buckets] or '(none)'}")
    print(f"  [BEFORE] DynamoDB tables: {existing_tables or '(none)'}")
    print(f"  [BEFORE] SQS queues: {existing_queues or '(none)'}")

    # S3 buckets
    s3.create_bucket(
        Bucket=RAW_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"  [OK] Created bucket: {RAW_BUCKET} (STANDARD storage, {REGION})")

    s3.create_bucket(
        Bucket=OUTPUT_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"  [OK] Created bucket: {OUTPUT_BUCKET} (STANDARD_IA intent, {REGION})")

    # DynamoDB table
    dynamo.create_table(
        TableName=JOBS_TABLE,
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  [OK] Created DynamoDB table: {JOBS_TABLE} (PAY_PER_REQUEST)")

    # -----------------------------------------------------------------------
    # SQS Queue — Standard (not FIFO)
    # WHY SQS? The upload Lambda and the worker Lambda are decoupled via a
    # queue. If Priya's upload triggers work directly (Lambda → Lambda), a
    # burst of 1000 simultaneous uploads would overwhelm the worker. With
    # SQS, messages accumulate in the queue and workers process at their own
    # pace. This is the "decoupling" pattern — SQS absorbs the traffic spike.
    #
    # WHY Standard (not FIFO)? Order doesn't matter for image processing.
    # Priya doesn't care if her job runs before or after someone else's.
    # Standard queues have higher throughput and lower cost.
    #
    # Visibility timeout = 60s: once a worker picks up a message, it has 60
    # seconds to finish. If it crashes, the message reappears for another
    # worker to retry. This is how SQS provides at-least-once delivery.
    # -----------------------------------------------------------------------
    response = sqs.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "VisibilityTimeout": "60",
        },
    )
    queue_url = response["QueueUrl"]
    print(f"  [OK] Created SQS queue: {QUEUE_NAME}")
    print(f"       URL: {queue_url}")
    print(f"       Visibility timeout: 60s")

    # After
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    existing_queues = sqs.list_queues().get("QueueUrls", [])
    print(f"  [AFTER] Buckets: {[b['Name'] for b in existing_buckets]}")
    print(f"  [AFTER] DynamoDB tables: {existing_tables}")
    print(f"  [AFTER] SQS queues: {[q.split('/')[-1] for q in existing_queues]}")

    return queue_url


# ---------------------------------------------------------------------------
# STEP 2: Generate Presigned PUT URL + Create Job Record
# ---------------------------------------------------------------------------
def step2_generate_presigned_put(s3, dynamo, job_id: str) -> str:
    banner(2, "PRIYA OPENS THE APP — Presigned URL + Job Record")

    input_key = f"{job_id}/original.jpg"
    now = datetime.now(timezone.utc).isoformat()

    print(f"  Priya requests upload URL for: {FILENAME}")
    print(f"  job_id: {job_id}")

    show_job_record(dynamo, job_id, "BEFORE")

    presigned_put_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": RAW_BUCKET, "Key": input_key},
        ExpiresIn=300,
    )
    print(f"\n  [OK] Presigned PUT URL generated (expires in 300s)")
    print(f"       URL: {presigned_put_url[:80]}...")

    dynamo.put_item(
        TableName=JOBS_TABLE,
        Item={
            "job_id": {"S": job_id},
            "user_email": {"S": USER_EMAIL},
            "status": {"S": "PENDING"},
            "input_bucket": {"S": RAW_BUCKET},
            "input_key": {"S": input_key},
            "created_at": {"S": now},
        },
    )
    print(f"  [OK] DynamoDB PutItem: job_id={job_id}, status=PENDING")

    show_job_record(dynamo, job_id, "AFTER")

    return presigned_put_url


# ---------------------------------------------------------------------------
# STEP 3: Upload Selfie
# ---------------------------------------------------------------------------
def step3_upload_selfie(s3, job_id: str):
    banner(3, "PRIYA UPLOADS HER SELFIE — Browser → S3 Presigned PUT")

    input_key = f"{job_id}/original.jpg"

    show_bucket_contents(s3, RAW_BUCKET, "BEFORE")

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=input_key,
        Body=FAKE_IMAGE,
        ContentType="image/jpeg",
    )
    print(f"\n  [OK] Uploaded {len(FAKE_IMAGE)} bytes to {RAW_BUCKET}/{input_key}")

    show_bucket_contents(s3, RAW_BUCKET, "AFTER")

    head = s3.head_object(Bucket=RAW_BUCKET, Key=input_key)
    print(f"  [VERIFY] Content-Type: {head['ContentType']}")
    print(f"  [VERIFY] Content-Length: {head['ContentLength']}")
    print(f"  [VERIFY] ETag: {head['ETag']}")


# ---------------------------------------------------------------------------
# STEP 4: S3 Event → SQS Queue
# ---------------------------------------------------------------------------
# In real AWS, S3 can be configured to send an event notification to SQS
# whenever an object is created. We simulate this manually: after the upload,
# we construct the event payload and send it to the queue ourselves.
#
# KEY TEACHING POINT: SQS = point-to-point guaranteed delivery. One consumer
# gets each message. If the consumer fails, the message reappears after the
# visibility timeout. This is different from SNS (which we'll add in v4).
# ---------------------------------------------------------------------------
def step4_s3_event_to_sqs(sqs, queue_url: str, job_id: str):
    banner(4, "S3 EVENT → SQS QUEUE")

    input_key = f"{job_id}/original.jpg"

    # Before — queue should be empty
    show_queue_depth(sqs, queue_url, "BEFORE")

    # Construct the S3 event payload (this is what real S3 sends to SQS)
    event_payload = {
        "job_id": job_id,
        "user_email": USER_EMAIL,
        "input_bucket": RAW_BUCKET,
        "input_key": input_key,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Send message to SQS
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event_payload),
    )
    message_id = response["MessageId"]
    print(f"  [OK] SQS message sent")
    print(f"       Message ID: {message_id}")
    print(f"       Payload: {json.dumps(event_payload, indent=2)}")

    # After — queue should have 1 message
    show_queue_depth(sqs, queue_url, "AFTER")


# ---------------------------------------------------------------------------
# STEP 5: Worker Polls SQS → Processes → Deletes Message
# ---------------------------------------------------------------------------
# The worker is a separate Lambda that polls SQS on a schedule. It:
#   1. Receives a message (the job details)
#   2. Processes the image
#   3. Updates DynamoDB
#   4. Deletes the message (acknowledges successful processing)
#
# If the worker crashes before deleting the message, it reappears after
# the visibility timeout (60s) — this is SQS's retry mechanism.
# ---------------------------------------------------------------------------
def step5_worker_processes_job(s3, sqs, dynamo, queue_url: str, job_id: str):
    banner(5, "WORKER POLLS SQS AND PROCESSES JOB")

    now = datetime.now(timezone.utc).isoformat()

    show_queue_depth(sqs, queue_url, "BEFORE")
    show_job_record(dynamo, job_id, "BEFORE")

    # Worker polls SQS — receives up to 1 message
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
    )
    messages = response.get("Messages", [])

    if not messages:
        print("  [ERROR] No messages in queue!")
        return

    message = messages[0]
    receipt_handle = message["ReceiptHandle"]
    payload = json.loads(message["Body"])
    print(f"\n  [OK] Received SQS message:")
    print(f"       job_id: {payload['job_id']}")
    print(f"       input_key: {payload['input_key']}")

    # Download original from S3
    input_key = payload["input_key"]
    output_key = f"{job_id}/ghibli-art.jpg"
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=input_key)
    original_bytes = obj["Body"].read()
    print(f"  [OK] Downloaded original: {len(original_bytes)} bytes from {RAW_BUCKET}")

    # Simulate style transfer
    metadata = f"GHIBLI_PROCESSED::job_id={job_id}::timestamp={now}::".encode()
    processed_bytes = metadata + original_bytes
    print(f"  [OK] Style transfer applied: {len(original_bytes)} → {len(processed_bytes)} bytes")

    # Upload processed image
    show_bucket_contents(s3, OUTPUT_BUCKET, "BEFORE")
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=output_key,
        Body=processed_bytes,
        ContentType="image/jpeg",
    )
    print(f"\n  [OK] Uploaded processed image to {OUTPUT_BUCKET}/{output_key}")
    show_bucket_contents(s3, OUTPUT_BUCKET, "AFTER")

    # Generate presigned GET URL for the output
    presigned_get_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": output_key},
        ExpiresIn=3600,
    )

    # Update DynamoDB: PENDING → COMPLETE
    dynamo.update_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET #s = :s, output_key = :ok, output_url = :ou, completed_at = :ca",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": {"S": "COMPLETE"},
            ":ok": {"S": output_key},
            ":ou": {"S": presigned_get_url[:80] + "..."},
            ":ca": {"S": now},
        },
    )
    print(f"  [OK] DynamoDB UpdateItem: status=PENDING → COMPLETE")

    # -----------------------------------------------------------------------
    # Delete the SQS message — this is the "acknowledgement"
    # If we don't delete it, the message reappears after the visibility
    # timeout and another worker would re-process the job (duplicate work).
    # In production, you'd also want idempotency checks in the worker.
    # -----------------------------------------------------------------------
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print(f"  [OK] SQS message deleted (acknowledged)")

    # After — queue should be empty, job should be COMPLETE
    show_queue_depth(sqs, queue_url, "AFTER")
    show_job_record(dynamo, job_id, "AFTER")


# ---------------------------------------------------------------------------
# STEP 6: Download Output
# ---------------------------------------------------------------------------
def step6_download_output(s3, job_id: str):
    banner(6, "PRIYA DOWNLOADS HER GHIBLI ART — Presigned GET URL")

    output_key = f"{job_id}/ghibli-art.jpg"

    presigned_get_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": output_key},
        ExpiresIn=3600,
    )
    print(f"  [OK] Presigned GET URL generated (expires in 3600s)")
    print(f"       URL: {presigned_get_url[:80]}...")

    response = s3.get_object(Bucket=OUTPUT_BUCKET, Key=output_key)
    downloaded = response["Body"].read()
    print(f"\n  [OK] Downloaded: {len(downloaded)} bytes")
    print(f"  [VERIFY] Starts with GHIBLI_PROCESSED: {downloaded[:30]}...")


# ---------------------------------------------------------------------------
# STEP 7: Verify End-to-End State
# ---------------------------------------------------------------------------
def step7_verify(s3, sqs, dynamo, queue_url: str, job_id: str):
    banner(7, "VERIFY END-TO-END STATE")

    show_bucket_contents(s3, RAW_BUCKET, "FINAL")
    show_bucket_contents(s3, OUTPUT_BUCKET, "FINAL")
    print()
    show_queue_depth(sqs, queue_url, "FINAL")
    print()
    show_job_record(dynamo, job_id, "FINAL")

    print(f"\n  AWS Region: {REGION} (Mumbai)")
    print(f"  Services used: S3, DynamoDB, SQS")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")
    print(f"\n  SUCCESS — Priya's decoupled journey complete!")


# ===========================================================================
# Main — all AWS calls inside a single @mock_aws context
# ===========================================================================
@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)
    dynamo = boto3.client("dynamodb", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)

    job_id = "a1b2c3d4-5678-9abc-def0-123456789abc"

    queue_url = step1_create_infrastructure(s3, dynamo, sqs)
    step2_generate_presigned_put(s3, dynamo, job_id)
    step3_upload_selfie(s3, job_id)
    step4_s3_event_to_sqs(sqs, queue_url, job_id)
    step5_worker_processes_job(s3, sqs, dynamo, queue_url, job_id)
    step6_download_output(s3, job_id)
    step7_verify(s3, sqs, dynamo, queue_url, job_id)


if __name__ == "__main__":
    run()
