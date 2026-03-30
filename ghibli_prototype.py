"""
Ghibli Art Generator — AWS Prototype (moto)

v4: S3 + DynamoDB + SQS + SNS — Pub-Sub Notifications

In v3, processing completes silently — Priya has no idea her art is ready.
She'd have to keep refreshing the app. SNS solves this: after the worker
finishes, it publishes ONE message to an SNS topic. SNS then fans out
to all subscribers — email, SMS, push notifications — simultaneously.

KEY DIFFERENCE: SQS vs SNS
  - SQS = point-to-point. One consumer gets each message. Used for work
    distribution (e.g., job queue).
  - SNS = pub-sub broadcast. ALL subscribers get every message. Used for
    notifications (e.g., "art is ready!").
  They solve different problems and are often used together.

New in this version:
  - SNS topic 'ghibli-notifications' with email subscription
  - Worker publishes completion notification via SNS
  - Before/after verification on SNS subscriptions and publishes

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
TOPIC_NAME = "ghibli-notifications"
USER_EMAIL = "priya@example.com"
FILENAME = "priya-selfie.jpg"
FAKE_IMAGE = b"\xff\xd8\xff\xe0" + b"\x00" * 1000  # Fake JPEG header + padding


def banner(step: int, title: str):
    """Print a visible step header."""
    print(f"\n{'=' * 60}")
    print(f"STEP {step}: {title}")
    print(f"{'=' * 60}")


def show_bucket_contents(s3, bucket: str, label: str):
    """List all objects in a bucket."""
    response = s3.list_objects_v2(Bucket=bucket)
    objects = response.get("Contents", [])
    if not objects:
        print(f"  [{label}] Bucket '{bucket}': (empty)")
    else:
        print(f"  [{label}] Bucket '{bucket}':")
        for obj in objects:
            print(f"    - {obj['Key']}  ({obj['Size']} bytes)")


def show_job_record(dynamo, job_id: str, label: str):
    """Fetch and display a DynamoDB job record."""
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
    """Show how many messages are waiting in the queue."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = attrs["ApproximateNumberOfMessages"]
    in_flight = attrs["ApproximateNumberOfMessagesNotVisible"]
    print(f"  [{label}] Queue '{QUEUE_NAME}': {visible} waiting, {in_flight} in-flight")


def show_sns_subscriptions(sns, topic_arn: str, label: str):
    """List all subscriptions on an SNS topic."""
    subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    if not subs:
        print(f"  [{label}] SNS topic '{TOPIC_NAME}': (no subscribers)")
    else:
        print(f"  [{label}] SNS topic '{TOPIC_NAME}' subscribers:")
        for sub in subs:
            print(f"    - {sub['Protocol']}: {sub['Endpoint']}")


# ---------------------------------------------------------------------------
# STEP 1: Create Infrastructure — S3 + DynamoDB + SQS + SNS
# ---------------------------------------------------------------------------
def step1_create_infrastructure(s3, dynamo, sqs, sns) -> dict:
    banner(1, "INFRASTRUCTURE SETUP — S3 + DynamoDB + SQS + SNS")

    # Before
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    existing_queues = sqs.list_queues().get("QueueUrls", [])
    existing_topics = sns.list_topics()["Topics"]
    print(f"  [BEFORE] Buckets: {[b['Name'] for b in existing_buckets] or '(none)'}")
    print(f"  [BEFORE] DynamoDB tables: {existing_tables or '(none)'}")
    print(f"  [BEFORE] SQS queues: {existing_queues or '(none)'}")
    print(f"  [BEFORE] SNS topics: {[t['TopicArn'].split(':')[-1] for t in existing_topics] or '(none)'}")

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

    # SQS queue
    response = sqs.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={"VisibilityTimeout": "60"},
    )
    queue_url = response["QueueUrl"]
    print(f"  [OK] Created SQS queue: {QUEUE_NAME} (visibility timeout: 60s)")

    # -----------------------------------------------------------------------
    # SNS Topic + Email Subscription
    # WHY SNS? After the worker finishes, we need to notify Priya. We could
    # send an email directly from the worker code, but that tightly couples
    # the worker to "email". What if we also want to send a push notification?
    # Or an SMS? Or trigger another Lambda?
    #
    # SNS decouples the "something happened" event from "who cares about it".
    # The worker publishes ONE message. SNS fans it out to ALL subscribers.
    # Adding a new notification channel = adding a new subscription, not
    # changing the worker code. This is the pub-sub pattern.
    #
    # In real AWS, subscribing an email endpoint sends a confirmation email
    # that the user must click. In moto, it's confirmed automatically.
    # -----------------------------------------------------------------------
    topic_response = sns.create_topic(Name=TOPIC_NAME)
    topic_arn = topic_response["TopicArn"]
    print(f"  [OK] Created SNS topic: {TOPIC_NAME}")
    print(f"       ARN: {topic_arn}")

    # Before subscription
    show_sns_subscriptions(sns, topic_arn, "BEFORE subscribe")

    sns.subscribe(
        TopicArn=topic_arn,
        Protocol="email",
        Endpoint=USER_EMAIL,
    )
    print(f"  [OK] SNS email subscription: {USER_EMAIL}")

    # After subscription
    show_sns_subscriptions(sns, topic_arn, "AFTER subscribe")

    # After — all infrastructure
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    existing_queues = sqs.list_queues().get("QueueUrls", [])
    existing_topics = sns.list_topics()["Topics"]
    print(f"  [AFTER] Buckets: {[b['Name'] for b in existing_buckets]}")
    print(f"  [AFTER] DynamoDB tables: {existing_tables}")
    print(f"  [AFTER] SQS queues: {[q.split('/')[-1] for q in existing_queues]}")
    print(f"  [AFTER] SNS topics: {[t['TopicArn'].split(':')[-1] for t in existing_topics]}")

    return {"queue_url": queue_url, "topic_arn": topic_arn}


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
def step4_s3_event_to_sqs(sqs, queue_url: str, job_id: str):
    banner(4, "S3 EVENT → SQS QUEUE")

    input_key = f"{job_id}/original.jpg"

    show_queue_depth(sqs, queue_url, "BEFORE")

    event_payload = {
        "job_id": job_id,
        "user_email": USER_EMAIL,
        "input_bucket": RAW_BUCKET,
        "input_key": input_key,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event_payload),
    )
    message_id = response["MessageId"]
    print(f"  [OK] SQS message sent")
    print(f"       Message ID: {message_id}")
    print(f"       Payload: {json.dumps(event_payload, indent=2)}")

    show_queue_depth(sqs, queue_url, "AFTER")


# ---------------------------------------------------------------------------
# STEP 5: Worker Polls SQS → Processes → Updates DynamoDB
# ---------------------------------------------------------------------------
def step5_worker_processes_job(s3, sqs, dynamo, queue_url: str, job_id: str) -> str:
    banner(5, "WORKER POLLS SQS AND PROCESSES JOB")

    now = datetime.now(timezone.utc).isoformat()

    show_queue_depth(sqs, queue_url, "BEFORE")
    show_job_record(dynamo, job_id, "BEFORE")

    # Worker polls SQS
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    messages = response.get("Messages", [])

    if not messages:
        print("  [ERROR] No messages in queue!")
        return ""

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

    # Delete the SQS message (acknowledge)
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print(f"  [OK] SQS message deleted (acknowledged)")

    show_queue_depth(sqs, queue_url, "AFTER")
    show_job_record(dynamo, job_id, "AFTER")

    return presigned_get_url


# ---------------------------------------------------------------------------
# STEP 6: SNS Notification to Priya
# ---------------------------------------------------------------------------
# The worker has finished. Now we publish to SNS. In a real system, the
# worker Lambda would do this as its last step. We separate it here for
# clarity — to show SNS as a distinct concept.
#
# One publish → all subscribers notified simultaneously. If we later add
# an SMS subscriber or a webhook, the worker code doesn't change at all.
# ---------------------------------------------------------------------------
def step6_sns_notification(sns, topic_arn: str, job_id: str, download_url: str):
    banner(6, "SNS NOTIFICATION TO PRIYA")

    notification_payload = {
        "message": "Your Ghibli art is ready!",
        "download_url": download_url[:80] + "...",
        "job_id": job_id,
    }

    print(f"  Publishing to SNS topic: {TOPIC_NAME}")
    print(f"  Payload:")
    print(f"    {json.dumps(notification_payload, indent=4)}")

    response = sns.publish(
        TopicArn=topic_arn,
        Subject="Your Ghibli art is ready!",
        Message=json.dumps(notification_payload),
    )
    message_id = response["MessageId"]
    print(f"\n  [OK] SNS message published")
    print(f"       Message ID: {message_id}")
    print(f"  [OK] Simulated email to {USER_EMAIL}:")
    print(f'       Subject: "Your Ghibli art is ready!"')
    print(f"       Body: Your Ghibli art is ready! Download: {download_url[:60]}...")


# ---------------------------------------------------------------------------
# STEP 7: Download Output
# ---------------------------------------------------------------------------
def step7_download_output(s3, job_id: str):
    banner(7, "PRIYA DOWNLOADS HER GHIBLI ART — Presigned GET URL")

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
# STEP 8: Verify End-to-End State
# ---------------------------------------------------------------------------
def step8_verify(s3, sqs, sns, dynamo, queue_url: str, topic_arn: str, job_id: str):
    banner(8, "VERIFY END-TO-END STATE")

    show_bucket_contents(s3, RAW_BUCKET, "FINAL")
    show_bucket_contents(s3, OUTPUT_BUCKET, "FINAL")
    print()
    show_queue_depth(sqs, queue_url, "FINAL")
    print()
    show_sns_subscriptions(sns, topic_arn, "FINAL")
    print()
    show_job_record(dynamo, job_id, "FINAL")

    print(f"\n  AWS Region: {REGION} (Mumbai)")
    print(f"  Services used: S3, DynamoDB, SQS, SNS")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")
    print(f"\n  SUCCESS — Priya's full notification journey complete!")


# ===========================================================================
# Main — all AWS calls inside a single @mock_aws context
# ===========================================================================
@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)
    dynamo = boto3.client("dynamodb", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    job_id = "a1b2c3d4-5678-9abc-def0-123456789abc"

    infra = step1_create_infrastructure(s3, dynamo, sqs, sns)
    queue_url = infra["queue_url"]
    topic_arn = infra["topic_arn"]

    step2_generate_presigned_put(s3, dynamo, job_id)
    step3_upload_selfie(s3, job_id)
    step4_s3_event_to_sqs(sqs, queue_url, job_id)
    presigned_get_url = step5_worker_processes_job(s3, sqs, dynamo, queue_url, job_id)
    step6_sns_notification(sns, topic_arn, job_id, presigned_get_url)
    step7_download_output(s3, job_id)
    step8_verify(s3, sqs, sns, dynamo, queue_url, topic_arn, job_id)


if __name__ == "__main__":
    run()
