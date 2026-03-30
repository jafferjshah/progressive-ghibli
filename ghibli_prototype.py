"""
Ghibli Art Generator — AWS Prototype (moto)

v5: S3 + DynamoDB + SQS + SNS + IAM — Least Privilege

In v4, our Lambdas can do anything — there are no permissions boundaries.
In real AWS, a Lambda with no IAM restrictions could read every S3 bucket,
delete DynamoDB tables, or send messages to any queue. That's a security
nightmare.

IAM (Identity and Access Management) solves this with the PRINCIPLE OF
LEAST PRIVILEGE: each Lambda gets exactly the permissions it needs and
nothing more.

New in this version:
  - IAM role 'lambda-presign-role' — can only write to raw uploads + DynamoDB
  - IAM role 'lambda-worker-role' — can read raw, write output, update DB,
    poll SQS, publish SNS
  - Before/after verification on IAM roles and policies
  - Each role's policy printed to show exactly what's allowed

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

# IAM trust policy — allows Lambda service to assume the role
LAMBDA_TRUST_POLICY = json.dumps({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
})


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


def show_iam_roles(iam, label: str):
    """List IAM roles we created (filter to our lambda- prefix)."""
    roles = iam.list_roles()["Roles"]
    our_roles = [r for r in roles if r["RoleName"].startswith("lambda-")]
    if not our_roles:
        print(f"  [{label}] IAM roles (lambda-*): (none)")
    else:
        print(f"  [{label}] IAM roles (lambda-*):")
        for role in our_roles:
            print(f"    - {role['RoleName']}  (ARN: {role['Arn']})")


# ---------------------------------------------------------------------------
# STEP 1: Create Infrastructure — S3 + DynamoDB + SQS + SNS + IAM
# ---------------------------------------------------------------------------
def step1_create_infrastructure(s3, dynamo, sqs, sns, iam) -> dict:
    banner(1, "INFRASTRUCTURE SETUP — S3 + DynamoDB + SQS + SNS + IAM")

    # Before
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    existing_queues = sqs.list_queues().get("QueueUrls", [])
    existing_topics = sns.list_topics()["Topics"]
    print(f"  [BEFORE] Buckets: {[b['Name'] for b in existing_buckets] or '(none)'}")
    print(f"  [BEFORE] DynamoDB tables: {existing_tables or '(none)'}")
    print(f"  [BEFORE] SQS queues: {existing_queues or '(none)'}")
    print(f"  [BEFORE] SNS topics: {[t['TopicArn'].split(':')[-1] for t in existing_topics] or '(none)'}")
    show_iam_roles(iam, "BEFORE")

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

    # SNS topic + subscription
    topic_response = sns.create_topic(Name=TOPIC_NAME)
    topic_arn = topic_response["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=USER_EMAIL)
    print(f"  [OK] Created SNS topic: {TOPIC_NAME} + subscribed {USER_EMAIL}")

    # -----------------------------------------------------------------------
    # IAM Roles — Least Privilege
    #
    # WHY IAM? Every AWS service call requires permission. Without IAM roles,
    # you'd embed long-lived access keys in your code (terrible for security).
    # IAM roles let Lambda "assume" temporary credentials scoped to exactly
    # what it needs.
    #
    # PRINCIPLE OF LEAST PRIVILEGE: give each component the minimum
    # permissions required to do its job.
    #   - The presign Lambda only needs: PutObject on raw bucket + PutItem on DynamoDB
    #   - The worker Lambda needs more: read raw, write output, update DB, poll SQS, publish SNS
    #   - Neither Lambda can delete buckets, create tables, or access other accounts
    #
    # In real AWS, if a policy were missing, the call would fail with
    # AccessDeniedException. Moto doesn't enforce this, but we create the
    # roles and policies anyway to teach the pattern.
    # -----------------------------------------------------------------------

    # Role 1: lambda-presign-role
    presign_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject"],
                "Resource": f"arn:aws:s3:::{RAW_BUCKET}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["dynamodb:PutItem"],
                "Resource": f"arn:aws:dynamodb:*:*:table/{JOBS_TABLE}",
            },
        ],
    }
    iam.create_role(
        RoleName="lambda-presign-role",
        AssumeRolePolicyDocument=LAMBDA_TRUST_POLICY,
        Description="Presign Lambda — can upload to raw bucket and create job records",
    )
    iam.put_role_policy(
        RoleName="lambda-presign-role",
        PolicyName="presign-permissions",
        PolicyDocument=json.dumps(presign_policy),
    )
    print(f"  [OK] IAM role created: lambda-presign-role")
    print(f"       Permissions: s3:PutObject on {RAW_BUCKET}/*, dynamodb:PutItem on {JOBS_TABLE}")

    # Role 2: lambda-worker-role
    worker_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::{RAW_BUCKET}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject"],
                "Resource": f"arn:aws:s3:::{OUTPUT_BUCKET}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"],
                "Resource": f"arn:aws:dynamodb:*:*:table/{JOBS_TABLE}",
            },
            {
                "Effect": "Allow",
                "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
                "Resource": f"arn:aws:sqs:*:*:{QUEUE_NAME}",
            },
            {
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": f"arn:aws:sns:*:*:{TOPIC_NAME}",
            },
        ],
    }
    iam.create_role(
        RoleName="lambda-worker-role",
        AssumeRolePolicyDocument=LAMBDA_TRUST_POLICY,
        Description="Worker Lambda — processes images, updates state, sends notifications",
    )
    iam.put_role_policy(
        RoleName="lambda-worker-role",
        PolicyName="worker-permissions",
        PolicyDocument=json.dumps(worker_policy),
    )
    print(f"  [OK] IAM role created: lambda-worker-role")
    print(f"       Permissions: s3:Get/Put, dynamodb:Get/Put/Update, sqs:Receive/Delete, sns:Publish")

    # After
    show_iam_roles(iam, "AFTER")

    # Print full policies for inspection
    print(f"\n  --- lambda-presign-role policy ---")
    for stmt in presign_policy["Statement"]:
        print(f"    {stmt['Action']}  →  {stmt['Resource']}")
    print(f"  --- lambda-worker-role policy ---")
    for stmt in worker_policy["Statement"]:
        print(f"    {stmt['Action']}  →  {stmt['Resource']}")

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
    print(f"  (Executing as: lambda-presign-role)")

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
    print(f"  (Executing as: lambda-worker-role)")

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
def step6_sns_notification(sns, topic_arn: str, job_id: str, download_url: str):
    banner(6, "SNS NOTIFICATION TO PRIYA")

    notification_payload = {
        "message": "Your Ghibli art is ready!",
        "download_url": download_url[:80] + "...",
        "job_id": job_id,
    }

    print(f"  Publishing to SNS topic: {TOPIC_NAME}")
    print(f"  (Executing as: lambda-worker-role — sns:Publish allowed)")
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
def step8_verify(s3, sqs, sns, dynamo, iam, queue_url: str, topic_arn: str, job_id: str):
    banner(8, "VERIFY END-TO-END STATE")

    show_bucket_contents(s3, RAW_BUCKET, "FINAL")
    show_bucket_contents(s3, OUTPUT_BUCKET, "FINAL")
    print()
    show_queue_depth(sqs, queue_url, "FINAL")
    print()
    show_sns_subscriptions(sns, topic_arn, "FINAL")
    print()
    show_iam_roles(iam, "FINAL")
    print()
    show_job_record(dynamo, job_id, "FINAL")

    print(f"\n  AWS Region: {REGION} (Mumbai)")
    print(f"  Services used: S3, DynamoDB, SQS, SNS, IAM")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")
    print(f"\n  SUCCESS — Priya's secure, least-privilege journey complete!")


# ===========================================================================
# Main — all AWS calls inside a single @mock_aws context
# ===========================================================================
@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)
    dynamo = boto3.client("dynamodb", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)
    iam = boto3.client("iam", region_name=REGION)

    job_id = "a1b2c3d4-5678-9abc-def0-123456789abc"

    infra = step1_create_infrastructure(s3, dynamo, sqs, sns, iam)
    queue_url = infra["queue_url"]
    topic_arn = infra["topic_arn"]

    step2_generate_presigned_put(s3, dynamo, job_id)
    step3_upload_selfie(s3, job_id)
    step4_s3_event_to_sqs(sqs, queue_url, job_id)
    presigned_get_url = step5_worker_processes_job(s3, sqs, dynamo, queue_url, job_id)
    step6_sns_notification(sns, topic_arn, job_id, presigned_get_url)
    step7_download_output(s3, job_id)
    step8_verify(s3, sqs, sns, dynamo, iam, queue_url, topic_arn, job_id)


if __name__ == "__main__":
    run()
