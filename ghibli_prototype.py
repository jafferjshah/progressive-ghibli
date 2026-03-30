"""
Ghibli Art Generator — AWS Prototype (moto)

v7: Priya's Complete Journey — All Services Wired End-to-End

This is the full prototype. Every AWS service from Weeks 6-9 of course
20XWAA is wired together into one coherent flow:

  Priya uploads a selfie → S3 stores it → SQS queues the job →
  Lambda worker processes it → DynamoDB tracks the state →
  SNS notifies Priya → she downloads her Ghibli art.

All secured by IAM least-privilege roles, observed by CloudWatch logs
and alarms. Six AWS services, zero servers, fully serverless.

Services used:
  - S3         (Week 6) — object storage, presigned URLs
  - Lambda     (Week 7) — serverless compute (simulated as functions)
  - IAM        (Week 8) — least-privilege roles and policies
  - DynamoDB   (Week 9) — NoSQL job metadata
  - SQS        (Week 9) — decoupled job queue
  - SNS        (Week 9) — pub-sub notifications
  - CloudWatch (Week 7/8) — logs and alarms

Run with:
    poetry run python ghibli_prototype.py

No AWS credentials needed — everything is mocked via moto.
"""

from moto import mock_aws
import boto3
import json
import time
import uuid
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

# CloudWatch log group names — follows AWS Lambda convention
LOG_GROUP_PRESIGN = "/aws/lambda/generate-presigned-url"
LOG_GROUP_WORKER = "/aws/lambda/ghibli-style-worker"
ALARM_NAME = "SQS-HighQueueDepth"

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
            print(f"    - {role['RoleName']}")


def write_log(logs, log_group: str, stream: str, message: str):
    """Write a log event to CloudWatch Logs."""
    timestamp_ms = int(time.time() * 1000)
    try:
        logs.create_log_stream(logGroupName=log_group, logStreamName=stream)
    except logs.exceptions.ResourceAlreadyExistsException:
        pass
    logs.put_log_events(
        logGroupName=log_group,
        logStreamName=stream,
        logEvents=[{"timestamp": timestamp_ms, "message": message}],
    )


def show_log_events(logs, log_group: str, stream: str, label: str, limit: int = 5):
    """Retrieve and display recent log events from a CloudWatch log stream."""
    try:
        response = logs.get_log_events(
            logGroupName=log_group,
            logStreamName=stream,
            limit=limit,
        )
        events = response.get("events", [])
        if not events:
            print(f"  [{label}] {log_group}: (no events)")
        else:
            print(f"  [{label}] {log_group}:")
            for event in events:
                ts = datetime.fromtimestamp(event["timestamp"] / 1000, tz=timezone.utc)
                print(f"    [{ts.strftime('%H:%M:%SZ')}] {event['message']}")
    except Exception:
        print(f"  [{label}] {log_group}: (no log stream)")


# ---------------------------------------------------------------------------
# STEP 1: Create Infrastructure
# ---------------------------------------------------------------------------
def step1_create_infrastructure(s3, dynamo, sqs, sns, iam, logs, cw) -> dict:
    banner(1, "INFRASTRUCTURE SETUP — All Services")

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
    s3.create_bucket(Bucket=RAW_BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    s3.create_bucket(Bucket=OUTPUT_BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    print(f"  [OK] Created S3 buckets: {RAW_BUCKET}, {OUTPUT_BUCKET}")

    # DynamoDB table
    dynamo.create_table(
        TableName=JOBS_TABLE,
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  [OK] Created DynamoDB table: {JOBS_TABLE}")

    # SQS queue
    response = sqs.create_queue(QueueName=QUEUE_NAME, Attributes={"VisibilityTimeout": "60"})
    queue_url = response["QueueUrl"]
    print(f"  [OK] Created SQS queue: {QUEUE_NAME}")

    # SNS topic + subscription
    topic_response = sns.create_topic(Name=TOPIC_NAME)
    topic_arn = topic_response["TopicArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=USER_EMAIL)
    print(f"  [OK] Created SNS topic: {TOPIC_NAME} + subscribed {USER_EMAIL}")

    # IAM roles (same as v5)
    presign_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": f"arn:aws:s3:::{RAW_BUCKET}/*"},
            {"Effect": "Allow", "Action": ["dynamodb:PutItem"], "Resource": f"arn:aws:dynamodb:*:*:table/{JOBS_TABLE}"},
        ],
    }
    iam.create_role(RoleName="lambda-presign-role", AssumeRolePolicyDocument=LAMBDA_TRUST_POLICY)
    iam.put_role_policy(RoleName="lambda-presign-role", PolicyName="presign-permissions", PolicyDocument=json.dumps(presign_policy))

    worker_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{RAW_BUCKET}/*"},
            {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"], "Resource": f"arn:aws:s3:::{OUTPUT_BUCKET}/*"},
            {"Effect": "Allow", "Action": ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem"], "Resource": f"arn:aws:dynamodb:*:*:table/{JOBS_TABLE}"},
            {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], "Resource": f"arn:aws:sqs:*:*:{QUEUE_NAME}"},
            {"Effect": "Allow", "Action": ["sns:Publish"], "Resource": f"arn:aws:sns:*:*:{TOPIC_NAME}"},
        ],
    }
    iam.create_role(RoleName="lambda-worker-role", AssumeRolePolicyDocument=LAMBDA_TRUST_POLICY)
    iam.put_role_policy(RoleName="lambda-worker-role", PolicyName="worker-permissions", PolicyDocument=json.dumps(worker_policy))
    print(f"  [OK] Created IAM roles: lambda-presign-role, lambda-worker-role")

    # -----------------------------------------------------------------------
    # CloudWatch Log Groups
    # WHY CloudWatch Logs? When a Lambda runs, you can't SSH into it — it's
    # serverless, there's no server to access. CloudWatch Logs is where ALL
    # Lambda output goes. It's your only window into what happened.
    #
    # In real AWS, Lambda automatically creates log groups and writes
    # stdout/stderr there. We create them explicitly to show the pattern.
    # -----------------------------------------------------------------------
    logs.create_log_group(logGroupName=LOG_GROUP_PRESIGN)
    logs.create_log_group(logGroupName=LOG_GROUP_WORKER)
    print(f"  [OK] Created CloudWatch log groups:")
    print(f"       {LOG_GROUP_PRESIGN}")
    print(f"       {LOG_GROUP_WORKER}")

    # -----------------------------------------------------------------------
    # CloudWatch Alarm — SQS Queue Depth
    # WHY ALARMS? Logs tell you what happened in the past. Alarms tell you
    # something is wrong RIGHT NOW. If the queue has 100+ messages, either
    # the worker is down or traffic spiked beyond capacity.
    #
    # In production, this alarm could trigger:
    #   - Auto-scaling (add more Lambda concurrency)
    #   - PagerDuty alert (wake up the on-call engineer)
    #   - SNS notification (email the ops team)
    # -----------------------------------------------------------------------
    cw.put_metric_alarm(
        AlarmName=ALARM_NAME,
        MetricName="ApproximateNumberOfMessagesVisible",
        Namespace="AWS/SQS",
        Statistic="Average",
        Period=60,
        EvaluationPeriods=1,
        Threshold=100,
        ComparisonOperator="GreaterThanThreshold",
        Dimensions=[{"Name": "QueueName", "Value": QUEUE_NAME}],
        AlarmDescription="Alert when queue depth exceeds 100 messages — worker may be down",
    )
    print(f"  [OK] Created CloudWatch alarm: {ALARM_NAME} (threshold: >100 msgs)")

    # After
    show_iam_roles(iam, "AFTER")
    existing_groups = logs.describe_log_groups()["logGroups"]
    print(f"  [AFTER] CloudWatch log groups: {[g['logGroupName'] for g in existing_groups]}")
    alarms = cw.describe_alarms(AlarmNames=[ALARM_NAME])["MetricAlarms"]
    print(f"  [AFTER] CloudWatch alarms: {[a['AlarmName'] for a in alarms]}")

    return {"queue_url": queue_url, "topic_arn": topic_arn}


# ---------------------------------------------------------------------------
# STEP 2: Generate Presigned PUT URL + Create Job Record
# ---------------------------------------------------------------------------
def step2_generate_presigned_put(s3, dynamo, logs, job_id: str) -> str:
    banner(2, "PRIYA OPENS THE APP — Presigned URL + Job Record")

    input_key = f"{job_id}/original.jpg"
    now = datetime.now(timezone.utc).isoformat()
    stream = f"invocation-{job_id[:8]}"

    print(f"  Priya requests upload URL for: {FILENAME}")
    print(f"  job_id: {job_id}")
    print(f"  (Executing as: lambda-presign-role)")

    show_job_record(dynamo, job_id, "BEFORE")

    # Log: function invoked
    write_log(logs, LOG_GROUP_PRESIGN, stream, f"Upload request received: email={USER_EMAIL}, filename={FILENAME}")

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

    # Log: job created
    write_log(logs, LOG_GROUP_PRESIGN, stream, f"Job created: job_id={job_id}, status=PENDING")
    write_log(logs, LOG_GROUP_PRESIGN, stream, f"Presigned PUT URL generated, expires_in=300s")

    show_job_record(dynamo, job_id, "AFTER")

    return presigned_put_url


# ---------------------------------------------------------------------------
# STEP 3: Upload Selfie
# ---------------------------------------------------------------------------
def step3_upload_selfie(s3, job_id: str):
    banner(3, "PRIYA UPLOADS HER SELFIE — Browser → S3 Presigned PUT")

    input_key = f"{job_id}/original.jpg"

    show_bucket_contents(s3, RAW_BUCKET, "BEFORE")

    s3.put_object(Bucket=RAW_BUCKET, Key=input_key, Body=FAKE_IMAGE, ContentType="image/jpeg")
    print(f"\n  [OK] Uploaded {len(FAKE_IMAGE)} bytes to {RAW_BUCKET}/{input_key}")

    show_bucket_contents(s3, RAW_BUCKET, "AFTER")

    head = s3.head_object(Bucket=RAW_BUCKET, Key=input_key)
    print(f"  [VERIFY] Content-Type: {head['ContentType']}, Size: {head['ContentLength']}, ETag: {head['ETag']}")


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

    response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(event_payload))
    print(f"  [OK] SQS message sent (ID: {response['MessageId']})")
    print(f"       Payload: {json.dumps(event_payload, indent=2)}")

    show_queue_depth(sqs, queue_url, "AFTER")


# ---------------------------------------------------------------------------
# STEP 5: Worker Polls SQS → Processes → Updates DynamoDB
# ---------------------------------------------------------------------------
def step5_worker_processes_job(s3, sqs, dynamo, logs, queue_url: str, job_id: str) -> str:
    banner(5, "WORKER POLLS SQS AND PROCESSES JOB")

    now = datetime.now(timezone.utc).isoformat()
    stream = f"invocation-{job_id[:8]}"
    print(f"  (Executing as: lambda-worker-role)")

    show_queue_depth(sqs, queue_url, "BEFORE")
    show_job_record(dynamo, job_id, "BEFORE")

    # Worker polls SQS
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    messages = response.get("Messages", [])

    if not messages:
        print("  [ERROR] No messages in queue!")
        write_log(logs, LOG_GROUP_WORKER, stream, "ERROR: No messages available in queue")
        return ""

    message = messages[0]
    receipt_handle = message["ReceiptHandle"]
    payload = json.loads(message["Body"])
    print(f"\n  [OK] Received SQS message: job_id={payload['job_id']}")

    # Log: job received
    write_log(logs, LOG_GROUP_WORKER, stream, f"Job received: {payload['job_id']}")

    # Download original from S3
    input_key = payload["input_key"]
    output_key = f"{job_id}/ghibli-art.jpg"
    obj = s3.get_object(Bucket=RAW_BUCKET, Key=input_key)
    original_bytes = obj["Body"].read()
    print(f"  [OK] Downloaded original: {len(original_bytes)} bytes from {RAW_BUCKET}")
    write_log(logs, LOG_GROUP_WORKER, stream, f"Downloaded original image: {len(original_bytes)} bytes")

    # Simulate style transfer
    metadata = f"GHIBLI_PROCESSED::job_id={job_id}::timestamp={now}::".encode()
    processed_bytes = metadata + original_bytes
    print(f"  [OK] Style transfer applied: {len(original_bytes)} → {len(processed_bytes)} bytes")
    write_log(logs, LOG_GROUP_WORKER, stream, f"Ghibli style transfer complete: {len(processed_bytes)} bytes")

    # Upload processed image
    show_bucket_contents(s3, OUTPUT_BUCKET, "BEFORE")
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=output_key, Body=processed_bytes, ContentType="image/jpeg")
    print(f"\n  [OK] Uploaded processed image to {OUTPUT_BUCKET}/{output_key}")
    show_bucket_contents(s3, OUTPUT_BUCKET, "AFTER")
    write_log(logs, LOG_GROUP_WORKER, stream, f"Output uploaded to {OUTPUT_BUCKET}/{output_key}")

    # Generate presigned GET URL
    presigned_get_url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": OUTPUT_BUCKET, "Key": output_key}, ExpiresIn=3600,
    )

    # Update DynamoDB: PENDING → COMPLETE
    dynamo.update_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET #s = :s, output_key = :ok, output_url = :ou, completed_at = :ca",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": {"S": "COMPLETE"}, ":ok": {"S": output_key},
            ":ou": {"S": presigned_get_url[:80] + "..."}, ":ca": {"S": now},
        },
    )
    print(f"  [OK] DynamoDB UpdateItem: status=PENDING → COMPLETE")
    write_log(logs, LOG_GROUP_WORKER, stream, f"DynamoDB updated: status=COMPLETE")

    # Delete the SQS message (acknowledge)
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    print(f"  [OK] SQS message deleted (acknowledged)")

    show_queue_depth(sqs, queue_url, "AFTER")
    show_job_record(dynamo, job_id, "AFTER")

    return presigned_get_url


# ---------------------------------------------------------------------------
# STEP 6: SNS Notification to Priya
# ---------------------------------------------------------------------------
def step6_sns_notification(sns, logs, topic_arn: str, job_id: str, download_url: str):
    banner(6, "SNS NOTIFICATION TO PRIYA")

    stream = f"invocation-{job_id[:8]}"

    notification_payload = {
        "message": "Your Ghibli art is ready!",
        "download_url": download_url[:80] + "...",
        "job_id": job_id,
    }

    print(f"  Publishing to SNS topic: {TOPIC_NAME}")
    print(f"  Payload: {json.dumps(notification_payload, indent=4)}")

    response = sns.publish(
        TopicArn=topic_arn,
        Subject="Your Ghibli art is ready!",
        Message=json.dumps(notification_payload),
    )
    print(f"\n  [OK] SNS message published (ID: {response['MessageId']})")
    print(f"  [OK] Simulated email to {USER_EMAIL}:")
    print(f'       Subject: "Your Ghibli art is ready!"')
    print(f"       Body: Download your art: {download_url[:60]}...")

    write_log(logs, LOG_GROUP_WORKER, stream, f"SNS notification published for job {job_id}")


# ---------------------------------------------------------------------------
# STEP 7: Download Output
# ---------------------------------------------------------------------------
def step7_download_output(s3, job_id: str):
    banner(7, "PRIYA DOWNLOADS HER GHIBLI ART — Presigned GET URL")

    output_key = f"{job_id}/ghibli-art.jpg"

    presigned_get_url = s3.generate_presigned_url(
        "get_object", Params={"Bucket": OUTPUT_BUCKET, "Key": output_key}, ExpiresIn=3600,
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
def step8_verify(s3, sqs, sns, dynamo, iam, logs, cw, queue_url, topic_arn, job_id):
    banner(8, "VERIFY END-TO-END STATE")

    stream = f"invocation-{job_id[:8]}"

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

    # -----------------------------------------------------------------------
    # CloudWatch Logs — retrieve what each Lambda logged
    # In real AWS, you'd use CloudWatch Logs Insights to search across
    # thousands of invocations. Here we read the stream directly.
    # -----------------------------------------------------------------------
    print()
    show_log_events(logs, LOG_GROUP_PRESIGN, stream, "LOGS")
    print()
    show_log_events(logs, LOG_GROUP_WORKER, stream, "LOGS")

    # -----------------------------------------------------------------------
    # CloudWatch Alarm status
    # We manually set the alarm to OK to simulate a healthy system.
    # In real AWS, CloudWatch evaluates the metric automatically.
    # -----------------------------------------------------------------------
    cw.set_alarm_state(
        AlarmName=ALARM_NAME,
        StateValue="OK",
        StateReason="Queue depth within normal range",
    )
    alarm_status = cw.describe_alarms(AlarmNames=[ALARM_NAME])["MetricAlarms"]
    if alarm_status:
        a = alarm_status[0]
        print(f"\n  CloudWatch Alarm '{a['AlarmName']}': {a['StateValue']}")
        print(f"    Threshold: > {int(a['Threshold'])} messages → triggers alarm")
        print(f"    Description: {a['AlarmDescription']}")

    print(f"\n  {'=' * 56}")
    print(f"  SUCCESS — Priya's Ghibli art journey complete!")
    print(f"  {'=' * 56}")
    print(f"  Total services used: S3, SQS, SNS, DynamoDB, IAM,")
    print(f"                       CloudWatch (Logs + Alarms)")
    print(f"  AWS Region: {REGION} (Mumbai)")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")


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
    logs = boto3.client("logs", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)

    # In v1-v6, we used a fixed job_id for predictable output.
    # Now we use a real UUID — every run produces a unique job.
    job_id = str(uuid.uuid4())

    infra = step1_create_infrastructure(s3, dynamo, sqs, sns, iam, logs, cw)
    queue_url = infra["queue_url"]
    topic_arn = infra["topic_arn"]

    step2_generate_presigned_put(s3, dynamo, logs, job_id)
    step3_upload_selfie(s3, job_id)
    step4_s3_event_to_sqs(sqs, queue_url, job_id)
    presigned_get_url = step5_worker_processes_job(s3, sqs, dynamo, logs, queue_url, job_id)
    step6_sns_notification(sns, logs, topic_arn, job_id, presigned_get_url)
    step7_download_output(s3, job_id)
    step8_verify(s3, sqs, sns, dynamo, iam, logs, cw, queue_url, topic_arn, job_id)


if __name__ == "__main__":
    run()
