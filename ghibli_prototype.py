"""
Ghibli Art Generator — AWS Prototype (moto)

v2: S3 + DynamoDB — Job Metadata and Status Tracking

Building on v1's S3 storage, we now add DynamoDB to track job state.
Without a database, we have no way to know: Did Priya's job start?
Is it still processing? Did it finish? DynamoDB gives every job a
lifecycle: PENDING → PROCESSING → COMPLETE.

New in this version:
  - DynamoDB table 'jobs-metadata' with job_id as partition key
  - Job record created at upload time (status=PENDING)
  - Job record updated after processing (status=COMPLETE)
  - Before/after verification on every DynamoDB operation

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
            # DynamoDB returns typed values like {'S': 'value'} — extract the actual value
            actual = list(val.values())[0]
            print(f"    {key}: {actual}")


# ---------------------------------------------------------------------------
# STEP 1: Create S3 Buckets + DynamoDB Table
# ---------------------------------------------------------------------------
# WHY TWO BUCKETS? Separation of concerns. Raw uploads are temporary input;
# processed output is what Priya downloads. Different access patterns mean
# different storage classes and lifecycle policies.
#
# WHY DynamoDB? S3 is great for storing files but terrible for answering
# "what's the status of Priya's job?" You'd have to list objects and infer
# state from filenames. DynamoDB gives us a proper job record with status,
# timestamps, and the ability to query by job_id in milliseconds.
# ---------------------------------------------------------------------------
def step1_create_infrastructure(s3, dynamo):
    banner(1, "INFRASTRUCTURE SETUP — S3 + DynamoDB")

    # Before
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    print(f"  [BEFORE] Buckets: {[b['Name'] for b in existing_buckets] or '(none)'}")
    print(f"  [BEFORE] DynamoDB tables: {existing_tables or '(none)'}")

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

    # -----------------------------------------------------------------------
    # DynamoDB table — PAY_PER_REQUEST billing
    # WHY PAY_PER_REQUEST? Serverless means we don't want to guess capacity.
    # With on-demand billing, DynamoDB scales automatically. You pay per read
    # and write — perfect for unpredictable traffic (Priya might upload at
    # 3am or during a viral TikTok trend).
    # -----------------------------------------------------------------------
    dynamo.create_table(
        TableName=JOBS_TABLE,
        KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    print(f"  [OK] Created DynamoDB table: {JOBS_TABLE} (PAY_PER_REQUEST)")

    # After
    existing_buckets = s3.list_buckets()["Buckets"]
    existing_tables = dynamo.list_tables()["TableNames"]
    print(f"  [AFTER] Buckets: {[b['Name'] for b in existing_buckets]}")
    print(f"  [AFTER] DynamoDB tables: {existing_tables}")


# ---------------------------------------------------------------------------
# STEP 2: Generate Presigned PUT URL + Create Job Record
# ---------------------------------------------------------------------------
# WHY PRESIGNED URLs? The browser uploads directly to S3, bypassing the app
# server entirely. No bandwidth bottleneck, no memory pressure, and the
# browser never sees AWS credentials.
#
# NEW: We now also create a DynamoDB record with status=PENDING. This is
# the moment the job is "born" — from here we can track it through its
# entire lifecycle.
# ---------------------------------------------------------------------------
def step2_generate_presigned_put(s3, dynamo, job_id: str) -> str:
    banner(2, "PRIYA OPENS THE APP — Presigned URL + Job Record")

    input_key = f"{job_id}/original.jpg"
    now = datetime.now(timezone.utc).isoformat()

    print(f"  Priya requests upload URL for: {FILENAME}")
    print(f"  job_id: {job_id}")

    # Before — no job record exists yet
    show_job_record(dynamo, job_id, "BEFORE")

    # Generate presigned PUT URL
    presigned_put_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": RAW_BUCKET, "Key": input_key},
        ExpiresIn=300,
    )
    print(f"\n  [OK] Presigned PUT URL generated (expires in 300s)")
    print(f"       URL: {presigned_put_url[:80]}...")

    # Create job record in DynamoDB — status=PENDING
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

    # After — job record should exist with PENDING status
    show_job_record(dynamo, job_id, "AFTER")

    return presigned_put_url


# ---------------------------------------------------------------------------
# STEP 3: Upload via Presigned URL (simulated)
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
# STEP 4: Process Image + Update Job Status
# ---------------------------------------------------------------------------
# The worker downloads the original, applies "style transfer", uploads the
# result, and — crucially — updates the DynamoDB record to COMPLETE.
# This is the state transition that lets the rest of the system know
# Priya's art is ready.
# ---------------------------------------------------------------------------
def step4_process_and_upload_output(s3, dynamo, job_id: str):
    banner(4, "GHIBLI STYLE TRANSFER — Process, Upload, Update Status")

    input_key = f"{job_id}/original.jpg"
    output_key = f"{job_id}/ghibli-art.jpg"
    now = datetime.now(timezone.utc).isoformat()

    # Show job status before processing
    show_job_record(dynamo, job_id, "BEFORE")

    # Download original from raw bucket
    response = s3.get_object(Bucket=RAW_BUCKET, Key=input_key)
    original_bytes = response["Body"].read()
    print(f"\n  [OK] Downloaded original: {len(original_bytes)} bytes from {RAW_BUCKET}")

    # Simulate style transfer
    metadata = f"GHIBLI_PROCESSED::job_id={job_id}::timestamp={now}::".encode()
    processed_bytes = metadata + original_bytes
    print(f"  [OK] Style transfer applied: {len(original_bytes)} → {len(processed_bytes)} bytes")

    show_bucket_contents(s3, OUTPUT_BUCKET, "BEFORE")

    # Upload processed image
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

    # -----------------------------------------------------------------------
    # Update DynamoDB: PENDING → COMPLETE
    # WHY UpdateItem (not PutItem)? UpdateItem modifies specific attributes
    # without replacing the entire record. In a concurrent system, another
    # process might be reading this record — UpdateItem is atomic and safe.
    # -----------------------------------------------------------------------
    dynamo.update_item(
        TableName=JOBS_TABLE,
        Key={"job_id": {"S": job_id}},
        UpdateExpression="SET #s = :s, output_key = :ok, output_url = :ou, completed_at = :ca",
        ExpressionAttributeNames={"#s": "status"},  # 'status' is a reserved word in DynamoDB
        ExpressionAttributeValues={
            ":s": {"S": "COMPLETE"},
            ":ok": {"S": output_key},
            ":ou": {"S": presigned_get_url[:80] + "..."},
            ":ca": {"S": now},
        },
    )
    print(f"  [OK] DynamoDB UpdateItem: status=PENDING → COMPLETE")

    # After — job should now be COMPLETE with output details
    show_job_record(dynamo, job_id, "AFTER")


# ---------------------------------------------------------------------------
# STEP 5: Download Output
# ---------------------------------------------------------------------------
def step5_download_output(s3, job_id: str):
    banner(5, "PRIYA DOWNLOADS HER GHIBLI ART — Presigned GET URL")

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
# STEP 6: Verify End-to-End State
# ---------------------------------------------------------------------------
def step6_verify(s3, dynamo, job_id: str):
    banner(6, "VERIFY END-TO-END STATE")

    show_bucket_contents(s3, RAW_BUCKET, "FINAL")
    show_bucket_contents(s3, OUTPUT_BUCKET, "FINAL")
    print()
    show_job_record(dynamo, job_id, "FINAL")

    print(f"\n  AWS Region: {REGION} (Mumbai)")
    print(f"  Services used: S3, DynamoDB")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")
    print(f"\n  SUCCESS — Priya's S3 + DynamoDB journey complete!")


# ===========================================================================
# Main — all AWS calls inside a single @mock_aws context
# ===========================================================================
@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)
    dynamo = boto3.client("dynamodb", region_name=REGION)

    job_id = "a1b2c3d4-5678-9abc-def0-123456789abc"

    step1_create_infrastructure(s3, dynamo)
    step2_generate_presigned_put(s3, dynamo, job_id)
    step3_upload_selfie(s3, job_id)
    step4_process_and_upload_output(s3, dynamo, job_id)
    step5_download_output(s3, job_id)
    step6_verify(s3, dynamo, job_id)


if __name__ == "__main__":
    run()
