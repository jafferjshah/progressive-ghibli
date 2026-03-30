"""
Ghibli Art Generator — AWS Prototype (moto)

v1: S3 Basics — Buckets, Presigned URLs, Upload & Download

Priya wants to upload a selfie and get Ghibli-style art back.
In this version we set up the storage layer: two S3 buckets,
presigned URLs for secure upload/download, and verify every
operation by inspecting bucket state before and after.

Run with:
    poetry run python ghibli_prototype.py

No AWS credentials needed — everything is mocked via moto.
"""

from moto import mock_aws
import boto3
import json
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Constants — same values throughout the prototype for predictable output
# ---------------------------------------------------------------------------
REGION = "ap-south-1"  # Mumbai — Priya's region, data stays close to users
RAW_BUCKET = "ghibli-raw-uploads"
OUTPUT_BUCKET = "ghibli-output"
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


# ---------------------------------------------------------------------------
# STEP 1: Create S3 Buckets
# ---------------------------------------------------------------------------
# WHY TWO BUCKETS? Separation of concerns. Raw uploads are temporary input;
# processed output is what Priya downloads. Different access patterns mean
# different storage classes and lifecycle policies. In production, you might
# expire raw uploads after 24 hours but keep output longer.
# ---------------------------------------------------------------------------
def step1_create_buckets(s3):
    banner(1, "INFRASTRUCTURE SETUP — S3 BUCKETS")

    # Before — no buckets exist yet
    existing = s3.list_buckets()["Buckets"]
    print(f"  [BEFORE] Existing buckets: {[b['Name'] for b in existing] or '(none)'}")

    # Create raw uploads bucket — STANDARD storage (frequently accessed during upload)
    s3.create_bucket(
        Bucket=RAW_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"  [OK] Created bucket: {RAW_BUCKET} (STANDARD storage, {REGION})")

    # Create output bucket — STANDARD_IA (Infrequent Access)
    # Priya downloads her art once; after that it's rarely accessed.
    # STANDARD_IA is cheaper for storage, slightly more expensive per retrieval.
    s3.create_bucket(
        Bucket=OUTPUT_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    print(f"  [OK] Created bucket: {OUTPUT_BUCKET} (STANDARD_IA intent, {REGION})")

    # After — both buckets exist
    existing = s3.list_buckets()["Buckets"]
    print(f"  [AFTER] Existing buckets: {[b['Name'] for b in existing]}")


# ---------------------------------------------------------------------------
# STEP 2: Generate Presigned PUT URL
# ---------------------------------------------------------------------------
# WHY PRESIGNED URLs? The browser uploads directly to S3, bypassing the app
# server entirely. This means:
#   1. No server bandwidth bottleneck — S3 handles the heavy lifting
#   2. No server memory pressure — large files never touch your Lambda
#   3. Secure — the URL expires after 300 seconds and only allows PUT to
#      a specific key. The browser never sees AWS credentials.
# ---------------------------------------------------------------------------
def step2_generate_presigned_put(s3, job_id: str) -> str:
    banner(2, "PRIYA OPENS THE APP — Generate Presigned PUT URL")

    input_key = f"{job_id}/original.jpg"

    print(f"  Priya requests upload URL for: {FILENAME}")
    print(f"  job_id: {job_id}")
    print(f"  S3 key will be: {RAW_BUCKET}/{input_key}")

    # Generate presigned PUT URL — valid for 300 seconds
    presigned_put_url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": RAW_BUCKET, "Key": input_key},
        ExpiresIn=300,
    )

    print(f"  [OK] Presigned PUT URL generated (expires in 300s)")
    print(f"       URL: {presigned_put_url[:80]}...")

    return presigned_put_url


# ---------------------------------------------------------------------------
# STEP 3: Upload via Presigned URL (simulated)
# ---------------------------------------------------------------------------
# In real AWS, the browser would HTTP PUT to the presigned URL.
# In moto, presigned URL validation isn't enforced, so we use s3.put_object()
# directly — the boto3 call is identical to what happens behind the scenes.
# ---------------------------------------------------------------------------
def step3_upload_selfie(s3, job_id: str):
    banner(3, "PRIYA UPLOADS HER SELFIE — Browser → S3 Presigned PUT")

    input_key = f"{job_id}/original.jpg"

    # Before — bucket should be empty
    show_bucket_contents(s3, RAW_BUCKET, "BEFORE")

    # Simulate browser PUT using presigned URL
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=input_key,
        Body=FAKE_IMAGE,
        ContentType="image/jpeg",
    )
    print(f"\n  [OK] Uploaded {len(FAKE_IMAGE)} bytes to {RAW_BUCKET}/{input_key}")

    # After — object should appear
    show_bucket_contents(s3, RAW_BUCKET, "AFTER")

    # Verify the object metadata
    head = s3.head_object(Bucket=RAW_BUCKET, Key=input_key)
    print(f"  [VERIFY] Content-Type: {head['ContentType']}")
    print(f"  [VERIFY] Content-Length: {head['ContentLength']}")
    print(f"  [VERIFY] ETag: {head['ETag']}")


# ---------------------------------------------------------------------------
# STEP 4: Simulate Ghibli Style Transfer
# ---------------------------------------------------------------------------
# In production, this would be an ML model (SageMaker, etc.).
# For the prototype, we prepend a metadata header to prove data flowed
# end-to-end through the system.
# ---------------------------------------------------------------------------
def step4_process_and_upload_output(s3, job_id: str):
    banner(4, "GHIBLI STYLE TRANSFER — Process and Upload Output")

    input_key = f"{job_id}/original.jpg"
    output_key = f"{job_id}/ghibli-art.jpg"

    # Download original from raw bucket
    response = s3.get_object(Bucket=RAW_BUCKET, Key=input_key)
    original_bytes = response["Body"].read()
    print(f"  [OK] Downloaded original: {len(original_bytes)} bytes from {RAW_BUCKET}")

    # Simulate style transfer
    metadata = f"GHIBLI_PROCESSED::job_id={job_id}::timestamp={datetime.now(timezone.utc).isoformat()}::".encode()
    processed_bytes = metadata + original_bytes
    print(f"  [OK] Style transfer applied: {len(original_bytes)} → {len(processed_bytes)} bytes")

    # Before — output bucket should be empty
    show_bucket_contents(s3, OUTPUT_BUCKET, "BEFORE")

    # Upload processed image to output bucket
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=output_key,
        Body=processed_bytes,
        ContentType="image/jpeg",
    )
    print(f"\n  [OK] Uploaded processed image to {OUTPUT_BUCKET}/{output_key}")

    # After — output should appear
    show_bucket_contents(s3, OUTPUT_BUCKET, "AFTER")


# ---------------------------------------------------------------------------
# STEP 5: Generate Presigned GET URL and Download
# ---------------------------------------------------------------------------
# Same presigned URL pattern, but for GET. Priya clicks a link in her
# notification email — that link is a presigned GET URL. She never needs
# AWS credentials to download her art.
# ---------------------------------------------------------------------------
def step5_download_output(s3, job_id: str):
    banner(5, "PRIYA DOWNLOADS HER GHIBLI ART — Presigned GET URL")

    output_key = f"{job_id}/ghibli-art.jpg"

    # Generate presigned GET URL — valid for 1 hour
    presigned_get_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": OUTPUT_BUCKET, "Key": output_key},
        ExpiresIn=3600,
    )
    print(f"  [OK] Presigned GET URL generated (expires in 3600s)")
    print(f"       URL: {presigned_get_url[:80]}...")

    # Simulate Priya clicking the download link
    response = s3.get_object(Bucket=OUTPUT_BUCKET, Key=output_key)
    downloaded = response["Body"].read()
    print(f"\n  [OK] Downloaded: {len(downloaded)} bytes")
    print(f"  [VERIFY] Starts with GHIBLI_PROCESSED: {downloaded[:30]}...")


# ---------------------------------------------------------------------------
# STEP 6: Verify Final State
# ---------------------------------------------------------------------------
def step6_verify(s3):
    banner(6, "VERIFY END-TO-END STATE")

    show_bucket_contents(s3, RAW_BUCKET, "FINAL")
    show_bucket_contents(s3, OUTPUT_BUCKET, "FINAL")

    print(f"\n  AWS Region: {REGION} (Mumbai)")
    print(f"  Services used: S3 (presigned PUT, presigned GET, object storage)")
    print(f"  All interactions mocked via moto — zero real AWS calls made.")
    print(f"\n  SUCCESS — Priya's S3 journey complete!")


# ===========================================================================
# Main — all AWS calls inside a single @mock_aws context
# ===========================================================================
@mock_aws
def run():
    s3 = boto3.client("s3", region_name=REGION)

    # Use a fixed job_id for reproducible output in v1
    job_id = "a1b2c3d4-5678-9abc-def0-123456789abc"

    step1_create_buckets(s3)
    step2_generate_presigned_put(s3, job_id)
    step3_upload_selfie(s3, job_id)
    step4_process_and_upload_output(s3, job_id)
    step5_download_output(s3, job_id)
    step6_verify(s3)


if __name__ == "__main__":
    run()
