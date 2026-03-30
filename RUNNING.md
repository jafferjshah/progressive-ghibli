# Running the Ghibli Art Generator Prototype

## Setup (once)

```bash
git clone <repo-url>
cd ghibli

# Install dependencies via Poetry
poetry install
```

## Running Each Version

Every version is a single file. Check out the tag and run:

### v1 — S3 Basics

```bash
git checkout v1
poetry run python ghibli_prototype.py
```

Creates two S3 buckets, generates presigned URLs, uploads and downloads.

### v2 — DynamoDB

```bash
git checkout v2
poetry run python ghibli_prototype.py
```

Adds job metadata tracking. Watch the DynamoDB record go from `(not found)` → `PENDING` → `COMPLETE`.

### v3 — SQS

```bash
git checkout v3
poetry run python ghibli_prototype.py
```

Adds a message queue between upload and processing. Watch queue depth go `0 → 1 → 0`.

### v4 — SNS

```bash
git checkout v4
poetry run python ghibli_prototype.py
```

Adds pub-sub notifications. Priya gets a simulated email when her art is ready.

### v5 — IAM

```bash
git checkout v5
poetry run python ghibli_prototype.py
```

Adds least-privilege IAM roles. Each Lambda gets only the permissions it needs — printed in full.

### v6 — CloudWatch

```bash
git checkout v6
poetry run python ghibli_prototype.py
```

Adds log groups and a queue depth alarm. Verification step reads back the logs.

### v7 — Full Flow

```bash
git checkout v7
poetry run python ghibli_prototype.py
```

All services wired together. Real UUIDs. Priya's complete journey end-to-end.

## Comparing Versions

See what changed between any two versions:

```bash
git diff v1 v2    # what did DynamoDB add?
git diff v3 v4    # what did SNS add?
git diff v6 v7    # final polish
```

## Back to Latest

```bash
git checkout main
```
