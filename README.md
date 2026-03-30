# Ghibli Art Generator — AWS Prototype

Learn AWS cloud services by following the evolution of a serverless image processing pipeline.

Priya, a college student in Mumbai, uploads a selfie to the Ghibli Art app. Within seconds, the app transforms her photo into Studio Ghibli-style artwork and emails her: "Your Ghibli art is ready!" She clicks a link and downloads her Ghibli portrait.

All AWS interactions are mocked using [moto](https://github.com/getmoto/moto) — no AWS account, no credentials, no cost. Runs entirely on your local machine.

Based on the architecture taught in **course 20XWAA Cloud Computing** (Weeks 6–9).

## Setup

```bash
# Install dependencies
poetry install

# Run the prototype
poetry run python ghibli_prototype.py
```

## How to Use

Each version is tagged. Start from v1 and progress through v7:

```bash
git checkout v1    # start here
# read the code, run it, understand it
git checkout v2    # next version
# ...repeat...
git checkout v7    # full flow
```

Compare what changed between versions:
```bash
git diff v3 v4     # see what SNS added
```

## Versions

| Tag | Service Added | What Changes |
|-----|--------------|--------------|
| **v1** | S3 | Two buckets, presigned PUT/GET URLs, upload & download |
| **v2** | DynamoDB | Job metadata table, status tracking (PENDING → COMPLETE) |
| **v3** | SQS | Decouple upload from processing via message queue |
| **v4** | SNS | Pub-sub notification — email Priya when art is ready |
| **v5** | IAM | Least-privilege roles for each Lambda function |
| **v6** | CloudWatch | Log groups for each Lambda, SQS queue depth alarm |
| **v7** | Full Flow | All services wired together — Priya's complete journey |

## The Architecture

```
  Priya's Phone          AWS Cloud (mocked via moto)
  ┌──────────┐
  │  Upload   │──── presigned PUT URL ───▶ [ S3: ghibli-raw-uploads ]
  │  Selfie   │                                     │
  └──────────┘                            S3 event notification
                                                    │
                                                    ▼
                                          [ SQS: ghibli-jobs-queue ]
                                                    │
                                              worker polls
                                                    │
                                                    ▼
                                        [ Lambda: ghibli-style-worker ]
                                           │        │        │
                                    read original  write   update
                                    from S3      output   DynamoDB
                                                 to S3    status
                                                    │
                                                    ▼
                                          [ SNS: ghibli-notifications ]
                                                    │
                                              email to Priya
                                                    │
  ┌──────────┐                                      │
  │ Download  │◀── presigned GET URL ───  "Your Ghibli art is ready!"
  │  Art      │
  └──────────┘
```

## What You Learn

| Transition | Concept |
|-----------|---------|
| v1 → v2 | Why you need a database, not just file storage |
| v2 → v3 | Decoupling via message queues — handling traffic spikes |
| v3 → v4 | Point-to-point (SQS) vs pub-sub broadcast (SNS) |
| v4 → v5 | Security: least privilege — each function gets only what it needs |
| v5 → v6 | Observability: you can't SSH into serverless — logs and alarms are essential |
| v6 → v7 | Wiring it all together — from individual services to a coherent system |

## AWS Services Summary

| Service | Week | Role in This Prototype |
|---------|------|----------------------|
| **S3** | 6 | Object storage — raw uploads and processed output |
| **Lambda** | 7 | Serverless compute — presign URLs and process images |
| **IAM** | 8 | Security — least-privilege roles for each Lambda |
| **DynamoDB** | 9 | NoSQL database — job metadata and status tracking |
| **SQS** | 9 | Message queue — decouples upload from processing |
| **SNS** | 9 | Pub-sub — notifies Priya when her art is ready |
| **CloudWatch** | 7/8 | Observability — logs and queue depth alarms |

*Course: 20XWAA Cloud Computing | Region: ap-south-1 (Mumbai) | Mock framework: moto v5+*
