# DataFlip — Complete AWS Architecture Walkthrough

## Executive Summary

**DataFlip — Serverless Blue/Green Data Deployment System** is a cloud-native architecture implementing zero-downtime dataset promotion and rollback for analytical data lakes in AWS.

All components adhere strictly to cloud-first AWS principles, Terraform Infrastructure as Code (IaC), least-privilege security, zero-copy pointer promotion, and automated test verification (`pytest`).

---

## 1. Production Architecture Flow

```text
S3 (curated/green/*.parquet) 
       ↓ (s3:ObjectCreated notification)
AWS Lambda (handler.py) 
       ↓ (DataFrame validation)
Validation PASSED?
    ├── YES → Update AWS Glue Data Catalog Location → s3://.../curated/green/ (GREEN Activated)
    └── NO  → Retain Location → s3://.../curated/blue/ (BLUE Retained)
                 ↓ (Manual Rollback Invocation)
              Revert Location → s3://.../curated/blue/ (Instant Rollback)
```

- **Amazon S3**:
  - `s3://${S3_BUCKET}/curated/blue/`: Production known-good dataset.
  - `s3://${S3_BUCKET}/curated/green/`: Candidate dataset staging.
  - `s3://${S3_BUCKET}/curated/active_manifest.json`: Deployment state audit ledger.
- **AWS Glue Data Catalog**: External table `dataflip_db.sales_curated` whose `StorageDescriptor.Location` is the **authoritative pointer** for all downstream queries.
- **Amazon Athena**: Decoupled SQL query engine reading directly from `dataflip_db.sales_curated` with zero query downtime during releases.
- **AWS Lambda**: `dataflip-processor` microservice triggered automatically by S3 ObjectCreated events, performing schema validation, optimistic locking retry, and Glue catalog pointer flips.
- **AWS IAM**: Scoped least-privilege policy restricting Lambda strictly to the target S3 bucket, Glue catalog database/table, and CloudWatch log group.

---

## 2. Directory Structure

```text
dataflip/
├── lambda/
│   └── handler.py              # AWS Lambda release & rollback microservice
├── sql/
│   ├── athena_ddl.sql          # AWS Glue Data Catalog table DDL
│   └── athena_queries.sql      # Athena production analytics queries
├── terraform/
│   ├── main.tf                 # Terraform provider & backend configuration
│   ├── variables.tf            # Project variables
│   ├── outputs.tf              # Resource outputs (Lambda ARN, S3 bucket)
│   ├── s3.tf                   # S3 bucket, encryption, TLS, lifecycle rules
│   ├── glue.tf                 # AWS Glue database & table definitions
│   ├── lambda.tf               # AWS Lambda packaging & function definition
│   ├── iam.tf                  # Scoped least-privilege execution roles
│   └── s3_notification.tf      # S3 bucket notification & Lambda invocation permission
├── tests/
│   └── test_lambda_handler.py  # Unit tests for AWS Lambda logic & S3 trigger
├── docs/
│   ├── aws_cloud_architecture.md # Cloud architecture deep-dive
│   ├── cloud_interview_qa.md     # Technical interview preparation guide
│   ├── iam_policy.json           # Scoped IAM policy reference
│   └── walkthrough.md            # System architecture walkthrough
├── requirements.txt            # Python dependencies (boto3, pandas, pyarrow, powertools, pytest)
├── pytest.ini                  # Pytest configuration
└── README.md                   # Complete system documentation
```

---

## 3. Automated Test Verification Results (`pytest`)

Ran `pytest` inside the project root:

```text
============================= test session starts =============================
platform win32 -- Python 3.14.2, pytest-9.1.1, pluggy-1.6.0
rootdir: D:\cloud_projects\dataflip
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.12.1, typeguard-4.6.0
collected 9 items

tests\test_lambda_handler.py .........                                   [100%]

============================== 9 passed in 2.38s ==============================
```

---

## 4. Terraform Security & Validation

Validated Terraform configuration:

```bash
cd terraform
terraform validate  # SUCCESS ("Success! The configuration is valid.")
```

---

## 5. Cost Safety & Free-Tier Optimization

* **AWS Lambda**: Covered 100% by the 1,000,000 free monthly request tier.
* **Amazon S3**: Micro Parquet datasets consume less than 0.01% of free tier storage.
* **AWS Glue Data Catalog**: Metadata storage and requests under 1,000,000 per month are free.
* **Amazon Athena**: Queries scan only compressed columnar Parquet, minimizing bytes scanned per query.
