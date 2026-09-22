# DataFlip — Complete AWS Architecture Walkthrough

## Executive Summary

**DataFlip — Serverless Blue/Green Data Deployment Platform** is a domain-agnostic, dataset-driven architecture implementing zero-downtime dataset promotion and rollback for analytical data lakes in AWS.

All components adhere strictly to cloud-first AWS principles, Terraform Infrastructure as Code (IaC), least-privilege security, and metadata pointer promotion via AWS Glue Data Catalog.

---

## 1. Production Architecture Flow

```text
S3 (<dataset_name>/green/*.parquet) 
       ↓ (s3:ObjectCreated notification)
AWS Lambda (handler.py) 
       ↓ (PyArrow metadata-only schema discovery & validation)
Validation PASSED?
    ├── YES → Update/Create Glue Table (dataflip_db.<dataset_name>)
    │         ├── Location: s3://.../<dataset_name>/green/ (GREEN Activated)
    │         └── Columns: PyArrow-discovered schema definitions
    └── NO  → Retain Location → s3://.../<dataset_name>/blue/ (BLUE Retained)
                 ↓ (Rollback Invocation by dataset_name)
              Revert Location → s3://.../<dataset_name>/blue/ (Instant Rollback)
```

- **Amazon S3**:
  - `s3://${S3_BUCKET}/<dataset_name>/blue/`: Production known-good dataset.
  - `s3://${S3_BUCKET}/<dataset_name>/green/`: Candidate dataset staging.
  - `s3://${S3_BUCKET}/<dataset_name>/manifest.json`: Deployment state audit ledger.
- **AWS Glue Data Catalog**: Database `dataflip_db` containing dynamically provisioned tables `dataflip_db.<dataset_name>` whose `StorageDescriptor.Location` and `StorageDescriptor.Columns` serve as the **authoritative pointer** for all downstream analytical queries.
- **Amazon Athena**: Decoupled SQL query engine reading directly from `dataflip_db.<dataset_name>` with zero query downtime during releases.
- **AWS Lambda**: `dataflip-processor` microservice triggered automatically by S3 ObjectCreated events, performing PyArrow schema discovery, optimistic locking retry, dynamic table creation/updating, and Glue catalog pointer flips.
- **AWS IAM**: Scoped least-privilege policy restricting Lambda strictly to the target S3 bucket, Glue catalog database and dynamic tables (`dataflip_db/*`), and CloudWatch log group.

---

## 2. Directory Structure

```text
dataflip/
├── lambda/
│   └── handler.py              # Domain-agnostic Blue/Green release & rollback engine
├── sql/
│   ├── athena_ddl.sql          # AWS Glue Data Catalog database & reference DDL templates
│   └── athena_queries.sql      # Domain-agnostic Athena analytical queries
├── terraform/
│   ├── main.tf                 # Terraform provider & backend configuration
│   ├── variables.tf            # Project variables
│   ├── outputs.tf              # Resource outputs (Lambda ARN, S3 bucket, Glue DB)
│   ├── s3.tf                   # S3 bucket, encryption, TLS, lifecycle rules
│   ├── glue.tf                 # AWS Glue database definition (dynamic table provisioning)
│   ├── lambda.tf               # AWS Lambda packaging & function definition
│   ├── iam.tf                  # Scoped least-privilege execution roles
│   └── s3_notification.tf      # S3 bucket notification & Lambda invocation permission
├── docs/
│   ├── aws_cloud_architecture.md # Cloud architecture deep-dive
│   ├── cloud_interview_qa.md     # Technical interview preparation guide
│   ├── iam_policy.json           # Scoped IAM policy reference
│   └── walkthrough.md            # System architecture walkthrough
├── requirements.txt            # Production dependencies (boto3, pyarrow, aws-lambda-powertools)
├── LICENSE                     # MIT License
└── README.md                   # Complete system documentation
```

---

## 3. Production Architecture Verification

The repository contains strictly the functional application code, infrastructure as code, and reference architecture documentation. All core components are designed for direct serverless deployment:

1. **Lambda Engine**: Clean Python 3.11 implementation in `lambda/handler.py` supporting flat S3 key parsing (`<dataset_name>/green/`), schema discovery, Glue catalog promotion, optimistic locking retry, and rollback.
2. **Infrastructure**: Complete, verified Terraform modules for S3, Glue, Lambda, and IAM.

---

## 4. Terraform Security & Validation

Validated Terraform configuration:

```bash
cd terraform
terraform validate  # SUCCESS ("Success! The configuration is valid.")
```

---

## 5. Cost Safety & Free-Tier Optimization

* **Zero Idle Compute**: AWS Lambda operates strictly on-demand.
* **Storage**: Parquet compressed datasets consume minimal space under the 5GB S3 Free Tier.
* **Decoupled Architecture**: Zero EC2 instances, zero NAT gateways, zero always-running servers.
