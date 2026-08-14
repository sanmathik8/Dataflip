# 🔄 DataFlip — Zero-Downtime Blue/Green Data Switching

[![Amazon S3](https://img.shields.io/badge/AWS-Amazon_S3-569A31?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS-Glue_Catalog-FF9900?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/glue/)
[![Amazon Athena](https://img.shields.io/badge/AWS-Amazon_Athena-38BDF8?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/athena/)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-844FBA?logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Pytest](https://img.shields.io/badge/Testing-Pytest_17_Passed-0A9EDC?logo=pytest&logoColor=white)](https://docs.pytest.org/)

A cloud-based analytics data switching system on AWS implementing a **zero-downtime, zero-copy Blue/Green Data Deployment Pattern**.

---

## 🎯 Architectural Overview

DataFlip enables instant releases and rollbacks of analytical cloud datasets without copying data or corrupting active SQL queries. Candidate Parquet datasets stage in `curated/green/` S3 prefixes. An **AWS Lambda** validation handler verifies row counts and non-null constraints before updating `StorageDescriptor.Location` in the **AWS Glue Data Catalog** via Boto3, switching live **Amazon Athena** SQL queries in milliseconds.

```mermaid
flowchart LR
    A[🪣 S3 Candidate Dataset] --> B[⚡ AWS Lambda Validator]
    B -->|Validation Pass| C[🗄️ AWS Glue Data Catalog]
    C -->|Update Location Pointer| D[📊 Amazon Athena SQL]
    B -.->|Validation Fail| E[🔒 Retain Blue Pointer]
```

---

## ⚡ Key Engineering Features

- **⚡ Atomic Blue/Green Metadata Switching:** Updates Glue Data Catalog table location metadata properties (`StorageDescriptor.Location`) via Boto3, executing instant dataset switches.
- **🛡️ Deterministic Lambda Validation:** Runs row-count checks, schema validation, and non-null constraint checks before promoting candidate datasets.
- **💰 Zero Data-Copy Deployment:** Eliminates dataset copying costs and network latency, operating at a $0.00 baseline cost under the AWS Free Tier.
- **🧪 17-Test Pytest Suite:** Includes comprehensive unit and integration tests (`test_dataflip_local.py`, `test_lambda_handler.py`, `test_process_data.py`).
- **🏗️ Terraform IaC:** Declaratively provisions S3 buckets, Glue Data Catalog tables (`dataflip_db.sales_curated`), Lambda validation functions, and IAM policies (`s3.tf`, `glue.tf`, `lambda.tf`, `iam.tf`).

---

## 🛠️ Technology Stack

- **Cloud Services:** Amazon S3, AWS Glue Data Catalog, Amazon Athena, AWS Lambda, CloudWatch
- **Languages & Libraries:** Python, Boto3 SDK, Apache Parquet
- **Testing & IaC:** Pytest (17 passing integration tests), Terraform 1.14+

---

## 🚀 Quickstart & Usage

### 1. Run Local Integration Tests
```bash
pytest tests/
```

### 2. Deploy Infrastructure via Terraform
```bash
cd terraform
terraform init
terraform apply
```

---

## 📄 License
Distributed under the MIT License.
