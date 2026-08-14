# DataFlip — Cloud-Based Analytics Data Switching System

**DataFlip** is an enterprise-grade AWS Cloud Engineering project implementing a **Blue/Green Data Deployment Pattern**. It prevents candidate analytics datasets from directly overwriting active production data by establishing an automated validation gateway and atomic Glue Catalog pointer switching.

---

## 🎯 Cloud-First Engineering Focus

This project is built primarily as an **AWS Cloud Engineering** project:

```text
AWS Cloud Architecture & Security    85%
Data Analytics (Parquet / SQL)       10%
Automation & Terraform Safety         5%
```

---

## 🏛️ Cloud Architecture Overview

```text
                         AWS CLOUD
                             │
                             │
                           S3
                             │
              ┌──────────────┼──────────────┐
              │              │              │
             RAW            BLUE           GREEN
              │              │              │
              └──────────────┴──────────────┘
                             │
                          Lambda
                             │
                    Validate / Process
                             │
                             ↓
                    Glue Data Catalog
                             │
                             ↓
                          Athena
                             │
                             ↓
                       SQL Analytics
                             │
                         CloudWatch
```

---

## 📚 Core AWS Documentation & Guides

* 📘 [AWS Cloud Architecture & Engineering Deep-Dive](docs/aws_cloud_architecture.md): Complete analysis of S3 security, IAM execution roles, Lambda, Glue Catalog, Athena, and CloudWatch.
* 🎓 [Cloud Engineering Interview Preparation Guide](docs/cloud_interview_qa.md): Detailed answers to 15 key AWS Cloud interview questions.
* 📋 [Full Implementation & Test Walkthrough](docs/walkthrough.md): Complete phase-by-phase execution and verification log.

---

## 🚀 Quick Execution Guide

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Local Blue/Green Deployment Engine
```bash
python src/dataflip.py
```

### 3. Run Automated Unit Test Suite (17 Tests)
```bash
python -m pytest
```

### 4. Validate & Plan Infrastructure (Terraform)
```bash
cd terraform
terraform init
terraform fmt
terraform validate
terraform plan
```

> [!CAUTION]
> **TERRAFORM SAFETY RULE**: `terraform apply` is strictly prohibited. Terraform is used exclusively for infrastructure specification, validation, and planning.

---

## 🧪 Test Verification (17/17 Passing)

```text
tests\test_dataflip_local.py ......                                     [ 35%]
tests\test_lambda_handler.py .....                                      [ 64%]
tests\test_process_data.py ......                                       [100%]

============================== 17 passed in 1.12s ==============================
```

---

## 💰 AWS Cost Safety Guarantee

* **Serverless Compute**: Lambda charges $0.00 under 1,000,000 free monthly requests.
* **Storage**: Micro Parquet datasets (<1MB) consume less than 0.01% of free tier.
* **Catalog**: Glue Catalog updates are free under 1,000,000 requests/month.
* **Athena SQL**: Scanning small Parquet files costs <$0.0001 per query.
* **Always-Running Servers**: Zero EC2, zero ECS, zero NAT Gateway.
* **Net Monthly Cost**: **$0.00 (100% Free-Tier Safe)**.
