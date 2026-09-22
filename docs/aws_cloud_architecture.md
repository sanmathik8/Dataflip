# DataFlip — AWS Cloud Architecture & Engineering Deep-Dive

**DataFlip** is a domain-agnostic, serverless AWS data deployment platform implementing a **Blue/Green Data Deployment Pattern**. This document outlines the core AWS Cloud Engineering pillars powering DataFlip.

---

## 📐 Official AWS Cloud Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion_Storage [Amazon S3 — Serverless Storage Layer]
        GREEN_S3["🪣 Amazon S3 <dataset_name>/green/ Prefix — Candidate Staging"]
        BLUE_S3["🪣 Amazon S3 <dataset_name>/blue/ Prefix — Production Known-Good"]
        MANIFEST_S3["📋 Amazon S3 <dataset_name>/manifest.json — Audit Ledger"]
    end

    subgraph Validation_Compute [AWS Lambda — Serverless Compute Engine]
        LAMBDA["⚡ AWS Lambda Function: dataflip-processor"]
        PARSER["🔍 Dataset Key Parser"]
        ARROW["📊 PyArrow Metadata-Only Schema Discovery"]
        VALIDATOR["✅ Structural Validation (Columns Exist, Rows > 0)"]
        TYPE_MAP["🔄 PyArrow to Glue Data Type Mapping"]
        RETRY_LOOP["🔁 Metastore Retry Loop (Optimistic Lock Backoff)"]
    end

    subgraph Metastore_Catalog [AWS Glue Data Catalog — Central Metadata Repository]
        GLUE_DB["🗄️ AWS Glue Database: dataflip_db"]
        GLUE_TBL["📋 AWS Glue External Table: dataflip_db.<dataset_name>"]
        POINTER["StorageDescriptor.Location & Columns Pointer"]
    end

    subgraph Analytics_Execution [Amazon Athena — Decoupled SQL Query Engine]
        ATHENA_SQL["📊 Amazon Athena SQL Engine"]
        BI_DASH["📈 QuickSight / BI Analytics Dashboards"]
    end

    subgraph Security_Observability [AWS Identity, Security & Management]
        IAM_ROLE["🔐 AWS IAM Execution Role & Scoped Policies"]
        CW_LOGS["🪵 Amazon CloudWatch Logs: /aws/lambda/dataflip-processor"]
    end

    GREEN_S3 -->|s3:ObjectCreated (*.parquet)| LAMBDA
    IAM_ROLE -->|Grant Least-Privilege STS Credentials| LAMBDA
    LAMBDA --> PARSER
    PARSER --> ARROW
    ARROW --> VALIDATOR
    VALIDATOR -->|Validation PASSED| TYPE_MAP
    TYPE_MAP --> RETRY_LOOP
    RETRY_LOOP -->|glue:CreateTable / glue:UpdateTable| GLUE_TBL
    GLUE_DB --> GLUE_TBL
    GLUE_TBL --> POINTER
    POINTER -->|Points to Active Location| GREEN_S3
    ATHENA_SQL -->|Read Table Definition| GLUE_TBL
    ATHENA_SQL -->|Query Active S3 Parquet Data| GREEN_S3
    ATHENA_SQL --> BI_DASH
    LAMBDA -->|Stream Execution Logs| CW_LOGS
    LAMBDA -->|Write Deployment Audit State| MANIFEST_S3

    VALIDATOR -.->|Validation FAILED / Rollback| BLUE_S3
    BLUE_S3 -.->|Retain / Revert Location Pointer| POINTER
```

---

## 1. Amazon S3 Storage Architecture & Security

### Bucket & Prefix Design
```text
s3://<bucket_name>/
├── <dataset_name_1>/
│   ├── green/               # Candidate Parquet dataset undergoing validation
│   ├── blue/                # Active / Known-good production Parquet dataset
│   └── manifest.json        # Per-dataset deployment audit trail
└── <dataset_name_2>/
    ├── green/
    ├── blue/
    └── manifest.json
```

### S3 Security Controls
* **Block Public Access**: Enabled across all 4 S3 public access block settings (`IgnorePublicAcls`, `BlockPublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`).
* **Server-Side Encryption**: SSE-S3 (`AES256`) applied by default to all objects at rest.
* **Least-Privilege Policy**: Bucket access is restricted exclusively to the Lambda execution role and authorized Athena query executions.
* **Versioning**: Enabled on dataset prefixes to prevent accidental deletion of historical known-good datasets.

---

## 2. AWS IAM Security & Least Privilege

### Lambda Execution Role (`dataflip-lambda-execution-role`)
Lambda receives permissions via an **IAM Execution Role** attached to the function execution context.

#### Trust Policy (`AssumeRole`)
Allows the Lambda service principal to assume the role:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

#### Permission Policy Breakdown
No `AdministratorAccess` is granted. Scoped permissions include:
1. **S3 Permissions**: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` restricted to the analytics bucket and objects (`arn:aws:s3:::<bucket>`, `arn:aws:s3:::<bucket>/*`).
2. **Glue Permissions**: `glue:GetTable`, `glue:UpdateTable`, `glue:CreateTable`, `glue:GetDatabase` restricted to `dataflip_db` and its tables (`arn:aws:glue:*:*:table/dataflip_db/*`).
3. **CloudWatch Logs**: `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` restricted to `/aws/lambda/dataflip-processor:*`.

---

## 3. AWS Lambda Serverless Processing Engine

### Execution Model & Trigger
* **Runtime**: Python 3.11 / Python 3.14 compatible.
* **Memory & Timeout**: 512 MB RAM, 30-second timeout.
* **Layers**: Configured via `lambda_layer_arns` in Terraform (provides `pyarrow` in AWS).
* **Environment Variables**:
  - `S3_BUCKET`: Analytics S3 bucket identifier.
  - `GLUE_DATABASE`: `dataflip_db`.

### Event Handler Logic (`lambda/handler.py`)
1. **Receives Candidate Upload**: Triggered automatically by S3 ObjectCreated events for `.parquet` objects.
2. **Extracts Dataset Context**: Dynamically parses the dataset name from the S3 key; non-`green/` uploads are safely ignored.
3. **Metadata-Only PyArrow Schema Discovery**: Reads the Parquet file metadata footer dictionary directly from memory buffer without decoding record batches.
4. **Structural Validation**: Ensures schema contains at least one column and `metadata.num_rows > 0`.
5. **Data Type Mapping**: Converts PyArrow data types into AWS Glue Data Catalog types (`int`, `bigint`, `double`, `string`, `date`, `timestamp`, `boolean`, `decimal`).
6. **Glue Catalog Promotion (PASS)**:
   - If the table does not exist: calls `glue.create_table()` creating an `EXTERNAL_TABLE` with Parquet SerDe, discovered columns, and location pointing to `green/`.
   - If the table exists: calls `glue.update_table()` updating `StorageDescriptor.Columns` and `StorageDescriptor.Location` with exponential backoff retry on `ConcurrentModificationException`.
   - Writes successful deployment audit state to `s3://<bucket>/<dataset_name>/manifest.json`.
7. **Handles Failure (FAIL)**:
   - Retains Glue Catalog location at `s3://<bucket>/<dataset_name>/blue/`.
   - Writes rejection reason to `manifest.json` and CloudWatch Logs.

### Design Note: Invocation Modes & Cloud Deployment Status
`lambda/handler.py` branches on the incoming event payload to support two distinct execution paths:
1. **Live S3 ObjectCreated Events**: Configured via `aws_s3_bucket_notification` in [`terraform/s3_notification.tf`](../terraform/s3_notification.tf) (filtered to `.parquet` suffixes) and authorized via `aws_lambda_permission.allow_s3_invocation`. Uploading candidate files to `<dataset>/green/` triggers automatic schema validation and catalog promotion.
2. **Direct JSON Invocation**: Programmatic invocation via AWS CLI (`aws lambda invoke`), SDK, or automated test suites:
   - Rollback commands: `{"action": "rollback", "dataset_name": "<name>"}`
   - Simulation / verification events: `{"Records": [{"s3": {...}}]}`

**Deployment Status**:
The S3 trigger infrastructure is fully defined and passes `terraform validate`. However, it is not currently `terraform apply`'d to a live AWS account (`terraform.tfstate` shows `resources: []`). This is deliberate, to avoid ongoing AWS idle costs during development and testing, rather than a missing architectural capability.

---

## 4. AWS Glue Data Catalog & Athena SQL Integration

### How Glue Metadata Drives Athena
Athena is a decoupled, serverless query engine. It relies entirely on the **AWS Glue Data Catalog** to resolve column schemas and physical S3 storage locations:

```text
Athena Query -> Glue Catalog (dataflip_db.<dataset_name>) -> S3 Parquet Data
```

### The Switching Mechanism
DataFlip updates production analytics **without physical data copying or query downtime** by updating the Glue table definition:

```python
glue_client.update_table(
    DatabaseName='dataflip_db',
    TableInput={
        'Name': dataset_name,
        'StorageDescriptor': {
            'Location': f's3://{bucket}/{dataset_name}/green/',
            'Columns': glue_columns
        }
    }
)
```

*Note on Architecture*: Updating the Glue table definition updates catalog metadata pointers to correspond to the promoted dataset; it does not provide relational database-style two-phase commit transactions.

---

## 5. Amazon Athena SQL Analytics

### Queries Against Dynamically Managed Datasets
Athena queries execute directly against the dynamically created Glue catalog tables:

```sql
-- Preview records from the active dataset deployment
SELECT * FROM dataflip_db.<dataset_name> LIMIT 10;

-- Audit record counts across active dataset
SELECT COUNT(*) AS total_records FROM dataflip_db.<dataset_name>;
```

### Cost Awareness & Performance
* **Columnar Parquet Format**: Athena scans only requested columns rather than full objects, minimizing bytes scanned.
* **Cost Model**: $5.00 per TB scanned. Micro-datasets in DataFlip scan kilobytes, resulting in negligible per-query costs.

---

## 6. Amazon CloudWatch Logging & Observability

* **Log Group**: `/aws/lambda/dataflip-processor`
* **Log Output**: Standardized logging powered by Python's built-in `logging` module, recording dataset names, execution durations, validation status, catalog updates, and rollback actions.

---

## 7. Event-Driven Architecture

```text
S3 Object Creation (<dataset_name>/green/*.parquet)
         │
         ▼
S3 Event Notification (filter_suffix = ".parquet")
         │
         ▼
AWS Lambda (handler.py)
         │
    ┌────┴────┐
    ▼         ▼
Glue Update   CloudWatch Log
```

---

## 8. AWS Cost Safety Architecture

* **Serverless Compute**: Lambda invocations fall well within the AWS Free Tier (1,000,000 free monthly requests).
* **Storage**: Parquet compressed datasets consume minimal S3 capacity.
* **Catalog**: Glue Data Catalog API calls fall within the free monthly allowance.
* **Query Engine**: Athena charges only per data scanned ($5/TB).
* **Infrastructure**: Zero EC2 instances, zero ECS clusters, zero NAT Gateways.
