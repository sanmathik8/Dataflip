# DataFlip — Cloud Engineering Interview Preparation Guide

This guide provides deep technical answers for the 15 key AWS Cloud Engineering interview questions based on the DataFlip architecture.

---

### Q1: Why use Amazon S3 for analytics data storage?
**Answer**: Amazon S3 provides highly durable (99.999999999% / 11 9s), scalable, and cost-effective object storage decoupled from compute. S3 natively integrates with serverless query engines (Athena) and metadata catalogs (Glue). By organizing data into prefix conventions (`raw/`, `curated/blue/`, `curated/green/`), S3 acts as a serverless data lake foundation without managing disk partitions or database servers.

---

### Q2: Why separate datasets into BLUE and GREEN in S3?
**Answer**: Separating candidate data (`GREEN`) from active production data (`BLUE`) implements the Blue/Green deployment pattern for data pipelines. It prevents corrupted, incomplete, or ill-formatted datasets from directly overwriting active production data consumed by downstream BI dashboards (Athena/QuickSight). `BLUE` remains active until `GREEN` passes all quality validation checks.

---

### Q3: Why use AWS Lambda for the data validation and switching logic?
**Answer**: AWS Lambda is an event-driven, serverless compute service. It eliminates the cost and operational overhead of running 24/7 EC2 servers for occasional batch data updates. Lambda scales automatically on-demand, executes validation checks in seconds, and updates Glue Catalog metadata cleanly via `boto3`.

---

### Q4: How does AWS Lambda get permission to access S3 objects?
**Answer**: Lambda gets permission via an **IAM Execution Role** attached to the function. When Lambda executes, AWS Security Token Service (STS) assumes this role and issues temporary short-lived credentials (`AccessKeyId`, `SecretAccessKey`, `SessionToken`) to the execution environment. The IAM policy attached to the role explicitly allows `s3:GetObject` and `s3:PutObject` on the target bucket ARN.

---

### Q5: What is an IAM Execution Role and how does a Trust Policy work?
**Answer**: An IAM Execution Role is an IAM identity created for an AWS service (like Lambda) rather than a human user. A **Trust Policy** defines which service principals are allowed to assume the role. For Lambda, the trust policy specifies `"Principal": {"Service": "lambda.amazonaws.com"}` with action `"sts:AssumeRole"`.

---

### Q6: Why use the AWS Glue Data Catalog instead of directly pointing Athena to S3 files?
**Answer**: The AWS Glue Data Catalog serves as a centralized, Hive-compatible metadata repository. It decouples table definitions and schemas from physical S3 storage paths. By updating table location metadata in Glue (`glue:UpdateTable`), Athena queries immediately read the new dataset without changing SQL queries, recoding application logic, or moving physical files.

---

### Q7: How does Amazon Athena know where to find and read the data?
**Answer**: Athena does not store data. When a SQL query is submitted (`SELECT * FROM dataflip_db.sales_curated`), Athena queries the Glue Data Catalog for the table's `StorageDescriptor`. Glue returns the target S3 path (`s3://bucket/curated/green/`), file format (Parquet), and schema serializer/deserializer (`SerDe`). Athena then reads the Parquet objects directly from S3.

---

### Q8: Why use Apache Parquet instead of CSV for curated analytics data?
**Answer**: Parquet is a columnar, compressed binary storage format optimized for analytical queries. Unlike CSV (which requires scanning every row and column), Parquet allows Athena to scan only the specific columns requested in the `SELECT` clause (projection pushdown) and filter row groups using metadata statistics (predicate pushdown). This reduces data scanned by up to 90%, speeding up queries and drastically reducing Athena costs.

---

### Q9: How does the DataFlip dataset switch actually happen under the hood?
**Answer**: The switch is an atomic metadata operation in the AWS Glue Data Catalog. Lambda calls `boto3.client('glue').update_table()`, updating `StorageDescriptor.Location` from `s3://bucket/curated/blue/` to `s3://bucket/curated/green/`. No data files are copied or moved in S3, making the switch instantaneous and zero-cost.

---

### Q10: How does automated or manual Rollback work?
**Answer**: If a problem is detected after activating GREEN, rollback is executed by updating the Glue Data Catalog table location back to `s3://bucket/curated/blue/`. Because the original `BLUE` dataset was preserved in S3, production queries immediately revert to reading known-good data without data recovery procedures.

---

### Q11: How does Amazon CloudWatch monitor AWS Lambda executions?
**Answer**: AWS Lambda automatically integrates with CloudWatch Logs. Every standard `print()` or Python `logging` statement executed inside `lambda_handler` is streamed directly to CloudWatch Log Group `/aws/lambda/dataflip-processor`. CloudWatch tracks invocation counts, duration, memory utilization, and errors natively.

---

### Q12: What happens when the Lambda function fails during execution?
**Answer**: If Lambda fails (e.g., validation exception or unhandled runtime error), the Glue Data Catalog update call is never executed. The catalog table location remains unchanged, pointing securely to `s3://bucket/curated/blue/`. CloudWatch records the error traceback for alerting.

---

### Q13: How is Amazon S3 secured in this architecture?
**Answer**: S3 is secured through 4 defense-in-depth layers:
1. **Block Public Access**: Explicitly enabled to prevent public bucket access.
2. **IAM Least Privilege**: Access restricted to the specific Lambda execution role ARN.
3. **Server-Side Encryption**: SSE-S3 (`AES256`) encrypts all stored objects.
4. **Bucket Policies**: Enforce TLS/HTTPS transit security (`aws:SecureTransport`).

---

### Q14: How does this architecture remain virtually free (low-cost)?
**Answer**: The architecture is 100% serverless with zero always-running servers:
* S3 micro-datasets consume <1MB (Free Tier provides 5GB).
* Lambda invocations fit well within 1,000,000 free monthly requests.
* Glue Catalog operations fall within 1,000,000 free monthly requests.
* Athena charges $5/TB scanned; scanning kilobytes costs <$0.0001 per query.

---

### Q15: What would change for a production-scale implementation?
**Answer**: At enterprise scale:
1. **S3 Partitioning**: Add date partitioning (`curated/year=2026/month=01/day=15/`) to limit Athena scanning scope.
2. **EventBridge & Step Functions**: Use AWS Step Functions to orchestrate multi-step validation workflows across multiple Glue catalog tables.
3. **AWS Lake Formation**: Implement fine-grained column-level and row-level access control over the Glue Data Catalog.
4. **CloudWatch Alarms & SNS**: Trigger automated PagerDuty/Slack notifications upon validation failure.
