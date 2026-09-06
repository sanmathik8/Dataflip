import os
import io
import json
import time
import urllib.parse
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

logger = Logger(service="dataflip")

S3_BUCKET = os.environ.get('S3_BUCKET', 'dataflip-analytics-dev')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'dataflip_db')
GLUE_TABLE = os.environ.get('GLUE_TABLE', 'sales_curated')

READ_ONLY_GLUE_KEYS = {
    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'Owner',
    'IsMultiDialectView', 'IsMaterializedView'
}

def validate_records(records: list[dict]) -> tuple[bool, str]:
    """Validate data records generically in memory using Pandas."""
    if not records:
        return False, "Validation Failed: Empty record set (0 rows)"

    try:
        df = pd.DataFrame(records)
        if df.empty:
            return False, "Validation Failed: Empty record set (0 rows)"
        if len(df.columns) == 0:
            return False, "Validation Failed: Dataset has no columns"
    except Exception as e:
        return False, f"Validation Failed: {e}"

    return True, f"Validation Passed: {len(records)} records validated successfully"

def update_glue_table_location(glue_client, database: str, table_name: str, new_s3_location: str, max_retries: int = 3):
    """Update Glue Catalog Table S3 Location with optimistic locking & backoff retries."""
    for attempt in range(1, max_retries + 1):
        try:
            response = glue_client.get_table(DatabaseName=database, Name=table_name)
            table_input = {k: v for k, v in response['Table'].items() if k not in READ_ONLY_GLUE_KEYS}
            table_input['StorageDescriptor']['Location'] = new_s3_location

            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            logger.info(f"[Glue Switch Success] Attempt {attempt}/{max_retries}: Updated '{database}.{table_name}' to '{new_s3_location}'")
            return
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ['ConcurrentModificationException', 'AlreadyExistsException'] and attempt < max_retries:
                backoff_sec = (2 ** attempt) * 0.1
                logger.warning(f"[Glue Retry] {code} detected. Retrying in {backoff_sec:.2f}s...")
                time.sleep(backoff_sec)
            else:
                logger.error(f"[Glue Error] Non-retryable error: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"[Glue Error] Attempt {attempt}/{max_retries} failed: {e}", exc_info=True)
            if attempt >= max_retries:
                raise

def _write_s3_manifest(s3_client, active_dataset: str, status: str, message: str, location: str, extra: dict = None) -> None:
    """Write deployment manifest metadata to S3."""
    payload = {
        'active_dataset': active_dataset,
        'status': status,
        'message': message,
        's3_location': location,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    if extra:
        payload.update(extra)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key='curated/active_manifest.json',
        Body=json.dumps(payload, indent=2)
    )

def lambda_handler(event, context):
    """AWS Lambda entrypoint for DataFlip Blue/Green release & rollback."""
    start_time = time.time()
    request_id = getattr(context, 'aws_request_id', 'local-test-id')
    logger.append_keys(request_id=request_id, dataset=GLUE_TABLE)
    logger.info(f"[Invocation Start] Event: {json.dumps(event)}")

    s3_client, glue_client = boto3.client('s3'), boto3.client('glue')

    # 1. Handle Rollback Action
    if isinstance(event, dict) and event.get('action') == 'rollback':
        blue_location = f"s3://{S3_BUCKET}/curated/blue/"
        try:
            update_glue_table_location(glue_client, GLUE_DATABASE, GLUE_TABLE, blue_location)
            _write_s3_manifest(s3_client, 'blue', 'rolled_back', 'Production dataset reverted to BLUE', blue_location)
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'SUCCESS',
                    'message': 'Rollback completed to BLUE',
                    'active_location': blue_location,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.error(f"[Rollback Exception] {e}", exc_info=True)
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}

    # 2. Candidate Data Ingestion (Manual Records vs. S3 Event Notification)
    records = []
    source_info = "manual"

    if isinstance(event, dict) and "Records" in event and event["Records"] and "s3" in event["Records"][0]:
        s3_meta = event["Records"][0]["s3"]
        bucket_name = s3_meta.get("bucket", {}).get("name", S3_BUCKET)
        object_key = urllib.parse.unquote_plus(s3_meta.get("object", {}).get("key", ""))
        source_info = f"s3://{bucket_name}/{object_key}"
        logger.info(f"Processing candidate data from S3 event: {source_info}")
        try:
            obj_resp = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            parquet_bytes = obj_resp["Body"].read()
            df = pd.read_parquet(io.BytesIO(parquet_bytes))
            records = df.to_dict(orient="records")
        except Exception as e:
            logger.error(f"Failed to read Parquet object from {source_info}: {e}", exc_info=True)
            records = []
    elif isinstance(event, dict) and "records" in event:
        records = event.get("records", [])

    is_valid, validation_msg = validate_records(records)

    if is_valid:
        green_location = f"s3://{S3_BUCKET}/curated/green/"
        try:
            update_glue_table_location(glue_client, GLUE_DATABASE, GLUE_TABLE, green_location)
            _write_s3_manifest(s3_client, 'green', 'active', validation_msg, green_location, {'source': source_info})
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'ACTIVATED_GREEN',
                    'message': validation_msg,
                    'active_location': green_location,
                    'source': source_info,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.error(f"[Activation Error] {e}", exc_info=True)
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}
    else:
        blue_location = f"s3://{S3_BUCKET}/curated/blue/"
        try:
            _write_s3_manifest(s3_client, 'blue', 'retained_on_failure', validation_msg, blue_location, {
                'rejection_reason': validation_msg,
                'source': source_info
            })
        except Exception as e:
            logger.error(f"[Manifest Error] {e}", exc_info=True)

        duration_ms = round((time.time() - start_time) * 1000, 2)
        return {
            'statusCode': 422,
            'body': json.dumps({
                'status': 'REJECTED_GREEN',
                'reason': validation_msg,
                'active_location': blue_location,
                'source': source_info,
                'execution_time_ms': duration_ms
            })
        }
