import os
import io
import re
import json
import time
import urllib.parse
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

logger = Logger(service="dataflip")

S3_BUCKET = os.environ.get('S3_BUCKET', 'dataflip-analytics-dev')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'dataflip_db')

READ_ONLY_GLUE_KEYS = {
    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'Owner',
    'IsMultiDialectView', 'IsMaterializedView'
}

def parse_s3_key(key: str) -> dict | None:
    """
    Parse an S3 object key to extract dataset_name, slot ('green' or 'blue'), and filename.
    Matches paths such as:
      - '<dataset>/green/<filename>.parquet'
      - 'curated/<dataset>/green/<filename>.parquet'
      - 'staging/<dataset>/green/<filename>.parquet'
    """
    match = re.search(r'(?:^|/)(?P<dataset>[a-zA-Z0-9_-]+)/(?P<slot>green|blue)/(?P<filename>[^/]+)$', key)
    if not match:
        return None
    return {
        'dataset_name': match.group('dataset'),
        'slot': match.group('slot'),
        'filename': match.group('filename')
    }

def pyarrow_to_glue_type(arrow_type: pa.DataType) -> str:
    """Map a PyArrow DataType to an AWS Glue / Athena Data Catalog column type."""
    if pa.types.is_boolean(arrow_type):
        return 'boolean'
    elif pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
        return 'smallint'
    elif pa.types.is_int32(arrow_type):
        return 'int'
    elif pa.types.is_int64(arrow_type):
        return 'bigint'
    elif pa.types.is_float32(arrow_type):
        return 'float'
    elif pa.types.is_float64(arrow_type):
        return 'double'
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return 'string'
    elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return 'date'
    elif pa.types.is_timestamp(arrow_type):
        return 'timestamp'
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return 'binary'
    elif pa.types.is_decimal(arrow_type):
        return f'decimal({arrow_type.precision},{arrow_type.scale})'
    else:
        return 'string'

def extract_glue_columns(schema: pa.Schema) -> list[dict]:
    """Convert PyArrow schema fields into AWS Glue Catalog column definitions."""
    return [{'Name': field.name, 'Type': pyarrow_to_glue_type(field.type)} for field in schema]

def inspect_and_validate_parquet(parquet_bytes: bytes) -> tuple[bool, str, list[dict], int]:
    """
    Inspect Parquet file schema and metadata using PyArrow without decoding table records into memory.
    Returns: (is_valid, validation_message, glue_columns, row_count)
    """
    if not parquet_bytes:
        return False, "Validation Failed: Empty binary payload (0 bytes)", [], 0

    try:
        reader = pq.ParquetFile(io.BytesIO(parquet_bytes))
        metadata = reader.metadata
        schema = reader.schema_arrow

        if schema is None or len(schema) == 0:
            return False, "Validation Failed: Parquet schema contains no columns", [], 0

        row_count = metadata.num_rows
        if row_count == 0:
            return False, "Validation Failed: Parquet dataset contains 0 rows", [], 0

        glue_columns = extract_glue_columns(schema)
        return True, f"Validation Passed: {len(glue_columns)} columns and {row_count} rows discovered", glue_columns, row_count
    except Exception as e:
        return False, f"Validation Failed: Invalid Parquet file - {str(e)}", [], 0

def validate_records(records: list[dict]) -> tuple[bool, str]:
    """Validate data records generically using PyArrow."""
    if not records:
        return False, "Validation Failed: Empty record set (0 rows)"
    try:
        table = pa.Table.from_pylist(records)
        if table.num_rows == 0:
            return False, "Validation Failed: Empty record set (0 rows)"
        if table.num_columns == 0:
            return False, "Validation Failed: Dataset has no columns"
    except Exception as e:
        return False, f"Validation Failed: {e}"
    return True, f"Validation Passed: {len(records)} records validated successfully"

def promote_dataset(glue_client, database: str, dataset_name: str, new_s3_location: str, glue_columns: list[dict], max_retries: int = 3):
    """
    Create or update Glue Data Catalog table definition (schema and S3 storage location)
    to correspond to the promoted GREEN dataset, with optimistic locking & backoff retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            table_exists = True
            try:
                response = glue_client.get_table(DatabaseName=database, Name=dataset_name)
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') == 'EntityNotFoundException':
                    table_exists = False
                else:
                    raise

            if table_exists:
                table_input = {k: v for k, v in response['Table'].items() if k not in READ_ONLY_GLUE_KEYS}
                table_input['StorageDescriptor']['Location'] = new_s3_location
                table_input['StorageDescriptor']['Columns'] = glue_columns
                glue_client.update_table(DatabaseName=database, TableInput=table_input)
                logger.info(f"[Glue Update Success] Attempt {attempt}/{max_retries}: Updated '{database}.{dataset_name}' to '{new_s3_location}' with {len(glue_columns)} columns")
                return
            else:
                table_input = {
                    'Name': dataset_name,
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'classification': 'parquet',
                        'has_encrypted_data': 'true',
                        'parquet.compression': 'SNAPPY'
                    },
                    'StorageDescriptor': {
                        'Location': new_s3_location,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'Name': 'ParquetHiveSerDe',
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {'serialization.format': '1'}
                        },
                        'Columns': glue_columns
                    }
                }
                glue_client.create_table(DatabaseName=database, TableInput=table_input)
                logger.info(f"[Glue Create Success] Created new table '{database}.{dataset_name}' pointing to '{new_s3_location}' with {len(glue_columns)} columns")
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

def rollback_dataset(glue_client, database: str, dataset_name: str, blue_s3_location: str, max_retries: int = 3):
    """
    Roll back Glue Data Catalog table location to the known-good BLUE dataset prefix.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = glue_client.get_table(DatabaseName=database, Name=dataset_name)
            table_input = {k: v for k, v in response['Table'].items() if k not in READ_ONLY_GLUE_KEYS}
            table_input['StorageDescriptor']['Location'] = blue_s3_location

            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            logger.info(f"[Rollback Success] Reverted '{database}.{dataset_name}' to '{blue_s3_location}'")
            return
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ['ConcurrentModificationException', 'AlreadyExistsException'] and attempt < max_retries:
                backoff_sec = (2 ** attempt) * 0.1
                logger.warning(f"[Glue Rollback Retry] {code} detected. Retrying in {backoff_sec:.2f}s...")
                time.sleep(backoff_sec)
            else:
                logger.error(f"[Rollback Error] Non-retryable error: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"[Rollback Error] Attempt {attempt}/{max_retries} failed: {e}", exc_info=True)
            if attempt >= max_retries:
                raise

def _write_s3_manifest(s3_client, bucket: str, dataset_name: str, active_slot: str, status: str, message: str, location: str, extra: dict = None) -> None:
    """Write dataset deployment manifest metadata to S3."""
    payload = {
        'dataset_name': dataset_name,
        'active_slot': active_slot,
        'status': status,
        'message': message,
        's3_location': location,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    if extra:
        payload.update(extra)
    manifest_key = f"{dataset_name}/manifest.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(payload, indent=2)
    )

def lambda_handler(event, context):
    """
    Domain-agnostic AWS Lambda entrypoint for DataFlip Blue/Green release and rollback.
    Extracts dataset name dynamically, inspects Parquet schema via PyArrow, and updates
    Glue Catalog table definition (columns + location).
    """
    start_time = time.time()
    request_id = getattr(context, 'aws_request_id', 'local-test-id')
    logger.append_keys(request_id=request_id)
    logger.info(f"[Invocation Start] Event: {json.dumps(event)}")

    s3_client, glue_client = boto3.client('s3'), boto3.client('glue')

    # 1. Handle Rollback Action
    if isinstance(event, dict) and event.get('action') == 'rollback':
        dataset_name = event.get('dataset_name') or event.get('dataset')
        if not dataset_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'status': 'ERROR', 'message': 'dataset_name is required for rollback action'})
            }
        
        blue_location = f"s3://{S3_BUCKET}/{dataset_name}/blue/"
        try:
            rollback_dataset(glue_client, GLUE_DATABASE, dataset_name, blue_location)
            _write_s3_manifest(s3_client, S3_BUCKET, dataset_name, 'blue', 'rolled_back',
                               f"Production dataset reverted to BLUE", blue_location)
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'SUCCESS',
                    'message': f"Rollback completed to BLUE for dataset '{dataset_name}'",
                    'dataset_name': dataset_name,
                    'active_location': blue_location,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.error(f"[Rollback Exception] {e}", exc_info=True)
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}

    # 2. Ingestion Handling (S3 Event Notification vs Direct Payload)
    dataset_name = None
    glue_columns = []
    row_count = 0
    source_info = "manual"
    is_valid = False
    validation_msg = ""

    # Check for S3 event trigger
    if isinstance(event, dict) and "Records" in event and event["Records"] and "s3" in event["Records"][0]:
        s3_meta = event["Records"][0]["s3"]
        bucket_name = s3_meta.get("bucket", {}).get("name", S3_BUCKET)
        object_key = urllib.parse.unquote_plus(s3_meta.get("object", {}).get("key", ""))
        source_info = f"s3://{bucket_name}/{object_key}"
        logger.info(f"Processing candidate data from S3 event: {source_info}")

        parsed = parse_s3_key(object_key)
        if not parsed:
            logger.info(f"Ignoring non-dataset or non-deployment S3 key: {object_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'IGNORED', 'reason': 'Key does not match dataset green/blue pattern', 'key': object_key})
            }

        dataset_name = parsed['dataset_name']
        slot = parsed['slot']

        if slot != 'green':
            logger.info(f"Ignoring upload to '{slot}' slot: {object_key}")
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'IGNORED', 'reason': f"Upload to '{slot}' slot does not trigger promotion", 'key': object_key})
            }

        try:
            obj_resp = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            parquet_bytes = obj_resp["Body"].read()
            is_valid, validation_msg, glue_columns, row_count = inspect_and_validate_parquet(parquet_bytes)
        except Exception as e:
            logger.error(f"Failed to read/inspect Parquet object from {source_info}: {e}", exc_info=True)
            is_valid, validation_msg = False, f"Failed to retrieve/parse Parquet object: {e}"

    # Check for direct record payload (testing / programmatic API)
    elif isinstance(event, dict) and ("records" in event or "schema" in event):
        dataset_name = event.get("dataset_name") or event.get("dataset", "default_dataset")
        source_info = f"payload:{dataset_name}"
        if "schema" in event:
            glue_columns = event["schema"]
            row_count = event.get("row_count", 1)
            is_valid = len(glue_columns) > 0 and row_count > 0
            validation_msg = f"Payload schema provided with {len(glue_columns)} columns" if is_valid else "Empty schema or 0 rows"
        else:
            records = event.get("records", [])
            valid_recs, rec_msg = validate_records(records)
            if valid_recs:
                try:
                    table = pa.Table.from_pylist(records)
                    glue_columns = extract_glue_columns(table.schema)
                    row_count = table.num_rows
                    is_valid = True
                    validation_msg = f"Validation Passed: {len(glue_columns)} columns and {row_count} rows extracted"
                except Exception as e:
                    is_valid, validation_msg = False, f"Validation Failed: {e}"
            else:
                is_valid, validation_msg = False, rec_msg
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'status': 'ERROR', 'message': 'Unsupported invocation event format'})
        }

    # 3. Promotion or Rejection
    green_location = f"s3://{S3_BUCKET}/{dataset_name}/green/"
    blue_location = f"s3://{S3_BUCKET}/{dataset_name}/blue/"

    if is_valid:
        try:
            promote_dataset(glue_client, GLUE_DATABASE, dataset_name, green_location, glue_columns)
            _write_s3_manifest(s3_client, S3_BUCKET, dataset_name, 'green', 'active',
                               validation_msg, green_location, {
                                   'source': source_info,
                                   'columns': glue_columns,
                                   'row_count': row_count
                               })
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'ACTIVATED_GREEN',
                    'dataset_name': dataset_name,
                    'message': validation_msg,
                    'active_location': green_location,
                    'columns': glue_columns,
                    'row_count': row_count,
                    'source': source_info,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.error(f"[Activation Error] {e}", exc_info=True)
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}
    else:
        try:
            _write_s3_manifest(s3_client, S3_BUCKET, dataset_name or "unknown", 'blue',
                               'retained_on_failure', validation_msg, blue_location, {
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
                'dataset_name': dataset_name,
                'reason': validation_msg,
                'active_location': blue_location,
                'source': source_info,
                'execution_time_ms': duration_ms
            })
        }
