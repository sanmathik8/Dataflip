import io
import json
import logging
import os
import re
import time
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger("dataflip")
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ.get('S3_BUCKET', 'dataflip-analytics-dev')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'dataflip_db')

READ_ONLY_GLUE_KEYS = {
    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'Owner',
    'IsMultiDialectView', 'IsMaterializedView'
}

ARROW_TO_GLUE_TYPES = {
    pa.bool_(): 'boolean',
    pa.int8(): 'smallint',
    pa.int16(): 'smallint',
    pa.int32(): 'int',
    pa.int64(): 'bigint',
    pa.float32(): 'float',
    pa.float64(): 'double',
    pa.string(): 'string',
    pa.large_string(): 'string',
    pa.date32(): 'date',
    pa.date64(): 'date',
    pa.binary(): 'binary',
    pa.large_binary(): 'binary',
}


def sanitize_dataset_name(raw_name: str) -> str:
    """Sanitize dataset name for AWS Glue table naming (lowercase alphanumeric and underscores)."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', raw_name).strip('_').lower()
    return cleaned or "default_dataset"


def parse_s3_key(key: str) -> dict | None:
    """
    Extract dataset_name, slot ('green' or 'blue'), and filename from a flat S3 key:
      <dataset_name>/<slot>/<filename>
    """
    match = re.match(r'^(?P<dataset>[a-zA-Z0-9_-]+)/(?P<slot>green|blue)/(?P<filename>[^/]+)$', key)
    if not match:
        return None
    return {
        'dataset_name': sanitize_dataset_name(match.group('dataset')),
        'slot': match.group('slot'),
        'filename': match.group('filename')
    }


def pyarrow_to_glue_type(arrow_type: pa.DataType) -> str:
    """Map PyArrow data types to AWS Glue Data Catalog types."""
    if arrow_type in ARROW_TO_GLUE_TYPES:
        return ARROW_TO_GLUE_TYPES[arrow_type]
    if pa.types.is_timestamp(arrow_type):
        return 'timestamp'
    if pa.types.is_decimal(arrow_type):
        return f'decimal({arrow_type.precision},{arrow_type.scale})'
    return 'string'


def inspect_and_validate_parquet(parquet_bytes: bytes) -> tuple[bool, str, list[dict], int]:
    """Inspect Parquet footer metadata using PyArrow without decoding records into memory."""
    if not parquet_bytes:
        return False, "Validation Failed: Empty binary payload (0 bytes)", [], 0

    try:
        reader = pq.ParquetFile(io.BytesIO(parquet_bytes))
        schema = reader.schema_arrow
        row_count = reader.metadata.num_rows

        if schema is None or len(schema) == 0:
            return False, "Validation Failed: Parquet schema contains no columns", [], 0
        if row_count == 0:
            return False, "Validation Failed: Parquet dataset contains 0 rows", [], 0

        glue_columns = [{'Name': field.name, 'Type': pyarrow_to_glue_type(field.type)} for field in schema]
        return True, f"Validation Passed: {len(glue_columns)} columns and {row_count} rows discovered", glue_columns, row_count
    except Exception as e:
        return False, f"Validation Failed: Invalid Parquet file - {e}", [], 0


def set_glue_table_pointer(glue_client, database: str, dataset_name: str, location: str, columns: list[dict] | None = None, max_retries: int = 3) -> None:
    """
    Create or update the Glue Data Catalog table pointer (Location and Columns).
    Handles both promotion to GREEN and rollback to BLUE with concurrency retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            try:
                response = glue_client.get_table(DatabaseName=database, Name=dataset_name)
                table_exists = True
            except ClientError as ce:
                if ce.response.get('Error', {}).get('Code') == 'EntityNotFoundException':
                    table_exists = False
                else:
                    raise

            if table_exists:
                table_input = {k: v for k, v in response['Table'].items() if k not in READ_ONLY_GLUE_KEYS}
                table_input['StorageDescriptor']['Location'] = location
                if columns:
                    table_input['StorageDescriptor']['Columns'] = columns
                glue_client.update_table(DatabaseName=database, TableInput=table_input)
                logger.info("Updated Glue table '%s.%s' -> '%s'", database, dataset_name, location)
                return
            else:
                table_input = {
                    'Name': dataset_name,
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {'classification': 'parquet', 'has_encrypted_data': 'true', 'parquet.compression': 'SNAPPY'},
                    'StorageDescriptor': {
                        'Location': location,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'Name': 'ParquetHiveSerDe',
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                            'Parameters': {'serialization.format': '1'}
                        },
                        'Columns': columns or []
                    }
                }
                glue_client.create_table(DatabaseName=database, TableInput=table_input)
                logger.info("Created Glue table '%s.%s' -> '%s'", database, dataset_name, location)
                return
        except ClientError as e:
            code = e.response.get('Error', {}).get('Code', '')
            if code in ('ConcurrentModificationException', 'AlreadyExistsException') and attempt < max_retries:
                backoff_sec = (2 ** attempt) * 0.1
                logger.warning("Glue conflict (%s), retrying in %.2fs...", code, backoff_sec)
                time.sleep(backoff_sec)
            else:
                raise


def write_manifest(s3_client, bucket: str, dataset_name: str, active_slot: str, status: str, message: str, location: str, extra: dict | None = None) -> None:
    """Write deployment ledger record to S3 at <dataset>/manifest.json."""
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
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{dataset_name}/manifest.json",
        Body=json.dumps(payload, indent=2)
    )


def lambda_handler(event, context):
    """
    AWS Lambda handler for DataFlip:
    1. Direct JSON Rollback: {"action": "rollback", "dataset_name": "<name>"}
    2. S3 ObjectCreated Event: Validates Parquet and flips Glue table pointer to GREEN.
    """
    start_time = time.time()
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')

    # 1. Handle Direct Rollback Action
    if isinstance(event, dict) and event.get('action') == 'rollback':
        raw_dataset = event.get('dataset_name') or event.get('dataset')
        if not raw_dataset:
            return {'statusCode': 400, 'body': json.dumps({'status': 'ERROR', 'message': 'dataset_name is required for rollback'})}

        dataset_name = sanitize_dataset_name(raw_dataset)
        blue_location = f"s3://{S3_BUCKET}/{dataset_name}/blue/"
        try:
            set_glue_table_pointer(glue_client, GLUE_DATABASE, dataset_name, blue_location)
            write_manifest(s3_client, S3_BUCKET, dataset_name, 'blue', 'rolled_back', "Reverted to BLUE", blue_location)
            duration_ms = round((time.time() - start_time) * 1000, 2)
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'SUCCESS',
                    'message': f"Rolled back to BLUE for '{dataset_name}'",
                    'dataset_name': dataset_name,
                    'active_location': blue_location,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.exception("Rollback failed")
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}

    # 2. Handle S3 Event Notification
    if not (isinstance(event, dict) and event.get('Records') and 's3' in event['Records'][0]):
        return {'statusCode': 400, 'body': json.dumps({'status': 'ERROR', 'message': 'Unsupported event format'})}

    s3_meta = event['Records'][0]['s3']
    bucket_name = s3_meta.get('bucket', {}).get('name', S3_BUCKET)
    object_key = urllib.parse.unquote_plus(s3_meta.get('object', {}).get('key', ''))

    parsed = parse_s3_key(object_key)
    if not parsed:
        return {'statusCode': 200, 'body': json.dumps({'status': 'IGNORED', 'reason': 'Key does not match flat <dataset>/<slot>/<file> pattern', 'key': object_key})}

    dataset_name = parsed['dataset_name']
    slot = parsed['slot']

    if slot != 'green':
        return {'statusCode': 200, 'body': json.dumps({'status': 'IGNORED', 'reason': f"Upload to '{slot}' does not trigger promotion", 'key': object_key})}

    # 3. Retrieve and Validate Parquet Binary
    try:
        obj_resp = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        is_valid, validation_msg, glue_columns, row_count = inspect_and_validate_parquet(obj_resp['Body'].read())
    except Exception as e:
        is_valid, validation_msg, glue_columns, row_count = False, f"Failed to retrieve/parse Parquet: {e}", [], 0

    green_location = f"s3://{S3_BUCKET}/{dataset_name}/green/"
    blue_location = f"s3://{S3_BUCKET}/{dataset_name}/blue/"

    # 4. Promote or Reject
    if is_valid:
        try:
            set_glue_table_pointer(glue_client, GLUE_DATABASE, dataset_name, green_location, glue_columns)
            write_manifest(s3_client, S3_BUCKET, dataset_name, 'green', 'active', validation_msg, green_location, {
                'source': f"s3://{bucket_name}/{object_key}",
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
                    'row_count': row_count,
                    'execution_time_ms': duration_ms
                })
            }
        except Exception as e:
            logger.exception("Promotion failed")
            return {'statusCode': 500, 'body': json.dumps({'status': 'ERROR', 'message': str(e)})}
    else:
        write_manifest(s3_client, S3_BUCKET, dataset_name, 'blue', 'retained_on_failure', validation_msg, blue_location, {
            'rejection_reason': validation_msg,
            'source': f"s3://{bucket_name}/{object_key}"
        })
        duration_ms = round((time.time() - start_time) * 1000, 2)
        return {
            'statusCode': 422,
            'body': json.dumps({
                'status': 'REJECTED_GREEN',
                'dataset_name': dataset_name,
                'reason': validation_msg,
                'active_location': blue_location,
                'execution_time_ms': duration_ms
            })
        }
