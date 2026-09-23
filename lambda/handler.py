import io
import json
import os
import re
import time
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
glue = boto3.client('glue')

S3_BUCKET = os.environ.get('S3_BUCKET', 'dataflip-analytics-dev')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'dataflip_db')

READ_ONLY_GLUE_KEYS = {
    'DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
    'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'Owner',
    'IsMultiDialectView', 'IsMaterializedView'
}


def sanitize_name(name):
    """Sanitize dataset name to valid Glue table name (alphanumeric and underscores)."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name)).strip('_').lower()


def parse_s3_key(key):
    """Extract dataset, slot, and filename from flat S3 key: <dataset>/<slot>/<file>."""
    parts = key.split('/')
    if len(parts) == 3 and parts[1] in ('green', 'blue') and parts[2]:
        clean_name = sanitize_name(parts[0])
        if clean_name:
            return clean_name, parts[1], parts[2]
    return None, None, None


def arrow_to_glue_type(dtype):
    """Map PyArrow data type to AWS Glue / Athena SQL data type."""
    if pa.types.is_int64(dtype):
        return 'bigint'
    if pa.types.is_integer(dtype):
        return 'int'
    if pa.types.is_floating(dtype):
        return 'double'
    if pa.types.is_boolean(dtype):
        return 'boolean'
    if pa.types.is_date(dtype):
        return 'date'
    if pa.types.is_timestamp(dtype):
        return 'timestamp'
    if pa.types.is_decimal(dtype):
        return f'decimal({dtype.precision},{dtype.scale})'
    return 'string'


def validate_parquet(data):
    """Inspect Parquet footer metadata without loading records into memory."""
    try:
        reader = pq.ParquetFile(io.BytesIO(data))
        if reader.metadata.num_rows > 0 and len(reader.schema_arrow) > 0:
            columns = [{'Name': f.name, 'Type': arrow_to_glue_type(f.type)} for f in reader.schema_arrow]
            return True, columns, reader.metadata.num_rows
    except Exception:
        pass
    return False, [], 0


def set_glue_table_pointer(dataset, location, columns=None):
    """Create or update Glue table pointer to S3 location with concurrency retry."""
    for attempt in range(3):
        try:
            table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=dataset)['Table']
            table_input = {k: v for k, v in table.items() if k not in READ_ONLY_GLUE_KEYS}
            table_input['StorageDescriptor']['Location'] = location
            if columns:
                table_input['StorageDescriptor']['Columns'] = columns
            glue.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
            return
        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'EntityNotFoundException':
                glue.create_table(
                    DatabaseName=GLUE_DATABASE,
                    TableInput={
                        'Name': dataset,
                        'TableType': 'EXTERNAL_TABLE',
                        'Parameters': {'classification': 'parquet'},
                        'StorageDescriptor': {
                            'Location': location,
                            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                            'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                                'Parameters': {'serialization.format': '1'}
                            },
                            'Columns': columns or []
                        }
                    }
                )
                return
            if code in ('ConcurrentModificationException', 'AlreadyExistsException') and attempt < 2:
                time.sleep(0.2)
            else:
                raise


def write_manifest(dataset, active_slot, status, location, extra=None):
    """Write deployment ledger record to S3 at <dataset>/manifest.json."""
    payload = {
        'dataset_name': dataset,
        'active_slot': active_slot,
        'status': status,
        's3_location': location,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    }
    if extra:
        payload.update(extra)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{dataset}/manifest.json",
        Body=json.dumps(payload, indent=2)
    )


def lambda_handler(event, context):
    """
    AWS Lambda entry point:
    1. Direct JSON Rollback: {"action": "rollback", "dataset_name": "orders"}
    2. S3 ObjectCreated Event: Validates Parquet and flips Glue pointer to GREEN.
    """
    # 1. Direct Rollback
    if isinstance(event, dict) and event.get('action') == 'rollback':
        raw_dataset = event.get('dataset_name') or event.get('dataset', '')
        dataset = sanitize_name(raw_dataset)
        if not dataset:
            return {'status': 'error', 'message': 'dataset_name is required for rollback'}
        blue_location = f"s3://{S3_BUCKET}/{dataset}/blue/"
        set_glue_table_pointer(dataset, blue_location)
        write_manifest(dataset, 'blue', 'rolled_back', blue_location)
        return {'status': 'rolled_back', 'dataset': dataset}

    # 2. S3 Event Notification
    if not (isinstance(event, dict) and event.get('Records')):
        return {'status': 'ignored'}

    record = event['Records'][0].get('s3', {})
    bucket = record.get('bucket', {}).get('name', S3_BUCKET)
    key = urllib.parse.unquote_plus(record.get('object', {}).get('key', ''))

    dataset, slot, filename = parse_s3_key(key)
    if not dataset or slot != 'green':
        return {'status': 'ignored'}

    # 3. Read and Validate Parquet
    try:
        data = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        is_valid, columns, rows = validate_parquet(data)
    except Exception:
        is_valid, columns, rows = False, [], 0

    green_location = f"s3://{S3_BUCKET}/{dataset}/green/"
    blue_location = f"s3://{S3_BUCKET}/{dataset}/blue/"

    # 4. Promote to GREEN or Retain BLUE
    if is_valid:
        set_glue_table_pointer(dataset, green_location, columns)
        write_manifest(dataset, 'green', 'activated', green_location, {'rows': rows, 'columns': len(columns)})
        return {'status': 'activated', 'dataset': dataset, 'rows': rows}
    else:
        write_manifest(dataset, 'blue', 'rejected', blue_location, {'reason': 'Parquet validation failed'})
        return {'status': 'rejected', 'dataset': dataset}
