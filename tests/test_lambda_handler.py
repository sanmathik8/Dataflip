import io
import json
import importlib
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

handler_module = importlib.import_module("lambda.handler")
parse_s3_key = handler_module.parse_s3_key
sanitize_dataset_name = handler_module.sanitize_dataset_name
pyarrow_to_glue_type = handler_module.pyarrow_to_glue_type
extract_glue_columns = handler_module.extract_glue_columns
inspect_and_validate_parquet = handler_module.inspect_and_validate_parquet
validate_records = handler_module.validate_records
promote_dataset = handler_module.promote_dataset
rollback_dataset = handler_module.rollback_dataset
lambda_handler = handler_module.lambda_handler


# =====================================================================
# 1. Unit Tests: S3 Key Parsing & Dataset Sanitization
# =====================================================================

def test_sanitize_dataset_name():
    assert sanitize_dataset_name("user-telemetry") == "user_telemetry"
    assert sanitize_dataset_name("My-Dataset_01") == "my_dataset_01"
    assert sanitize_dataset_name("---") == "default_dataset"

def test_parse_s3_key_valid_patterns():
    # Direct dataset path
    res1 = parse_s3_key("telemetry/green/metrics.parquet")
    assert res1 == {'dataset_name': 'telemetry', 'slot': 'green', 'filename': 'metrics.parquet'}

    # Hyphenated path gets sanitized for Glue
    res_hyphen = parse_s3_key("user-analytics/green/events.parquet")
    assert res_hyphen == {'dataset_name': 'user_analytics', 'slot': 'green', 'filename': 'events.parquet'}

    # Prefixed staging path
    res2 = parse_s3_key("curated/customer_profiles/green/users.parquet")
    assert res2 == {'dataset_name': 'customer_profiles', 'slot': 'green', 'filename': 'users.parquet'}

    # Blue slot path
    res3 = parse_s3_key("iot_sensors/blue/snapshot.parquet")
    assert res3 == {'dataset_name': 'iot_sensors', 'slot': 'blue', 'filename': 'snapshot.parquet'}


def test_parse_s3_key_invalid_patterns():
    assert parse_s3_key("random_file.parquet") is None
    assert parse_s3_key("curated/telemetry/yellow/file.parquet") is None
    assert parse_s3_key("telemetry/data.parquet") is None


# =====================================================================
# 2. Unit Tests: PyArrow to AWS Glue Type Mapping
# =====================================================================

def test_pyarrow_to_glue_type_mappings():
    assert pyarrow_to_glue_type(pa.bool_()) == "boolean"
    assert pyarrow_to_glue_type(pa.int8()) == "smallint"
    assert pyarrow_to_glue_type(pa.int16()) == "smallint"
    assert pyarrow_to_glue_type(pa.int32()) == "int"
    assert pyarrow_to_glue_type(pa.int64()) == "bigint"
    assert pyarrow_to_glue_type(pa.float32()) == "float"
    assert pyarrow_to_glue_type(pa.float64()) == "double"
    assert pyarrow_to_glue_type(pa.string()) == "string"
    assert pyarrow_to_glue_type(pa.large_string()) == "string"
    assert pyarrow_to_glue_type(pa.date32()) == "date"
    assert pyarrow_to_glue_type(pa.date64()) == "date"
    assert pyarrow_to_glue_type(pa.timestamp("ms")) == "timestamp"
    assert pyarrow_to_glue_type(pa.binary()) == "binary"
    assert pyarrow_to_glue_type(pa.decimal128(10, 2)) == "decimal(10,2)"


def test_extract_glue_columns():
    fields = [
        pa.field("sensor_id", pa.string()),
        pa.field("reading", pa.float64()),
        pa.field("timestamp", pa.int64()),
    ]
    schema = pa.schema(fields)
    glue_cols = extract_glue_columns(schema)
    expected = [
        {"Name": "sensor_id", "Type": "string"},
        {"Name": "reading", "Type": "double"},
        {"Name": "timestamp", "Type": "bigint"},
    ]
    assert glue_cols == expected


# =====================================================================
# 3. Unit Tests: Parquet Inspection & Structural Validation
# =====================================================================

def test_inspect_and_validate_parquet_success():
    # Generate in-memory Parquet
    table = pa.table({
        'device_id': pa.array(['dev-001', 'dev-002']),
        'temperature': pa.array([21.5, 22.8]),
        'is_online': pa.array([True, False])
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)

    is_valid, msg, cols, row_count = inspect_and_validate_parquet(buf.getvalue())
    assert is_valid is True
    assert row_count == 2
    assert len(cols) == 3
    assert cols[0] == {'Name': 'device_id', 'Type': 'string'}
    assert cols[1] == {'Name': 'temperature', 'Type': 'double'}
    assert cols[2] == {'Name': 'is_online', 'Type': 'boolean'}


def test_inspect_and_validate_parquet_empty_payload():
    is_valid, msg, cols, row_count = inspect_and_validate_parquet(b'')
    assert is_valid is False
    assert "Empty binary payload" in msg


def test_inspect_and_validate_parquet_zero_rows():
    # Empty schema with 0 rows
    table = pa.table({
        'col_a': pa.array([], type=pa.int64())
    })
    buf = io.BytesIO()
    pq.write_table(table, buf)

    is_valid, msg, cols, row_count = inspect_and_validate_parquet(buf.getvalue())
    assert is_valid is False
    assert "0 rows" in msg


def test_inspect_and_validate_parquet_corrupt_bytes():
    is_valid, msg, cols, row_count = inspect_and_validate_parquet(b'not_a_parquet_file')
    assert is_valid is False
    assert "Invalid Parquet file" in msg


def test_validate_records():
    assert validate_records([{'a': 1, 'b': 'x'}])[0] is True
    assert validate_records([])[0] is False


# =====================================================================
# 4. Unit Tests: Dynamic Glue Table Promotion & Rollback
# =====================================================================

def test_promote_dataset_creates_new_table():
    mock_glue = MagicMock()
    # Simulate table does not exist
    not_found_err = ClientError({'Error': {'Code': 'EntityNotFoundException'}}, 'GetTable')
    mock_glue.get_table.side_effect = not_found_err

    cols = [{'Name': 'id', 'Type': 'bigint'}, {'Name': 'val', 'Type': 'double'}]
    promote_dataset(mock_glue, 'dataflip_db', 'telemetry', 's3://bucket/telemetry/green/', cols)

    mock_glue.get_table.assert_called_once_with(DatabaseName='dataflip_db', Name='telemetry')
    mock_glue.create_table.assert_called_once()
    create_args = mock_glue.create_table.call_args[1]
    assert create_args['DatabaseName'] == 'dataflip_db'
    table_input = create_args['TableInput']
    assert table_input['Name'] == 'telemetry'
    assert table_input['TableType'] == 'EXTERNAL_TABLE'
    assert table_input['StorageDescriptor']['Location'] == 's3://bucket/telemetry/green/'
    assert table_input['StorageDescriptor']['Columns'] == cols


def test_promote_dataset_updates_existing_table():
    mock_glue = MagicMock()
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'telemetry',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {
                'Location': 's3://bucket/telemetry/blue/',
                'Columns': [{'Name': 'old_col', 'Type': 'string'}]
            }
        }
    }

    new_cols = [{'Name': 'new_col', 'Type': 'int'}]
    promote_dataset(mock_glue, 'dataflip_db', 'telemetry', 's3://bucket/telemetry/green/', new_cols)

    mock_glue.get_table.assert_called_once()
    mock_glue.update_table.assert_called_once()
    update_args = mock_glue.update_table.call_args[1]
    table_input = update_args['TableInput']
    assert table_input['StorageDescriptor']['Location'] == 's3://bucket/telemetry/green/'
    assert table_input['StorageDescriptor']['Columns'] == new_cols


def test_promote_dataset_concurrency_retry_success():
    mock_glue = MagicMock()
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'telemetry',
            'StorageDescriptor': {'Location': 's3://bucket/telemetry/blue/', 'Columns': []}
        }
    }
    concurrency_err = ClientError({'Error': {'Code': 'ConcurrentModificationException'}}, 'UpdateTable')
    mock_glue.update_table.side_effect = [concurrency_err, None]

    cols = [{'Name': 'id', 'Type': 'bigint'}]
    promote_dataset(mock_glue, 'dataflip_db', 'telemetry', 's3://bucket/telemetry/green/', cols)

    assert mock_glue.update_table.call_count == 2


def test_promote_dataset_concurrency_retry_exhaustion():
    mock_glue = MagicMock()
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'telemetry',
            'StorageDescriptor': {'Location': 's3://bucket/telemetry/blue/', 'Columns': []}
        }
    }
    concurrency_err = ClientError({'Error': {'Code': 'ConcurrentModificationException'}}, 'UpdateTable')
    mock_glue.update_table.side_effect = concurrency_err

    with pytest.raises(ClientError):
        promote_dataset(mock_glue, 'dataflip_db', 'telemetry', 's3://bucket/telemetry/green/', [], max_retries=2)

    assert mock_glue.update_table.call_count == 2


def test_rollback_dataset():
    mock_glue = MagicMock()
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'telemetry',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/telemetry/green/'}
        }
    }

    rollback_dataset(mock_glue, 'dataflip_db', 'telemetry', 's3://bucket/telemetry/blue/')

    mock_glue.get_table.assert_called_once()
    mock_glue.update_table.assert_called_once()
    table_input = mock_glue.update_table.call_args[1]['TableInput']
    assert table_input['StorageDescriptor']['Location'] == 's3://bucket/telemetry/blue/'


# =====================================================================
# 5. Integration Tests: Lambda Handler End-to-End
# =====================================================================

@patch('boto3.client')
def test_lambda_handler_s3_trigger_arbitrary_datasets(mock_boto_client):
    """Verify that multiple completely different datasets are processed by the same Lambda."""
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_boto_client.side_effect = lambda svc: mock_s3 if svc == 's3' else mock_glue

    # 1. First dataset: IoT Telemetry
    table_telemetry = pa.table({
        'device_id': pa.array(['sensor-A']),
        'temperature': pa.array([45.2]),
        'is_alert': pa.array([False])
    })
    buf1 = io.BytesIO()
    pq.write_table(table_telemetry, buf1)

    mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=buf1.getvalue()))}
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'iot_telemetry',
            'StorageDescriptor': {'Location': 's3://dataflip-analytics-dev/iot_telemetry/blue/', 'Columns': []}
        }
    }

    event1 = {
        "Records": [{
            "s3": {
                "bucket": {"name": "dataflip-analytics-dev"},
                "object": {"key": "iot_telemetry/green/metrics.parquet"}
            }
        }]
    }

    res1 = lambda_handler(event1, None)
    assert res1['statusCode'] == 200
    body1 = json.loads(res1['body'])
    assert body1['status'] == 'ACTIVATED_GREEN'
    assert body1['dataset_name'] == 'iot_telemetry'
    assert body1['active_location'] == 's3://dataflip-analytics-dev/iot_telemetry/green/'
    assert body1['row_count'] == 1
    assert len(body1['columns']) == 3

    # Verify manifest written for dataset 1
    mock_s3.put_object.assert_called()
    manifest_call1 = mock_s3.put_object.call_args[1]
    assert manifest_call1['Key'] == 'iot_telemetry/manifest.json'

    # 2. Second dataset: Customer Profiles (completely different schema)
    table_customers = pa.table({
        'customer_id': pa.array(['cust-99']),
        'signup_date': pa.array(['2026-09-01']),
        'balance': pa.array([1250.75])
    })
    buf2 = io.BytesIO()
    pq.write_table(table_customers, buf2)

    mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=buf2.getvalue()))}
    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'customer_profiles',
            'StorageDescriptor': {'Location': 's3://dataflip-analytics-dev/customer_profiles/blue/', 'Columns': []}
        }
    }

    event2 = {
        "Records": [{
            "s3": {
                "bucket": {"name": "dataflip-analytics-dev"},
                "object": {"key": "customer_profiles/green/data.parquet"}
            }
        }]
    }

    res2 = lambda_handler(event2, None)
    assert res2['statusCode'] == 200
    body2 = json.loads(res2['body'])
    assert body2['status'] == 'ACTIVATED_GREEN'
    assert body2['dataset_name'] == 'customer_profiles'
    manifest_call2 = mock_s3.put_object.call_args[1]
    assert manifest_call2['Key'] == 'customer_profiles/manifest.json'


@patch('boto3.client')
def test_lambda_handler_s3_trigger_non_green_ignored(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_boto_client.side_effect = lambda svc: mock_s3 if svc == 's3' else mock_glue

    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "dataflip-analytics-dev"},
                "object": {"key": "telemetry/blue/data.parquet"}
            }
        }]
    }

    res = lambda_handler(event, None)
    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['status'] == 'IGNORED'
    mock_glue.get_table.assert_not_called()
    mock_glue.update_table.assert_not_called()


@patch('boto3.client')
def test_lambda_handler_s3_trigger_empty_rejection(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_boto_client.side_effect = lambda svc: mock_s3 if svc == 's3' else mock_glue

    # Empty Parquet with 0 rows
    table = pa.table({'col_a': pa.array([], type=pa.int32())})
    buf = io.BytesIO()
    pq.write_table(table, buf)

    mock_s3.get_object.return_value = {'Body': MagicMock(read=MagicMock(return_value=buf.getvalue()))}

    event = {
        "Records": [{
            "s3": {
                "bucket": {"name": "dataflip-analytics-dev"},
                "object": {"key": "telemetry/green/empty.parquet"}
            }
        }]
    }

    res = lambda_handler(event, None)
    assert res['statusCode'] == 422
    body = json.loads(res['body'])
    assert body['status'] == 'REJECTED_GREEN'
    assert body['dataset_name'] == 'telemetry'
    mock_glue.update_table.assert_not_called()
    mock_s3.put_object.assert_called_once()
    assert mock_s3.put_object.call_args[1]['Key'] == 'telemetry/manifest.json'


@patch('boto3.client')
def test_lambda_handler_rollback_action(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()
    mock_boto_client.side_effect = lambda svc: mock_s3 if svc == 's3' else mock_glue

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'telemetry',
            'StorageDescriptor': {'Location': 's3://dataflip-analytics-dev/telemetry/green/'}
        }
    }

    event = {'action': 'rollback', 'dataset_name': 'telemetry'}
    res = lambda_handler(event, None)

    assert res['statusCode'] == 200
    body = json.loads(res['body'])
    assert body['status'] == 'SUCCESS'
    assert 'Rollback completed to BLUE' in body['message']
    assert body['active_location'] == 's3://dataflip-analytics-dev/telemetry/blue/'
    mock_glue.update_table.assert_called_once()
    mock_s3.put_object.assert_called_once()
    assert mock_s3.put_object.call_args[1]['Key'] == 'telemetry/manifest.json'


@patch('boto3.client')
def test_lambda_handler_rollback_missing_dataset_name(mock_boto_client):
    event = {'action': 'rollback'}
    res = lambda_handler(event, None)
    assert res['statusCode'] == 400
    body = json.loads(res['body'])
    assert 'dataset_name is required' in body['message']
