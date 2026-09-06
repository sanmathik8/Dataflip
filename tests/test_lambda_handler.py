import json
import io
import importlib
import pandas as pd
from unittest.mock import MagicMock, patch

lambda_module = importlib.import_module("lambda.handler")
lambda_handler = lambda_module.lambda_handler
validate_records = lambda_module.validate_records

def test_validate_records_success():
    valid = [
        {'col_a': 1, 'col_b': 'test'}
    ]
    is_valid, msg = validate_records(valid)
    assert is_valid is True
    assert "validated successfully" in msg

def test_validate_records_empty_records():
    invalid = []
    is_valid, msg = validate_records(invalid)
    assert is_valid is False
    assert "Empty record set" in msg

@patch('boto3.client')
def test_lambda_handler_activate_green(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    def client_factory(service_name):
        if service_name == 's3':
            return mock_s3
        elif service_name == 'glue':
            return mock_glue
        return MagicMock()

    mock_boto_client.side_effect = client_factory

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'sales_curated',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/curated/blue/'}
        }
    }

    event = {
        'records': [
            {'temp': 25.5, 'humidity': 60}
        ]
    }

    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['status'] == 'ACTIVATED_GREEN'
    mock_glue.update_table.assert_called_once()
    mock_s3.put_object.assert_called_once()

@patch('boto3.client')
def test_lambda_handler_reject_green(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    event = {
        'records': [] # Empty -> Invalid
    }

    response = lambda_handler(event, None)
    assert response['statusCode'] == 422
    body = json.loads(response['body'])
    assert body['status'] == 'REJECTED_GREEN'
    mock_glue.update_table.assert_not_called()
    mock_s3.put_object.assert_called_once()

@patch('boto3.client')
def test_lambda_handler_rollback(mock_boto_client):
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'sales_curated',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/curated/green/'}
        }
    }

    event = {'action': 'rollback'}

    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['status'] == 'SUCCESS'
    assert 'Rollback completed to BLUE' in body['message']
    mock_glue.update_table.assert_called_once()

@patch('boto3.client')
def test_lambda_handler_glue_concurrency_retry(mock_boto_client):
    from botocore.exceptions import ClientError
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'sales_curated',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/curated/blue/'}
        }
    }

    client_error = ClientError({'Error': {'Code': 'ConcurrentModificationException'}}, 'UpdateTable')
    mock_glue.update_table.side_effect = [client_error, None]

    event = {
        'records': [
            {'x': 1, 'y': 2}
        ]
    }

    response = lambda_handler(event, None)
    assert response['statusCode'] == 200
    assert mock_glue.update_table.call_count == 2

@patch('boto3.client')
def test_lambda_handler_glue_retry_exhaustion_returns_500(mock_boto_client):
    from botocore.exceptions import ClientError
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'sales_curated',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/curated/blue/'}
        }
    }

    client_error = ClientError({'Error': {'Code': 'ConcurrentModificationException'}}, 'UpdateTable')
    mock_glue.update_table.side_effect = client_error

    event = {
        'records': [
            {'x': 1, 'y': 2}
        ]
    }

    response = lambda_handler(event, None)
    assert response['statusCode'] == 500
    body = json.loads(response['body'])
    assert body['status'] == 'ERROR'
    assert mock_glue.update_table.call_count == 3

@patch('boto3.client')
def test_lambda_handler_s3_event_trigger_success(mock_boto_client):
    """Test S3 ObjectCreated event notification triggers Parquet download and GREEN activation."""
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    mock_glue.get_table.return_value = {
        'Table': {
            'Name': 'sales_curated',
            'DatabaseName': 'dataflip_db',
            'StorageDescriptor': {'Location': 's3://bucket/curated/blue/'}
        }
    }

    # Synthesize valid Parquet binary payload
    df = pd.DataFrame({'order_id': [1, 2], 'amount': [100.0, 200.0]})
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    mock_body = MagicMock()
    mock_body.read.return_value = buf.getvalue()
    mock_s3.get_object.return_value = {'Body': mock_body}

    s3_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": "dataflip-analytics-dev"},
                    "object": {"key": "curated/green/sales.parquet"}
                }
            }
        ]
    }

    response = lambda_handler(s3_event, None)
    assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['status'] == 'ACTIVATED_GREEN'
    assert "curated/green/sales.parquet" in body['source']
    mock_s3.get_object.assert_called_once_with(
        Bucket="dataflip-analytics-dev",
        Key="curated/green/sales.parquet"
    )
    mock_glue.update_table.assert_called_once()
    mock_s3.put_object.assert_called_once()

@patch('boto3.client')
def test_lambda_handler_s3_event_trigger_empty_rejection(mock_boto_client):
    """Test S3 event with empty Parquet file rejects release and retains BLUE."""
    mock_s3 = MagicMock()
    mock_glue = MagicMock()

    mock_boto_client.side_effect = lambda service: mock_s3 if service == 's3' else mock_glue

    # Synthesize empty Parquet binary payload
    df_empty = pd.DataFrame()
    buf = io.BytesIO()
    df_empty.to_parquet(buf, index=False)
    mock_body = MagicMock()
    mock_body.read.return_value = buf.getvalue()
    mock_s3.get_object.return_value = {'Body': mock_body}

    s3_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {"name": "dataflip-analytics-dev"},
                    "object": {"key": "curated/green/empty.parquet"}
                }
            }
        ]
    }

    response = lambda_handler(s3_event, None)
    assert response['statusCode'] == 422
    body = json.loads(response['body'])
    assert body['status'] == 'REJECTED_GREEN'
    mock_glue.update_table.assert_not_called()
    mock_s3.put_object.assert_called_once()
