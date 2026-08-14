import json
import pytest
import importlib
from unittest.mock import MagicMock, patch

lambda_module = importlib.import_module("lambda.handler")
lambda_handler = lambda_module.lambda_handler
validate_records = lambda_module.validate_records

def test_validate_records_success():
    valid = [
        {
            'order_id': 1, 'order_date': '2026-01-01', 'product': 'P1',
            'category': 'Cat1', 'quantity': 2, 'unit_price': 10.0,
            'region': 'North', 'revenue': 20.0
        }
    ]
    is_valid, msg = validate_records(valid)
    assert is_valid is True
    assert "validated successfully" in msg

def test_validate_records_invalid_quantity():
    invalid = [
        {
            'order_id': 1, 'order_date': '2026-01-01', 'product': 'P1',
            'category': 'Cat1', 'quantity': 0, 'unit_price': 10.0,
            'region': 'North', 'revenue': 0.0
        }
    ]
    is_valid, msg = validate_records(invalid)
    assert is_valid is False
    assert "Invalid quantity" in msg

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
            {
                'order_id': 100, 'order_date': '2026-01-10', 'product': 'ProdA',
                'category': 'CatA', 'quantity': 5, 'unit_price': 20.0,
                'region': 'West', 'revenue': 100.0
            }
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
        'records': [
            {
                'order_id': 101, 'order_date': '2026-01-10', 'product': 'ProdB',
                'category': 'CatB', 'quantity': -1, 'unit_price': 20.0, # Invalid
                'region': 'West', 'revenue': -20.0
            }
        ]
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
