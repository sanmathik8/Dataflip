import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET', 'dataflip-analytics-bucket')
GLUE_DATABASE = os.environ.get('GLUE_DATABASE', 'dataflip_db')
GLUE_TABLE = os.environ.get('GLUE_TABLE', 'sales_curated')

REQUIRED_COLUMNS = [
    'order_id', 'order_date', 'product', 'category',
    'quantity', 'unit_price', 'region', 'revenue'
]

def validate_records(records: list[dict]) -> tuple[bool, str]:
    """Validate data records in memory."""
    if not records or len(records) == 0:
        return False, "Validation Failed: Empty record set (0 rows)"

    # Check required columns on first record
    sample = records[0]
    missing = [col for col in REQUIRED_COLUMNS if col not in sample]
    if missing:
        return False, f"Validation Failed: Missing required columns {missing}"

    for idx, row in enumerate(records):
        if row.get('order_id') is None:
            return False, f"Validation Failed: Null order_id at row index {idx}"
        
        try:
            qty = float(row.get('quantity', 0))
            if qty <= 0:
                return False, f"Validation Failed: Invalid quantity ({qty}) at row index {idx}"
        except (ValueError, TypeError):
            return False, f"Validation Failed: Non-numeric quantity at row index {idx}"

        try:
            price = float(row.get('unit_price', -1))
            if price < 0:
                return False, f"Validation Failed: Invalid unit_price ({price}) at row index {idx}"
        except (ValueError, TypeError):
            return False, f"Validation Failed: Non-numeric unit_price at row index {idx}"

        try:
            rev = float(row.get('revenue', -1))
            if rev < 0:
                return False, f"Validation Failed: Invalid revenue ({rev}) at row index {idx}"
        except (ValueError, TypeError):
            return False, f"Validation Failed: Non-numeric revenue at row index {idx}"

    return True, f"Validation Passed: {len(records)} records validated successfully"

def update_glue_table_location(glue_client, database: str, table_name: str, new_s3_location: str):
    """Update Glue Catalog Table S3 Location to switch active dataset."""
    response = glue_client.get_table(DatabaseName=database, Name=table_name)
    table_input = response['Table']
    
    # Strip read-only fields returned by get_table
    read_only_keys = ['DatabaseName', 'CreateTime', 'UpdateTime', 'CreatedBy',
                      'IsRegisteredWithLakeFormation', 'CatalogId', 'VersionId', 'Owner']
    for key in read_only_keys:
        table_input.pop(key, None)
        
    table_input['StorageDescriptor']['Location'] = new_s3_location
    
    glue_client.update_table(
        DatabaseName=database,
        TableInput=table_input
    )
    logger.info(f"Glue Catalog Table '{database}.{table_name}' location updated to: {new_s3_location}")

def lambda_handler(event, context):
    """AWS Lambda entrypoint for DataFlip Blue/Green release & rollback."""
    logger.info(f"DataFlip Lambda Invoked. Event: {json.dumps(event)}")
    
    s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    
    # 1. Handle Manual Rollback Action
    if isinstance(event, dict) and event.get('action') == 'rollback':
        logger.info("Executing Rollback Action...")
        blue_location = f"s3://{S3_BUCKET}/curated/blue/"
        try:
            update_glue_table_location(glue_client, GLUE_DATABASE, GLUE_TABLE, blue_location)
            manifest = {
                'active_dataset': 'blue',
                'status': 'rolled_back',
                'message': 'Production dataset reverted to BLUE'
            }
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key='curated/active_manifest.json',
                Body=json.dumps(manifest, indent=2)
            )
            return {
                'statusCode': 200,
                'body': json.dumps({'status': 'SUCCESS', 'message': 'Rollback completed to BLUE'})
            }
        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'status': 'ERROR', 'message': str(e)})
            }

    # 2. Extract Candidate Data Payload or S3 Event
    records = event.get('records', [])
    
    # Validate Candidate Data
    is_valid, validation_msg = validate_records(records)
    
    if is_valid:
        logger.info(f"GREEN Dataset Validation Passed: {validation_msg}")
        green_location = f"s3://{S3_BUCKET}/curated/green/"
        
        # Activate GREEN in Glue Data Catalog
        try:
            update_glue_table_location(glue_client, GLUE_DATABASE, GLUE_TABLE, green_location)
            
            manifest = {
                'active_dataset': 'green',
                'status': 'active',
                'message': validation_msg,
                's3_location': green_location
            }
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key='curated/active_manifest.json',
                Body=json.dumps(manifest, indent=2)
            )
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'ACTIVATED_GREEN',
                    'message': validation_msg,
                    'active_location': green_location
                })
            }
        except Exception as e:
            logger.error(f"Failed to update Glue Catalog: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps({'status': 'ERROR', 'message': str(e)})
            }
    else:
        logger.warning(f"GREEN Dataset Validation Failed: {validation_msg}. Retaining BLUE dataset.")
        blue_location = f"s3://{S3_BUCKET}/curated/blue/"
        
        manifest = {
            'active_dataset': 'blue',
            'status': 'retained_on_failure',
            'rejection_reason': validation_msg,
            's3_location': blue_location
        }
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key='curated/active_manifest.json',
                Body=json.dumps(manifest, indent=2)
            )
        except Exception as e:
            logger.error(f"Failed to write manifest: {e}")

        return {
            'statusCode': 422,
            'body': json.dumps({
                'status': 'REJECTED_GREEN',
                'reason': validation_msg,
                'active_location': blue_location
            })
        }
