import io
import json
import os
import re
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


s3 = boto3.client("s3")
glue = boto3.client("glue")

S3_BUCKET = os.environ.get("S3_BUCKET", "dataflip-analytics-dev")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "dataflip_db")


def sanitize_name(name):
    """Make dataset name safe for Glue table."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(name)).strip("_").lower()


def parse_s3_key(key):
    """Extract dataset, slot and filename from dataset/green/file.parquet."""
    parts = key.split("/")

    if len(parts) == 3 and parts[1] in ("green", "blue"):
        return sanitize_name(parts[0]), parts[1], parts[2]

    return None, None, None


def arrow_to_glue_type(dtype):
    """Convert PyArrow types to Glue/Athena types."""
    if pa.types.is_integer(dtype):
        return "bigint"
    if pa.types.is_floating(dtype):
        return "double"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_timestamp(dtype):
        return "timestamp"

    return "string"


def validate_parquet(data):
    """Check whether Parquet contains rows and columns."""
    try:
        reader = pq.ParquetFile(io.BytesIO(data))

        if reader.metadata.num_rows == 0:
            return False, [], 0

        if len(reader.schema_arrow) == 0:
            return False, [], 0

        columns = [
            {
                "Name": field.name,
                "Type": arrow_to_glue_type(field.type)
            }
            for field in reader.schema_arrow
        ]

        return True, columns, reader.metadata.num_rows

    except Exception:
        return False, [], 0


def set_glue_table_pointer(dataset, location, columns):
    """Create or update Glue table to point to an S3 location."""

    try:
        table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=dataset
        )["Table"]

        table["StorageDescriptor"]["Location"] = location
        table["StorageDescriptor"]["Columns"] = columns

        # Remove fields Glue does not allow us to send back.
        for key in [
            "DatabaseName",
            "CreateTime",
            "UpdateTime",
            "CreatedBy",
            "CatalogId",
            "VersionId"
        ]:
            table.pop(key, None)

        glue.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table
        )

    except glue.exceptions.EntityNotFoundException:

        glue.create_table(
            DatabaseName=GLUE_DATABASE,
            TableInput={
                "Name": dataset,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet"
                },
                "StorageDescriptor": {
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },
                    "Columns": columns
                }
            }
        )


def write_manifest(dataset, active_slot, status, location):
    """Record the current deployment status in S3."""

    manifest = {
        "dataset_name": dataset,
        "active_slot": active_slot,
        "status": status,
        "s3_location": location
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{dataset}/manifest.json",
        Body=json.dumps(manifest, indent=2)
    )


def lambda_handler(event, context):

    # 1. Rollback to BLUE
    if event.get("action") == "rollback":

        dataset = sanitize_name(
            event.get("dataset_name", "")
        )

        blue_location = f"s3://{S3_BUCKET}/{dataset}/blue/"

        set_glue_table_pointer(
            dataset,
            blue_location,
            []
        )

        write_manifest(
            dataset,
            "blue",
            "rolled_back",
            blue_location
        )

        return {
            "status": "rolled_back",
            "dataset": dataset
        }

    # 2. Read S3 event
    if "Records" not in event:
        return {"status": "ignored"}

    record = event["Records"][0]["s3"]

    bucket = record["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        record["object"]["key"]
    )

    # 3. Extract dataset and slot
    dataset, slot, filename = parse_s3_key(key)

    if not dataset or slot != "green":
        return {"status": "ignored"}

    # 4. Read and validate Parquet
    try:
        data = s3.get_object(
            Bucket=bucket,
            Key=key
        )["Body"].read()

        is_valid, columns, rows = validate_parquet(data)

    except Exception:
        is_valid = False
        columns = []
        rows = 0

    green_location = f"s3://{S3_BUCKET}/{dataset}/green/"
    blue_location = f"s3://{S3_BUCKET}/{dataset}/blue/"

    # 5. Activate GREEN or keep BLUE
    if is_valid:

        set_glue_table_pointer(
            dataset,
            green_location,
            columns
        )

        write_manifest(
            dataset,
            "green",
            "activated",
            green_location
        )

        return {
            "status": "activated",
            "dataset": dataset,
            "rows": rows
        }

    write_manifest(
        dataset,
        "blue",
        "rejected",
        blue_location
    )

    return {
        "status": "rejected",
        "dataset": dataset
    }
