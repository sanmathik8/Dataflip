import io
import json
import os
import re
import urllib.parse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


# AWS clients
s3 = boto3.client("s3")
glue = boto3.client("glue")


# Configuration from Terraform environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "dataflip-analytics-dev")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "dataflip_db")


def sanitize_name(name):
    """Make dataset name safe for Glue table."""
    return re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        str(name)
    ).strip("_").lower()


def parse_s3_key(key):
    """
    Expected S3 structure:

    dataset/green/file.parquet
    dataset/blue/file.parquet
    """

    parts = key.split("/")

    if len(parts) == 3 and parts[1] in ("green", "blue"):
        return (
            sanitize_name(parts[0]),
            parts[1],
            parts[2]
        )

    return None, None, None


def arrow_to_glue_type(dtype):
    """Convert PyArrow datatype to Glue/Athena datatype."""

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
    """
    Check whether the Parquet file is readable
    and contains rows and columns.
    """

    try:

        reader = pq.ParquetFile(
            io.BytesIO(data)
        )

        # Check rows
        if reader.metadata.num_rows == 0:
            return False, [], 0

        # Check columns
        if len(reader.schema_arrow) == 0:
            return False, [], 0

        # Build Glue schema
        columns = [
            {
                "Name": field.name,
                "Type": arrow_to_glue_type(field.type)
            }
            for field in reader.schema_arrow
        ]

        return (
            True,
            columns,
            reader.metadata.num_rows
        )

    except Exception:
        return False, [], 0


def promote_green_to_blue(dataset, filename):
    """
    Copy the validated GREEN file to BLUE.

    BLUE always represents the current production dataset.
    """

    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={
            "Bucket": S3_BUCKET,
            "Key": f"{dataset}/green/{filename}"
        },
        Key=f"{dataset}/blue/{filename}"
    )


def set_glue_table_pointer(dataset, location, columns):
    """
    Create or update Glue table
    to point to the given S3 location.
    """

    try:

        # Check whether the Glue table already exists
        table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=dataset
        )["Table"]

        # Change S3 location
        table["StorageDescriptor"]["Location"] = location

        # Update schema
        table["StorageDescriptor"]["Columns"] = columns

        # Remove fields that Glue does not allow us
        # to send back during update_table()
        for key in [
            "DatabaseName",
            "CreateTime",
            "UpdateTime",
            "CreatedBy",
            "CatalogId",
            "VersionId"
        ]:
            table.pop(key, None)

        # Update existing table
        glue.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table
        )

    except glue.exceptions.EntityNotFoundException:

        # Create the Glue table if it doesn't exist
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

                    "InputFormat":
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",

                    "OutputFormat":
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",

                    "SerdeInfo": {
                        "SerializationLibrary":
                            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    },

                    "Columns": columns
                }
            }
        )


def write_manifest(
    dataset,
    active_slot,
    status,
    location,
    rows=0,
    columns=0
):
    """
    Store deployment status in S3.
    """

    manifest = {
        "dataset_name": dataset,
        "active_slot": active_slot,
        "status": status,
        "s3_location": location,
        "rows": rows,
        "columns": columns
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{dataset}/manifest.json",
        Body=json.dumps(
            manifest,
            indent=2
        )
    )


def lambda_handler(event, context):

    # =========================================================
    # 1. ROLLBACK
    # =========================================================

    if event.get("action") == "rollback":

        dataset = sanitize_name(
            event.get("dataset_name", "")
        )

        if not dataset:
            return {
                "status": "error",
                "message": "dataset_name is required"
            }

        blue_location = (
            f"s3://{S3_BUCKET}/{dataset}/blue/"
        )

        # Point Glue back to BLUE
        set_glue_table_pointer(
            dataset,
            blue_location,
            []
        )

        # Record rollback
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


    # =========================================================
    # 2. READ S3 EVENT
    # =========================================================

    if "Records" not in event:
        return {
            "status": "ignored"
        }

    record = event["Records"][0]["s3"]

    bucket = record["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        record["object"]["key"]
    )


    # =========================================================
    # 3. EXTRACT DATASET AND SLOT
    # =========================================================

    dataset, slot, filename = parse_s3_key(key)

    # We only process GREEN uploads
    if not dataset or slot != "green":
        return {
            "status": "ignored"
        }


    # =========================================================
    # 4. READ AND VALIDATE PARQUET
    # =========================================================

    try:

        data = s3.get_object(
            Bucket=bucket,
            Key=key
        )["Body"].read()

        is_valid, columns, rows = validate_parquet(
            data
        )

    except Exception:

        is_valid = False
        columns = []
        rows = 0


    # =========================================================
    # 5. CREATE BLUE/GREEN LOCATIONS
    # =========================================================

    green_location = (
        f"s3://{S3_BUCKET}/{dataset}/green/"
    )

    blue_location = (
        f"s3://{S3_BUCKET}/{dataset}/blue/"
    )


    # =========================================================
    # 6. ACTIVATE GREEN
    # =========================================================

    if is_valid:

        # Promote validated GREEN file to BLUE
        promote_green_to_blue(
            dataset,
            filename
        )

        # Glue always points to current production BLUE
        set_glue_table_pointer(
            dataset,
            blue_location,
            columns
        )

        # Record successful deployment
        write_manifest(
            dataset,
            "blue",
            "activated",
            blue_location,
            rows,
            len(columns)
        )

        return {
            "status": "activated",
            "dataset": dataset,
            "rows": rows
        }


    # =========================================================
    # 7. INVALID DATA → KEEP BLUE
    # =========================================================

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
