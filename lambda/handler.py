import io
import json
import os
import re
import urllib.parse
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


s3 = boto3.client("s3")
glue = boto3.client("glue")

S3_BUCKET = os.environ.get("S3_BUCKET", "dataflip-analytics-dev")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "dataflip_db")

SLOTS = ("blue", "green")


def sanitize_name(name):
    return re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        str(name)
    ).strip("_").lower()


def other_slot(slot):
    if slot == "blue":
        return "green"
    return "blue"


def slot_prefix(dataset, slot):
    return f"{dataset}/{slot}/"


def slot_location(dataset, slot):
    return f"s3://{S3_BUCKET}/{dataset}/{slot}/"


def manifest_key(dataset):
    return f"{dataset}/manifest.json"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def parse_incoming_key(key):
    parts = key.split("/")

    if (
        len(parts) == 3
        and parts[0] == "incoming"
        and parts[2].lower().endswith(".parquet")
    ):
        dataset = sanitize_name(parts[1])

        if dataset:
            return dataset, parts[2]

    return None, None


def arrow_to_glue_type(dtype):
    if pa.types.is_integer(dtype):
        return "bigint"

    if pa.types.is_floating(dtype):
        return "double"

    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision},{dtype.scale})"

    if pa.types.is_boolean(dtype):
        return "boolean"

    if pa.types.is_date(dtype):
        return "date"

    if pa.types.is_timestamp(dtype):
        return "timestamp"

    return "string"


def validate_parquet(data):
    try:
        reader = pq.ParquetFile(
            io.BytesIO(data)
        )

        if reader.metadata.num_rows == 0:
            return False, [], 0

        if len(reader.schema_arrow) == 0:
            return False, [], 0

        reader.read_row_group(0)

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


def delete_prefix(prefix):
    if not any(
        prefix.endswith(f"{slot}/")
        for slot in SLOTS
    ):
        raise ValueError("Invalid slot prefix")

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=prefix
    ):
        objects = [
            {"Key": obj["Key"]}
            for obj in page.get("Contents", [])
        ]

        if objects:
            s3.delete_objects(
                Bucket=S3_BUCKET,
                Delete={
                    "Objects": objects,
                    "Quiet": True
                }
            )


def load_manifest(dataset):
    try:
        response = s3.get_object(
            Bucket=S3_BUCKET,
            Key=manifest_key(dataset)
        )

        stored = json.loads(
            response["Body"].read()
        )

    except s3.exceptions.NoSuchKey:
        stored = {}

    default_manifest = {
        "dataset": dataset,
        "active_slot": None,
        "previous_slot": None,
        "version": 0,
        "slots": {},
        "last_source_id": None,
        "last_status": None,
        "updated_at": None
    }

    default_manifest.update(stored)

    if "slots" not in default_manifest:
        default_manifest["slots"] = {}

    return default_manifest


def write_manifest(dataset, manifest):
    manifest["updated_at"] = now_iso()

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key(dataset),
        Body=json.dumps(
            manifest,
            indent=2
        ),
        ContentType="application/json"
    )


def get_active_slot(dataset, manifest):
    try:
        table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=dataset
        )["Table"]

        location = table[
            "StorageDescriptor"
        ].get("Location", "")

        for slot in SLOTS:
            if location.rstrip("/").endswith(
                f"/{slot}"
            ):
                return slot

    except glue.exceptions.EntityNotFoundException:
        pass

    return manifest.get("active_slot")


def set_glue_table_pointer(
    dataset,
    location,
    columns
):
    try:
        table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=dataset
        )["Table"]

        table_input = {
            "Name": table["Name"],
            "TableType": table.get(
                "TableType",
                "EXTERNAL_TABLE"
            ),
            "Parameters": table.get(
                "Parameters",
                {}
            ),
            "StorageDescriptor": table[
                "StorageDescriptor"
            ]
        }

        table_input[
            "StorageDescriptor"
        ]["Location"] = location

        table_input[
            "StorageDescriptor"
        ]["Columns"] = columns

        glue.update_table(
            DatabaseName=GLUE_DATABASE,
            TableInput=table_input
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


def get_source_id(s3_object, key):
    etag = s3_object.get("eTag", "")

    etag = etag.strip('"')

    return f"{key}#{etag}"


def deploy(record):

    s3_info = record["s3"]

    bucket = s3_info["bucket"]["name"]

    key = urllib.parse.unquote_plus(
        s3_info["object"]["key"]
    )

    if bucket != S3_BUCKET:
        return {"status": "ignored"}

    dataset, filename = parse_incoming_key(key)

    if not dataset:
        return {"status": "ignored"}

    manifest = load_manifest(dataset)

    source_id = get_source_id(
        s3_info["object"],
        key
    )

    if manifest.get(
        "last_source_id"
    ) == source_id:

        return {
            "status": "duplicate_skipped",
            "dataset": dataset
        }

    active = get_active_slot(
        dataset,
        manifest
    )

    target = other_slot(active)

    data = s3.get_object(
        Bucket=bucket,
        Key=key
    )["Body"].read()

    is_valid, columns, rows = validate_parquet(
        data
    )

    if not is_valid:

        manifest["last_status"] = "rejected"
        manifest["last_source_id"] = source_id

        write_manifest(
            dataset,
            manifest
        )

        return {
            "status": "rejected",
            "dataset": dataset
        }

    delete_prefix(
        slot_prefix(
            dataset,
            target
        )
    )

    s3.copy(
        {
            "Bucket": bucket,
            "Key": key
        },
        S3_BUCKET,
        f"{slot_prefix(dataset, target)}{filename}"
    )

    set_glue_table_pointer(
        dataset,
        slot_location(dataset, target),
        columns
    )

    version = manifest.get(
        "version",
        0
    ) + 1

    manifest["slots"][target] = {
        "version": version,
        "rows": rows,
        "columns": columns,
        "source_key": key,
        "deployed_at": now_iso()
    }

    manifest.update({
        "active_slot": target,
        "previous_slot": active,
        "version": version,
        "last_status": "activated",
        "last_source_id": source_id
    })

    write_manifest(
        dataset,
        manifest
    )

    return {
        "status": "activated",
        "dataset": dataset,
        "active_slot": target,
        "previous_slot": active,
        "version": version,
        "rows": rows
    }


def rollback(dataset):

    manifest = load_manifest(dataset)

    previous = manifest.get(
        "previous_slot"
    )

    if previous not in SLOTS:
        return {
            "status": "error",
            "message": "No previous version available"
        }

    active = get_active_slot(
        dataset,
        manifest
    )

    if previous == active:
        return {
            "status": "error",
            "message": "Invalid rollback state"
        }

    previous_data = manifest[
        "slots"
    ].get(previous)

    if not previous_data:
        return {
            "status": "error",
            "message": "Previous deployment not found"
        }

    set_glue_table_pointer(
        dataset,
        slot_location(
            dataset,
            previous
        ),
        previous_data["columns"]
    )

    manifest.update({
        "active_slot": previous,
        "previous_slot": active,
        "last_status": "rolled_back"
    })

    write_manifest(
        dataset,
        manifest
    )

    return {
        "status": "rolled_back",
        "dataset": dataset,
        "active_slot": previous,
        "previous_slot": active
    }


def lambda_handler(event, context):

    if event.get("action") == "rollback":

        dataset = sanitize_name(
            event.get(
                "dataset_name",
                ""
            )
        )

        return rollback(dataset)

    records = event.get(
        "Records"
    )

    if not records:
        return {
            "status": "ignored"
        }

    results = [
        deploy(record)
        for record in records
    ]

    if len(results) == 1:
        return results[0]

    return {
        "status": "processed",
        "results": results
    }
