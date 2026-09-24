import io
import json
import logging
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
glue = boto3.client("glue")
S3_BUCKET = os.environ.get("S3_BUCKET", "dataflip-analytics-dev")
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "dataflip_db")
ENFORCE_SCHEMA_COMPAT = os.environ.get("ENFORCE_SCHEMA_COMPAT", "true").lower() == "true"

SLOTS = ("blue", "green")
INCOMING_PREFIX = "incoming"
GLUE_RETRIES = 5
GLUE_TABLE_INPUT_KEYS = (
    "Name", "Description", "Owner", "Retention", "StorageDescriptor",
    "PartitionKeys", "ViewOriginalText", "ViewExpandedText",
    "TableType", "Parameters",
)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def sanitize_name(name):
    
    return re.sub(r"[^a-zA-Z0-9_]", "_", str(name)).strip("_").lower()

def other_slot(slot):
    
    return "green" if slot == "blue" else "blue"

def slot_prefix(dataset, slot):
    return f"{dataset}/{slot}/"

def slot_location(dataset, slot):
    return f"s3://{S3_BUCKET}/{dataset}/{slot}/"

def manifest_key(dataset):
    return f"{dataset}/manifest.json"

def slot_from_location(location):
    
    for slot in SLOTS:
        if location and location.rstrip("/").endswith(f"/{slot}"):
            return slot
    return None

def parse_incoming_key(key):
    
    parts = key.split("/")
    if len(parts) == 3 and parts[0] == INCOMING_PREFIX and parts[2].lower().endswith(".parquet"):
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
        reader = pq.ParquetFile(io.BytesIO(data))

        if reader.metadata.num_rows == 0:
            return False, [], 0, "file has zero rows"
        if len(reader.schema_arrow) == 0:
            return False, [], 0, "file has no columns"

        reader.read_row_group(0)

        columns = [
            {"Name": field.name, "Type": arrow_to_glue_type(field.type)}
            for field in reader.schema_arrow
        ]
        return True, columns, reader.metadata.num_rows, ""

    except Exception as exc:
        return False, [], 0, f"unreadable parquet: {exc}"

def schema_problems(active_columns, new_columns):
    
    new_types = {c["Name"].lower(): c["Type"] for c in new_columns}
    problems = []
    for col in active_columns:
        name = col["Name"].lower()
        if name not in new_types:
            problems.append(f"missing column '{name}'")
        elif new_types[name] != col["Type"]:
            problems.append(f"type of '{name}' changed {col['Type']} -> {new_types[name]}")
    return problems

def delete_prefix(prefix):
    
    if not any(prefix.endswith(f"/{slot}/") for slot in SLOTS):
        raise ValueError(f"refusing to delete non-slot prefix: {prefix}")

    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects, "Quiet": True})
            deleted += len(objects)
    return deleted

def prefix_has_objects(prefix):
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0

def source_id_for(s3_object, key):
    
    etag = s3_object.get("eTag") or s3.head_object(Bucket=S3_BUCKET, Key=key)["ETag"]
    return f"{key}#{etag.strip(chr(34))}"

def new_manifest(dataset):
    return {
        "dataset": dataset,
        "active_slot": None,
        "previous_slot": None,
        "version": 0,
        "slots": {},
        "last_status": None,
        "last_source_id": None,
        "last_reason": "",
        "updated_at": None,
    }

def load_manifest(dataset):
    
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=manifest_key(dataset))["Body"].read()
        stored = json.loads(body)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            stored = {}
        else:
            raise
    return {**new_manifest(dataset), **stored}

def write_manifest(dataset, manifest):
    manifest["updated_at"] = now_iso()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key(dataset),
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )

def glue_active_slot(dataset):
    
    try:
        table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=dataset)["Table"]
    except glue.exceptions.EntityNotFoundException:
        return None
    return slot_from_location(table["StorageDescriptor"].get("Location"))

def resolve_active_slot(dataset, manifest):
    
    manifest_slot = manifest.get("active_slot")
    glue_slot = glue_active_slot(dataset)
    if glue_slot and glue_slot != manifest_slot:
        logger.warning("Manifest/Glue drift for %s: manifest=%s glue=%s (trusting Glue)",
                       dataset, manifest_slot, glue_slot)
        return glue_slot
    return manifest_slot

def set_glue_table_pointer(dataset, location, columns):
    
    if not columns:
        raise ValueError("refusing to write a Glue table with no columns")

    for attempt in range(GLUE_RETRIES):
        try:
            try:
                table = glue.get_table(DatabaseName=GLUE_DATABASE, Name=dataset)["Table"]
            except glue.exceptions.EntityNotFoundException:
                glue.create_table(
                    DatabaseName=GLUE_DATABASE,
                    TableInput={
                        "Name": dataset,
                        "TableType": "EXTERNAL_TABLE",
                        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
                        "StorageDescriptor": {
                            "Location": location,
                            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                            "SerdeInfo": {
                                "SerializationLibrary":
                                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                            },
                            "Columns": columns,
                        },
                    },
                )
                return

            table_input = {k: v for k, v in table.items() if k in GLUE_TABLE_INPUT_KEYS}
            table_input["StorageDescriptor"]["Location"] = location
            table_input["StorageDescriptor"]["Columns"] = columns
            glue.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
            return

        except (glue.exceptions.ConcurrentModificationException,
                glue.exceptions.AlreadyExistsException):
            time.sleep(0.2 * (2 ** attempt))

    raise RuntimeError(f"Glue update for {dataset} kept conflicting after {GLUE_RETRIES} attempts")

def deploy_record(record):
    s3_info = record["s3"]
    bucket = s3_info["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_info["object"]["key"])
    dataset, filename = parse_incoming_key(key)
    if bucket != S3_BUCKET or not dataset:
        return {"status": "ignored", "key": key}

    source_id = source_id_for(s3_info["object"], key)
    manifest = load_manifest(dataset)
    if manifest["last_source_id"] == source_id:
        return {"status": "duplicate_skipped", "dataset": dataset}
    active = resolve_active_slot(dataset, manifest)
    target = other_slot(active)
    data = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    is_valid, columns, rows, reason = validate_parquet(data)

    if is_valid and ENFORCE_SCHEMA_COMPAT and active:
        problems = schema_problems(manifest["slots"].get(active, {}).get("columns", []), columns)
        if problems:
            is_valid, reason = False, "schema incompatible: " + "; ".join(problems)
    if not is_valid:
        manifest.update({"last_status": "rejected", "last_source_id": source_id, "last_reason": reason})
        write_manifest(dataset, manifest)
        logger.warning("Rejected %s: %s", key, reason)
        return {"status": "rejected", "dataset": dataset, "reason": reason, "active_slot": active}
    delete_prefix(slot_prefix(dataset, target))
    s3.copy({"Bucket": S3_BUCKET, "Key": key}, S3_BUCKET, f"{slot_prefix(dataset, target)}{filename}")
    set_glue_table_pointer(dataset, slot_location(dataset, target), columns)
    version = manifest["version"] + 1
    manifest["slots"][target] = {
        "version": version,
        "rows": rows,
        "columns": columns,
        "source_key": key,
        "deployed_at": now_iso(),
    }
    manifest.update({
        "active_slot": target,
        "previous_slot": active,
        "version": version,
        "last_status": "activated",
        "last_source_id": source_id,
        "last_reason": "",
    })
    write_manifest(dataset, manifest)

    return {"status": "activated", "dataset": dataset, "active_slot": target,
            "previous_slot": active, "version": version, "rows": rows}

def rollback(dataset):
    
    if not dataset:
        return {"status": "error", "message": "dataset_name is required"}

    manifest = load_manifest(dataset)
    previous = manifest.get("previous_slot")
    if previous not in SLOTS:
        return {"status": "error", "message": "no previous slot to roll back to"}

    active = resolve_active_slot(dataset, manifest)
    if previous == active:
        return {"status": "error", "message": "manifest and Glue disagree; refusing to roll back"}

    previous_meta = manifest["slots"].get(previous, {})
    if not previous_meta.get("columns"):
        return {"status": "error", "message": f"no schema recorded for slot '{previous}'"}
    if not prefix_has_objects(slot_prefix(dataset, previous)):
        return {"status": "error", "message": f"slot '{previous}' is empty; cannot roll back"}

    set_glue_table_pointer(dataset, slot_location(dataset, previous), previous_meta["columns"])
    manifest.update({
        "active_slot": previous,
        "previous_slot": active,
        "last_status": "rolled_back",
        "last_reason": f"rolled back from {active} to {previous}",
    })
    write_manifest(dataset, manifest)

    return {"status": "rolled_back", "dataset": dataset,
            "active_slot": previous, "previous_slot": active}

def lambda_handler(event, context):
    if event.get("action") == "rollback":
        return rollback(sanitize_name(event.get("dataset_name", "")))
    records = event.get("Records")
    if not records:
        return {"status": "ignored"}

    results = [deploy_record(r) for r in records]
    return results[0] if len(results) == 1 else {"status": "processed", "results": results}
