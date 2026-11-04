"""
MarginWatch - Bronze core logic (plain Python, no Spark), used by the Glue job.

For ONE landed file:
  1. read its bytes and its landing metadata (sha256, received time) from S3
  2. pick the parser for it (Excel / PDF / CSV) and parse it -> rows, values still as text
  3. map the file's column names to Bronze column names using the data contract
     (config/bronze_tables.json) and check the required columns are there
  4. add lineage columns: which file, which S3 key, its checksum, when it arrived, which row, which run
If anything is wrong with the file, it is NOT dropped and the job does NOT fail:
one row goes to bronze_quarantine with the reason, and the other 400 files carry on.
"""
import json
from datetime import datetime, timezone

from parsers import SchemaError, file_identity, pick_parser

SOURCES = ("email", "agent_bank", "collateral_hub", "reference")
QUARANTINE = "bronze_quarantine"


def load_contract(text):
    contract = json.loads(text)
    lineage = contract["_lineage"]
    tables = {k: v for k, v in contract.items() if not k.startswith("_")}
    return tables, lineage


def table_columns(tables, lineage, table):
    """Column order of a Bronze table (the partition column business_date is added by the writer)."""
    t = tables[table]
    return t["file_level"] + list(t["columns"]) + lineage


def list_landing_keys(s3, bucket, business_date):
    keys = []
    for source in SOURCES:
        prefix = f"source={source}/business_date={business_date}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].rsplit("/", 1)[-1].startswith("_"):
                    keys.append(obj["Key"])
    return sorted(keys)


def map_columns(table_spec, parsed_row):
    """File column names -> Bronze column names. Unknown or missing-required columns = SchemaError."""
    reverse = {src: dst for dst, src in table_spec["columns"].items()}
    file_cols = [c for c in parsed_row if c != "source_row"]
    unknown = [c for c in file_cols if c not in reverse]
    if unknown:
        raise SchemaError(f"new column(s) not in the data contract: {unknown}")
    out = {reverse[c]: parsed_row[c] for c in file_cols}
    missing = [c for c in table_spec["required"] if c not in out]
    if missing:
        raise SchemaError(f"required column(s) missing: {missing}")
    return out


def _as_text(v):
    return None if v is None else str(v)


def process_object(s3, bucket, key, tables, lineage, run_id, ingested_at):
    """Returns a list of (bronze_table, row_dict). Never raises for a bad file: it goes to quarantine."""
    source = key.split("/", 1)[0].split("=", 1)[1]
    file_name = key.rsplit("/", 1)[-1]
    obj = s3.get_object(Bucket=bucket, Key=key)
    meta = obj.get("Metadata", {})
    base = {
        "source_system": source, "source_file": file_name, "source_key": key,
        "source_sha256": meta.get("sha256"), "received_at_utc": meta.get("received-at-utc"),
        "source_row": None, "ingest_run_id": run_id, "ingested_at_utc": ingested_at,
    }
    table, parser = pick_parser(source, file_name)
    try:
        if parser is None:
            raise SchemaError("no parser for this file name")
        rows, _notes = parser(obj["Body"].read())
        spec = tables[table]
        ident = file_identity(file_name) if spec["file_level"] else {}
        out = []
        for r in rows:
            data = map_columns(spec, r)
            row = {c: ident.get(c) for c in spec["file_level"]}
            row.update({c: _as_text(data.get(c)) for c in spec["columns"]})
            row.update(base, source_row=_as_text(r.get("source_row")))
            out.append((table, row))
        if not out:
            raise SchemaError("file has no data rows")
        return out
    except Exception as e:                          # SchemaError, corrupt Excel/PDF, bad encoding ...
        row = {"error": f"{type(e).__name__}: {e}"[:1000], "target_table": table}
        row.update(base)
        return [(QUARANTINE, row)]


def now_utc():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
