"""Databricks job entry point for the CBACT04C migration.

NOT EXECUTED in this repository: this file is prepared for a later workspace
run; see docs/DATABRICKS-HANDOVER.md and target/databricks/README.md. It keeps
all platform I/O here and reuses the same validate_inputs + compute from
target.interest, so the business logic is identical to the local CLI.

Parameters (Databricks widgets when run as a job, argparse otherwise):
    accounts_table, xrefs_table, categories_table, rates_table   - inputs
    accounts_out_table, transactions_out_table                   - outputs
    batch_date                                                   - 10 chars
    as_of                                                        - DB2-format
        timestamp; defaults to the job run time
    run_id                                                       - required,
        used for idempotent re-runs of the transactions append

No catalog, schema, host or credential values are hard-coded; table names are
supplied by the job configuration.

Write/retry behaviour: transactions are appended first with run_id and
batch_date columns; before appending, rows for the same run_id are deleted so
a retried run is idempotent. The accounts output snapshot table is then fully
overwritten, which is also idempotent. If the job fails between the two
writes, it raises without publishing anything further; the documented retry
is to re-run the job with the SAME run_id, which removes the partial
transaction append and rewrites the accounts snapshot.
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone

from pyspark.sql import functions as F

# Platform shim only: identical business logic lives in target.interest.
from target.interest import (
    PRESERVE_EOF_DEFECT,
    UPSTREAM_COMMIT,
    InputValidationError,
    compute,
    validate_inputs,
)

RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+){0,2}$")
TABLE_PARAMS = ("accounts_table", "xrefs_table", "categories_table",
                "rates_table", "accounts_out_table", "transactions_out_table")
REQUIRED_PARAMS = TABLE_PARAMS + ("batch_date", "run_id")


def qualified_table(name, param):
    """Validate a table identifier and return it with each part backticked."""
    if not TABLE_NAME_PATTERN.fullmatch(str(name or "")):
        raise InputValidationError(
            "MALFORMED_INPUT", f"invalid {param}: {name!r}")
    return ".".join(f"`{part}`" for part in str(name).split("."))


def run_time_clock():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d-%H.%M.%S.") + f"{now.microsecond:06d}"


def run_job(spark, params):
    # run_id and table names are interpolated into SQL; bound their shapes.
    if not RUN_ID_PATTERN.fullmatch(str(params["run_id"])):
        raise InputValidationError(
            "MALFORMED_INPUT", f"invalid run_id: {params['run_id']!r}")
    tables = {name: qualified_table(params[name], name) for name in TABLE_PARAMS}
    frames = {
        "accounts": spark.table(tables["accounts_table"]),
        "xrefs": spark.table(tables["xrefs_table"]),
        "categories": spark.table(tables["categories_table"]),
        "rates": spark.table(tables["rates_table"]),
    }
    validate_inputs(frames)
    accounts_out, transactions_out = compute(
        frames, batch_date=params["batch_date"], as_of=params["as_of"])

    # Step 1: idempotent append of this run's transactions.
    spark.sql(
        f"DELETE FROM {tables['transactions_out_table']} "
        f"WHERE run_id = '{params['run_id']}'")
    (transactions_out
        .withColumn("run_id", F.lit(params["run_id"]))
        .withColumn("batch_date", F.lit(params["batch_date"]))
        .write.format("delta").mode("append")
        .saveAsTable(tables["transactions_out_table"]))

    # Step 2: full snapshot overwrite of the accounts output. If the job dies
    # between the steps, retrying with the same run_id restores consistency:
    # the DELETE above removes the earlier partial append and the overwrite
    # replaces any prior snapshot.
    accounts_out.write.format("delta").mode("overwrite") \
        .saveAsTable(tables["accounts_out_table"])

    return {"status": "success", "run_id": params["run_id"],
            "as_of": params["as_of"], "batch_date": params["batch_date"],
            "upstream_commit": UPSTREAM_COMMIT,
            "preserve_eof_defect": PRESERVE_EOF_DEFECT}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in REQUIRED_PARAMS:
        parser.add_argument(f"--{name.replace('_', '-')}", required=True)
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args(argv)
    params = vars(args)
    params["as_of"] = params["as_of"] or run_time_clock()
    return params


def _widgets(spark):
    """Read job parameters from dbutils widgets when no argv is present.

    Widgets are only READ, never created: calling widgets.text() would shadow
    missing parameters with empty strings and hide a misconfigured job. A
    widget that does not exist raises on .get() and is treated as absent.
    """
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
    except Exception:  # noqa: BLE001 - dbutils unavailable outside Databricks
        return None
    params = {}
    for name in REQUIRED_PARAMS + ("as_of",):
        try:
            params[name] = dbutils.widgets.get(name) or None
        except Exception:  # noqa: BLE001 - widget not defined for this job
            params[name] = None
    missing = [name for name in REQUIRED_PARAMS if not params[name]]
    if missing:
        raise InputValidationError(
            "MALFORMED_INPUT",
            f"missing required widget parameter(s): {', '.join(missing)}")
    params["as_of"] = params["as_of"] or run_time_clock()
    return params


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    params = parse_args() if len(sys.argv) > 1 else _widgets(spark)
    if params is None:
        raise InputValidationError(
            "MALFORMED_INPUT",
            "no parameters: pass CLI arguments or dbutils widgets")
    return run_job(spark, params)


if __name__ == "__main__":
    main()
