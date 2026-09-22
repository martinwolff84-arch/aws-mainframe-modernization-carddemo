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


def run_time_clock():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d-%H.%M.%S.") + f"{now.microsecond:06d}"


def run_job(spark, params):
    # run_id is interpolated into a DELETE statement; bound its character set.
    if not RUN_ID_PATTERN.fullmatch(str(params["run_id"])):
        raise InputValidationError(
            "MALFORMED_INPUT", f"invalid run_id: {params['run_id']!r}")
    frames = {
        "accounts": spark.table(params["accounts_table"]),
        "xrefs": spark.table(params["xrefs_table"]),
        "categories": spark.table(params["categories_table"]),
        "rates": spark.table(params["rates_table"]),
    }
    validate_inputs(frames)
    accounts_out, transactions_out = compute(
        frames, batch_date=params["batch_date"], as_of=params["as_of"])

    # Step 1: idempotent append of this run's transactions.
    spark.sql(
        f"DELETE FROM {params['transactions_out_table']} "
        f"WHERE run_id = '{params['run_id']}'")
    (transactions_out
        .withColumn("run_id", F.lit(params["run_id"]))
        .withColumn("batch_date", F.lit(params["batch_date"]))
        .write.format("delta").mode("append")
        .saveAsTable(params["transactions_out_table"]))

    # Step 2: full snapshot overwrite of the accounts output. If the job dies
    # between the steps, retrying with the same run_id restores consistency:
    # the DELETE above removes the earlier partial append and the overwrite
    # replaces any prior snapshot.
    accounts_out.write.format("delta").mode("overwrite") \
        .saveAsTable(params["accounts_out_table"])

    return {"status": "success", "run_id": params["run_id"],
            "as_of": params["as_of"], "batch_date": params["batch_date"],
            "upstream_commit": UPSTREAM_COMMIT,
            "preserve_eof_defect": PRESERVE_EOF_DEFECT}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("accounts_table", "xrefs_table", "categories_table",
                 "rates_table", "accounts_out_table", "transactions_out_table",
                 "batch_date", "run_id"):
        parser.add_argument(f"--{name.replace('_', '-')}", required=True)
    parser.add_argument("--as-of", default=None)
    args = parser.parse_args(argv)
    params = vars(args)
    params["as_of"] = params["as_of"] or run_time_clock()
    return params


def _widgets(spark):
    """Read job parameters from dbutils widgets when available."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        dbutils = DBUtils(spark)
    except Exception:  # noqa: BLE001 - dbutils unavailable outside Databricks
        return None
    names = ("accounts_table", "xrefs_table", "categories_table", "rates_table",
             "accounts_out_table", "transactions_out_table", "batch_date",
             "run_id", "as_of")
    for name in names:
        dbutils.widgets.text(name, "")
    params = {name: dbutils.widgets.get(name) for name in names}
    params["as_of"] = params["as_of"] or run_time_clock()
    return params


def main():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    params = _widgets(spark) or parse_args()
    return run_job(spark, params)


if __name__ == "__main__":
    main()
