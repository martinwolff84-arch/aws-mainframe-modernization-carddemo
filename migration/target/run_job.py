"""Local CLI entry point for the PySpark CBACT04C migration.

Usage:
    python target/run_job.py --case baseline --fixtures fixtures/cases.json \
        --output artifacts/target-baseline.json [--as-of DB2TS] [--explain]

Writes canonical JSON {case, status, error_kind, accounts, transactions,
engine}. Exit codes: 0 success, 2 expected business rejection
(MISSING_ACCOUNT/MISSING_XREF/MISSING_RATE), 3 technical input rejection
(DUPLICATE_KEY/MALFORMED_INPUT/UNSUPPORTED_RANGE), 4 unclassified failure.
Output is written atomically (temp file in the same directory, then
os.replace) and a result JSON is written on every classified path, so no
stale success payload can be mistaken for this run's output.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import platform
import sys
import traceback
from contextlib import redirect_stdout
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from target.interest import (
    PRESERVE_EOF_DEFECT,
    REFERENCE_CLOCK,
    UPSTREAM_COMMIT,
    BusinessRejection,
    InputValidationError,
    run_case_frames,
    to_canonical,
)


def build_spark(log_level="ERROR"):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("cbact04c-migration")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel(log_level)
    return spark


def write_atomic(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def engine_block(spark, case, as_of):
    return {
        "spark_version": spark.version,
        "python": platform.python_version(),
        "java": spark._jvm.java.lang.System.getProperty("java.version"),
        "upstream_commit": UPSTREAM_COMMIT,
        "as_of": as_of,
        "batch_date": str(case.get("batch_date", "2026092200")),
        "preserve_eof_defect": PRESERVE_EOF_DEFECT,
    }


def explain(accounts_df, transactions_df):
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        print("== accounts_out ==")
        accounts_df.explain(mode="formatted")
        print("== transactions_out ==")
        transactions_df.explain(mode="formatted")
    return buffer.getvalue()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, help="Name from the fixtures file")
    parser.add_argument("--fixtures", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--as-of", default=REFERENCE_CLOCK,
                        help="DB2-format timestamp stamped on transactions")
    parser.add_argument("--explain", action="store_true",
                        help="Write formatted Spark plans to <output>.plan.txt")
    parser.add_argument("--log-level", default="ERROR",
                        help="Spark context log level (default ERROR)")
    args = parser.parse_args(argv)

    result = {"case": args.case, "status": "error", "error_kind": None,
              "accounts": None, "transactions": None, "engine": None}
    spark = build_spark(args.log_level)
    exit_code = 0
    try:
        cases = json.loads(args.fixtures.read_text())["cases"]
        case = next((c for c in cases if c["name"] == args.case), None)
        if case is None:
            raise InputValidationError("MALFORMED_INPUT", f"unknown case: {args.case}")
        result["engine"] = engine_block(spark, case, args.as_of)
        accounts_df, transactions_df = run_case_frames(spark, case, args.as_of)
        result.update(to_canonical(accounts_df, transactions_df),
                      status="success", error_kind=None)
        if args.explain:
            write_atomic(Path(str(args.output) + ".plan.txt"),
                         explain(accounts_df, transactions_df))
    except BusinessRejection as exc:
        result.update(status="error", error_kind=exc.kind, detail=exc.detail)
        exit_code = 2
    except InputValidationError as exc:
        result.update(status="error", error_kind=exc.kind, detail=exc.detail)
        exit_code = 3
    except Exception:  # noqa: BLE001 - classified as unclassified by contract
        traceback.print_exc(file=sys.stderr)
        result.update(status="error", error_kind="UNCLASSIFIED_TARGET_ERROR")
        exit_code = 4
    finally:
        spark.stop()

    write_atomic(args.output, json.dumps(result, indent=2) + "\n")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
