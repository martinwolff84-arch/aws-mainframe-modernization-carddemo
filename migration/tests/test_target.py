"""Tests for the PySpark CBACT04C target implementation.

One module-scoped SparkSession; business logic is exercised through the real
DataFrame path and read back via to_canonical. The acceptance harness
(scripts/compare_target.py) is separate; these are unit-level checks.
"""
import copy
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import os

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from target.interest import (
    BusinessRejection,
    InputValidationError,
    frames_from_case,
    run_case_frames,
    to_canonical,
    validate_inputs,
)

FIXTURES = json.loads((ROOT / "fixtures/cases.json").read_text())["cases"]
EVIDENCE_BASELINE = json.loads(
    (ROOT / "evidence/reference/baseline.json").read_text())


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("cbact04c-test")
               .config("spark.ui.enabled", "false")
               .config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def load_case(name="baseline"):
    return copy.deepcopy(next(c for c in FIXTURES if c["name"] == name))


def run_case(spark, case):
    accounts_df, transactions_df = run_case_frames(spark, case)
    return to_canonical(accounts_df, transactions_df)


def account(result, account_id):
    return next(a for a in result["accounts"] if a["account_id"] == account_id)


def amounts(result):
    return [t["amount"] for t in result["transactions"]]


def case_with(account=None, categories=None, rates=None, xrefs=None):
    """Return a minimal one-account case built on the baseline's shape."""
    case = load_case("baseline")
    case["name"] = "test"
    case["accounts"] = account if account is not None else [
        dict(a) for a in case["accounts"][:1]]
    if xrefs is not None:
        case["xrefs"] = xrefs
    else:
        case["xrefs"] = [x for x in case["xrefs"]
                         if x["account_id"] in {a["account_id"] for a in case["accounts"]}]
    if categories is not None:
        case["categories"] = categories
    else:
        case["categories"] = [c for c in case["categories"]
                              if c["account_id"] in {a["account_id"] for a in case["accounts"]}]
    if rates is not None:
        case["rates"] = rates
    return case


def simple_rate_case(balance, rate, account=None, category="0010"):
    """Single account/category with a STANDARD specific rate."""
    case = case_with()
    case["categories"] = [{"account_id": case["accounts"][0]["account_id"],
                           "type": "01", "category": category, "balance": balance}]
    case["rates"] = [{"group": "STANDARD", "type": "01", "category": category,
                      "rate": rate}]
    return case


# --- canonical baseline -----------------------------------------------------

def test_baseline_matches_reference_evidence(spark):
    result = run_case(spark, load_case("baseline"))
    assert result["accounts"] == EVIDENCE_BASELINE["accounts"]
    assert result["transactions"] == EVIDENCE_BASELINE["transactions"]


def test_input_order_case_matches_baseline(spark):
    result = run_case(spark, load_case("input_order"))
    expected = run_case(spark, load_case("baseline"))
    assert result == expected


# --- arithmetic anchors -----------------------------------------------------

@pytest.mark.parametrize("balance,rate,expected", [
    ("1200.00", "12.00", "12.00"),
    ("100.50", "12.00", "1.00"),
    ("100.99", "12.00", "1.00"),
    ("-100.50", "12.00", "-1.00"),
    ("100.50", "-12.00", "-1.00"),
    ("-100.50", "-12.00", "1.00"),
    ("12345.67", "9.99", "102.77"),
    ("-12345.67", "9.99", "-102.77"),
])
def test_arithmetic_anchors(spark, balance, rate, expected):
    result = run_case(spark, simple_rate_case(balance, rate))
    assert amounts(result) == [expected]


def test_fractional_cents_do_not_sum_to_a_cent(spark):
    case = case_with()
    acct = case["accounts"][0]["account_id"]
    case["categories"] = [
        {"account_id": acct, "type": "01", "category": "0010", "balance": "0.50"},
        {"account_id": acct, "type": "01", "category": "0011", "balance": "0.50"},
    ]
    case["rates"] = [
        {"group": "STANDARD", "type": "01", "category": "0010", "rate": "12.00"},
        {"group": "STANDARD", "type": "01", "category": "0011", "rate": "12.00"},
    ]
    result = run_case(spark, case)
    assert amounts(result) == ["0.00", "0.00"]
    # truncation happens per category before summing
    assert account(result, acct)["current_balance"] == "1000.00"


@pytest.mark.parametrize("balance,rate,expected", [
    ("30.03", "9.99", "0.24"),     # decimal division would give 0.25
    ("165.02", "7.49", "1.02"),    # decimal division would give 1.03
    ("-30.03", "9.99", "-0.24"),
    ("-165.02", "7.49", "-1.02"),
])
def test_no_decimal_division_hazard(spark, balance, rate, expected):
    result = run_case(spark, simple_rate_case(balance, rate))
    assert amounts(result) == [expected]


def test_spark_div_truncates_toward_zero(spark):
    # -100.50 x 12.00 -> -10050 * 1200 = -120600; COBOL truncation needs -100,
    # not the -101 a floor division would give.
    df = spark.createDataFrame([(-10050, 1200)],
                               T.StructType([T.StructField("a", T.LongType()),
                                             T.StructField("b", T.LongType())]))
    out = df.select(F.expr("(a * b) DIV 120000").alias("c")).first()["c"]
    assert out == -100


# --- precision boundaries ---------------------------------------------------

@pytest.mark.parametrize("balance", ["120000000.00", "-120000000.00"])
def test_max_in_range_balance_accepted(spark, balance):
    result = run_case(spark, simple_rate_case(balance, "9999.99"))
    sign = "-" if balance.startswith("-") else ""
    assert amounts(result) == [f"{sign}999999000.00"]


@pytest.mark.parametrize("balance", ["999999999.99", "-999999999.99"])
def test_interest_overflow_rejected(spark, balance):
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, simple_rate_case(balance, "9999.99"))
    assert exc.value.kind == "UNSUPPORTED_RANGE"


def test_balance_over_input_capacity_rejected(spark):
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, simple_rate_case("1000000000.00", "12.00"))
    assert exc.value.kind == "UNSUPPORTED_RANGE"


def test_balance_with_three_decimals_rejected(spark):
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, simple_rate_case("100.505", "12.00"))
    assert exc.value.kind == "MALFORMED_INPUT"


# --- duplicate keys ----------------------------------------------------------

def test_duplicate_account_row_rejected(spark):
    case = load_case("baseline")
    case["accounts"].append(dict(case["accounts"][0]))
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "DUPLICATE_KEY"


def test_duplicate_card_number_rejected(spark):
    case = load_case("baseline")
    dup = dict(case["xrefs"][0])
    dup["account_id"] = "00000000002"
    case["xrefs"].append(dup)
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "DUPLICATE_KEY"


def test_duplicate_xref_account_rejected(spark):
    case = load_case("baseline")
    dup = dict(case["xrefs"][0])
    dup["card_number"] = "9999000000000000"
    case["xrefs"].append(dup)
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "DUPLICATE_KEY"


def test_duplicate_category_key_rejected(spark):
    case = load_case("baseline")
    case["categories"].append(dict(case["categories"][0]))
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "DUPLICATE_KEY"


def test_duplicate_rate_key_rejected(spark):
    case = load_case("baseline")
    case["rates"].append(dict(case["rates"][0]))
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "DUPLICATE_KEY"


# --- xref field validation ---------------------------------------------------

def test_validate_shapes_rejects_bad_width_on_dataframes(spark):
    """validate_inputs on hand-built frames catches shapes frames_from_case
    would never produce (the Databricks path has no JSON normalisation)."""
    frames = frames_from_case(spark, load_case("baseline"))
    frames["xrefs"] = spark.createDataFrame(
        [("4444", "000000001", "00000000001")], frames["xrefs"].schema)
    with pytest.raises(InputValidationError) as exc:
        validate_inputs(frames)
    assert exc.value.kind == "MALFORMED_INPUT"


def test_validate_shapes_rejects_out_of_range_rate(spark):
    frames = frames_from_case(spark, load_case("baseline"))
    wide = T.StructType([T.StructField("group", T.StringType()),
                         T.StructField("type", T.StringType()),
                         T.StructField("category", T.StringType()),
                         T.StructField("rate", T.DecimalType(10, 2))])
    frames["rates"] = spark.createDataFrame(
        [("STANDARD", "01", "0010", Decimal("12345.00"))], wide)
    with pytest.raises(InputValidationError) as exc:
        validate_inputs(frames)
    assert exc.value.kind == "UNSUPPORTED_RANGE"


def test_short_card_number_rejected(spark):
    case = load_case("baseline")
    case["xrefs"][0]["card_number"] = "444400000000001"
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "MALFORMED_INPUT"


def test_non_numeric_customer_id_rejected(spark):
    case = load_case("baseline")
    case["xrefs"][0]["customer_id"] = "12X"
    with pytest.raises(InputValidationError) as exc:
        run_case(spark, case)
    assert exc.value.kind == "MALFORMED_INPUT"


# --- business error precedence ----------------------------------------------

def test_missing_xref_beats_missing_rate_and_later_missing_account(spark):
    case = load_case("baseline")
    case["accounts"] = [a for a in case["accounts"] if a["account_id"] != "00000000005"]
    case["xrefs"] = [x for x in case["xrefs"] if x["account_id"] != "00000000002"]
    case["rates"] = [r for r in case["rates"]
                     if not (r["group"] == "DEFAULT" and r["type"] == "01"
                             and r["category"] == "0010")]
    with pytest.raises(BusinessRejection) as exc:
        run_case(spark, case)
    assert exc.value.kind == "MISSING_XREF"
    assert exc.value.detail == "00000000002"


def test_missing_rate_beats_later_missing_account(spark):
    case = load_case("baseline")
    case["accounts"] = [a for a in case["accounts"] if a["account_id"] != "00000000004"]
    case["rates"] = [r for r in case["rates"]
                     if not (r["group"] == "DEFAULT" and r["type"] == "02"
                             and r["category"] == "0010")]
    with pytest.raises(BusinessRejection) as exc:
        run_case(spark, case)
    assert exc.value.kind == "MISSING_RATE"
    assert exc.value.detail == "00000000002"


def test_missing_account_wins_when_everything_broken(spark):
    case = load_case("baseline")
    case["accounts"] = [a for a in case["accounts"] if a["account_id"] != "00000000001"]
    case["xrefs"] = [x for x in case["xrefs"] if x["account_id"] != "00000000002"]
    case["rates"] = [r for r in case["rates"] if r["group"] != "DEFAULT"]
    with pytest.raises(BusinessRejection) as exc:
        run_case(spark, case)
    assert exc.value.kind == "MISSING_ACCOUNT"
    assert exc.value.detail == "00000000001"


# --- EOF defect and zero-rate behaviour --------------------------------------

def test_last_processed_account_not_rewritten_when_zero_rate(spark):
    case = load_case("baseline")
    for a in case["accounts"]:
        if a["account_id"] == "00000000006":
            a["group"] = "ZERO"
    result = run_case(spark, case)
    assert all("00000000006" not in t["description"] for t in result["transactions"])
    acct6 = account(result, "00000000006")
    assert (acct6["current_balance"], acct6["cycle_credit"],
            acct6["cycle_debit"]) == ("900.00", "8.00", "9.00")
    acct5 = account(result, "00000000005")
    assert acct5["cycle_credit"] == "0.00" and acct5["cycle_debit"] == "0.00"
    assert acct5["current_balance"] == "50.00"


def test_removed_last_account_shifts_eof_defect(spark):
    case = load_case("baseline")
    case["categories"] = [c for c in case["categories"]
                          if c["account_id"] != "00000000006"]
    result = run_case(spark, case)
    acct5 = account(result, "00000000005")
    assert (acct5["current_balance"], acct5["cycle_credit"],
            acct5["cycle_debit"]) == ("50.00", "10.00", "20.00")
    tx5 = [t for t in result["transactions"] if "00000000005" in t["description"]]
    assert [t["amount"] for t in tx5] == ["0.00", "0.00"]


def test_specific_zero_rate_suppresses_transaction(spark):
    result = run_case(spark, load_case("baseline"))
    assert all("00000000003" not in t["description"] for t in result["transactions"])
    acct3 = account(result, "00000000003")
    assert (acct3["current_balance"], acct3["cycle_credit"],
            acct3["cycle_debit"]) == ("500.00", "0.00", "0.00")


# --- CLI contract -------------------------------------------------------------

def _run_cli(case, output, *extra):
    return subprocess.run(
        [sys.executable, str(ROOT / "target/run_job.py"),
         "--case", case, "--fixtures", str(ROOT / "fixtures/cases.json"),
         "--output", str(output), *extra],
        cwd=ROOT, capture_output=True, text=True, timeout=300, check=False)


def test_cli_repeatability_byte_identical(tmp_path):
    out1, out2 = tmp_path / "a.json", tmp_path / "b.json"
    assert _run_cli("baseline", out1).returncode == 0
    assert _run_cli("baseline", out2).returncode == 0
    assert out1.read_bytes() == out2.read_bytes()


def test_cli_error_case_writes_null_arrays(tmp_path):
    out = tmp_path / "err.json"
    proc = _run_cli("missing_account", out)
    assert proc.returncode != 0
    payload = json.loads(out.read_text())
    assert payload["status"] == "error"
    assert payload["error_kind"] == "MISSING_ACCOUNT"
    assert payload["accounts"] is None and payload["transactions"] is None
    assert not (tmp_path / "err.json.tmp").exists()


def test_cli_success_exit_and_output(tmp_path):
    out = tmp_path / "ok.json"
    proc = _run_cli("baseline", out)
    assert proc.returncode == 0
    payload = json.loads(out.read_text())
    assert payload["status"] == "success" and payload["error_kind"] is None
    assert payload["accounts"] and payload["transactions"]
    assert not (tmp_path / "ok.json.tmp").exists()
