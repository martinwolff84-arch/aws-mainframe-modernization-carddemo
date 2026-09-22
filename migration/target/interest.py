"""PySpark migration of the CBACT04C interest-calculation batch.

Pure Spark DataFrame transformations only: no file I/O, no argparse, no dbutils.
The mainline mirrors the original main loop: category balances drive processing
in (account, type, category) key order; account and cross-reference are read at
each account boundary before any rate lookup for that account; the account-group
rate wins, falling back to DEFAULT; a present zero rate suppresses a
transaction; per-category interest truncates toward zero before summing; account
totals and cycle resets are rewritten at the next account boundary.

``PRESERVE_EOF_DEFECT`` keeps the documented original defect: the account flush
in the source's EOF branch is unreachable dead code, so the final processed
account is never rewritten. A corrected mode would need a separately reviewed
contract and separate expectations; it is intentionally not a runtime option.

Input DataFrames mirror the JSON fixture schema. The optional account fields
default to the same constants the reference adapter seeds into the COBOL
record: credit_limit 10000.00, cash_credit_limit 5000.00, open_date
"2020-01-01", expiration_date "2030-01-01", reissue_date "2025-01-01",
zip "12345", active "Y". ``frames_from_case`` applies them when keys are absent.

The transaction sequence uses ``row_number()`` over a single unpartitioned
ordering window; that is deliberate for this bounded demo input and would need
revisiting before large-volume use.
"""
from __future__ import annotations

import re
from decimal import Decimal

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

REFERENCE_CLOCK = "2026-09-22-12.00.00.000000"
BUSINESS_ERRORS = ("MISSING_ACCOUNT", "MISSING_XREF", "MISSING_RATE")
PRESERVE_EOF_DEFECT = True

UPSTREAM_COMMIT = "59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e"

MAX_SEQUENCE = 999999
MAX_INTEREST_CENTS = 10**11        # S9(9)V99 capacity in cents
MAX_ACCOUNT_TOTAL_CENTS = 10**12   # S9(10)V99 capacity in cents

ACCOUNT_DEFAULTS = {
    "credit_limit": Decimal("10000.00"),
    "cash_credit_limit": Decimal("5000.00"),
    "open_date": "2020-01-01",
    "expiration_date": "2030-01-01",
    "reissue_date": "2025-01-01",
    "zip": "12345",
    "active": "Y",
}

ACCOUNTS_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType(), False),
    T.StructField("group", T.StringType(), False),
    T.StructField("current_balance", T.DecimalType(12, 2), False),
    T.StructField("cycle_credit", T.DecimalType(12, 2), False),
    T.StructField("cycle_debit", T.DecimalType(12, 2), False),
    T.StructField("active", T.StringType(), False),
    T.StructField("credit_limit", T.DecimalType(12, 2), False),
    T.StructField("cash_credit_limit", T.DecimalType(12, 2), False),
    T.StructField("open_date", T.StringType(), False),
    T.StructField("expiration_date", T.StringType(), False),
    T.StructField("reissue_date", T.StringType(), False),
    T.StructField("zip", T.StringType(), False),
])

XREFS_SCHEMA = T.StructType([
    T.StructField("card_number", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("account_id", T.StringType(), False),
])

CATEGORIES_SCHEMA = T.StructType([
    T.StructField("account_id", T.StringType(), False),
    T.StructField("type", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("balance", T.DecimalType(11, 2), False),
])

RATES_SCHEMA = T.StructType([
    T.StructField("group", T.StringType(), False),
    T.StructField("type", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("rate", T.DecimalType(6, 2), False),
])


class InputValidationError(ValueError):
    """Technical input violation; ``kind`` names the class of defect."""

    def __init__(self, kind, detail):
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


class BusinessRejection(Exception):
    """Expected business abort; ``kind`` is one of BUSINESS_ERRORS."""

    def __init__(self, kind, detail):
        if kind not in BUSINESS_ERRORS:
            raise ValueError(f"Unknown business error kind: {kind}")
        super().__init__(f"{kind}: {detail}")
        self.kind = kind
        self.detail = detail


def _decimal(raw, field, integer_digits):
    try:
        value = Decimal(str(raw))
    except Exception as exc:
        raise InputValidationError("MALFORMED_INPUT", f"{field} is not numeric: {raw!r}") from exc
    if not value.is_finite():
        raise InputValidationError("MALFORMED_INPUT", f"{field} is not numeric: {raw!r}")
    if value.is_nan():
        raise InputValidationError("MALFORMED_INPUT", f"{field} is not numeric: {raw!r}")
    if value.as_tuple().exponent < -2:
        raise InputValidationError("MALFORMED_INPUT", f"{field} has more than 2 decimals: {raw!r}")
    if abs(value) >= Decimal(10) ** integer_digits:
        raise InputValidationError("UNSUPPORTED_RANGE",
                                   f"{field}={value} exceeds S9({integer_digits})V99 capacity")
    return value


def _normalize_account_id(raw):
    value = str(raw)
    if not re.fullmatch(r"\d{1,11}", value):
        raise InputValidationError("MALFORMED_INPUT", f"account_id is not 1-11 digits: {raw!r}")
    return value.zfill(11)


def _normalize_category(raw):
    value = str(raw)
    if not re.fullmatch(r"\d{1,4}", value):
        raise InputValidationError("MALFORMED_INPUT", f"category is not 1-4 digits: {raw!r}")
    return value.zfill(4)


def _normalize_type(raw):
    value = str(raw)
    if len(value) != 2:
        raise InputValidationError("MALFORMED_INPUT", f"type is not exactly 2 characters: {raw!r}")
    return value


def _normalize_group(raw):
    value = str(raw)
    if not value or len(value) > 10:
        raise InputValidationError("MALFORMED_INPUT", f"group must be 1-10 characters: {raw!r}")
    return value


def _normalize_customer_id(raw):
    value = str(raw)
    if not re.fullmatch(r"\d{1,9}", value):
        raise InputValidationError("MALFORMED_INPUT", f"customer_id is not 1-9 digits: {raw!r}")
    return value.zfill(9)


def _require(raw, field, record_label):
    if field not in raw:
        raise InputValidationError(
            "MALFORMED_INPUT", f"{record_label} record is missing {field!r}")
    return raw[field]


def frames_from_case(spark, case):
    """Build the four input DataFrames from a fixture case dict.

    Normalizes source identifiers to their COBOL widths, applies the reference
    adapter's seeded defaults for absent optional account fields, and validates
    numeric ranges without ever converting money through binary float.
    """
    batch_date = str(case.get("batch_date", "2026092200"))
    if len(batch_date) != 10:
        raise InputValidationError("MALFORMED_INPUT",
                                   f"batch_date must have exactly 10 characters: {batch_date!r}")

    accounts = []
    for raw in case.get("accounts") or []:
        accounts.append({
            "account_id": _normalize_account_id(_require(raw, "account_id", "accounts")),
            "group": _normalize_group(_require(raw, "group", "accounts")),
            "current_balance": _decimal(_require(raw, "current_balance", "accounts"),
                                        "current_balance", 10),
            "cycle_credit": _decimal(raw.get("cycle_credit", "0"), "cycle_credit", 10),
            "cycle_debit": _decimal(raw.get("cycle_debit", "0"), "cycle_debit", 10),
            "active": str(raw.get("active", ACCOUNT_DEFAULTS["active"])),
            "credit_limit": _decimal(raw.get("credit_limit", ACCOUNT_DEFAULTS["credit_limit"]),
                                     "credit_limit", 10),
            "cash_credit_limit": _decimal(
                raw.get("cash_credit_limit", ACCOUNT_DEFAULTS["cash_credit_limit"]),
                "cash_credit_limit", 10),
            "open_date": str(raw.get("open_date", ACCOUNT_DEFAULTS["open_date"])),
            "expiration_date": str(raw.get("expiration_date", ACCOUNT_DEFAULTS["expiration_date"])),
            "reissue_date": str(raw.get("reissue_date", ACCOUNT_DEFAULTS["reissue_date"])),
            "zip": str(raw.get("zip", ACCOUNT_DEFAULTS["zip"])),
        })

    xrefs = []
    for raw in case.get("xrefs") or []:
        card_number = str(_require(raw, "card_number", "xrefs"))
        if len(card_number) != 16:
            raise InputValidationError("MALFORMED_INPUT",
                                       f"card_number is not exactly 16 characters: {card_number!r}")
        xrefs.append({
            "card_number": card_number,
            "customer_id": _normalize_customer_id(_require(raw, "customer_id", "xrefs")),
            "account_id": _normalize_account_id(_require(raw, "account_id", "xrefs")),
        })

    categories = []
    for raw in case.get("categories") or []:
        categories.append({
            "account_id": _normalize_account_id(_require(raw, "account_id", "categories")),
            "type": _normalize_type(_require(raw, "type", "categories")),
            "category": _normalize_category(_require(raw, "category", "categories")),
            "balance": _decimal(_require(raw, "balance", "categories"),
                                "category balance", 9),
        })

    rates = []
    for raw in case.get("rates") or []:
        rates.append({
            "group": _normalize_group(_require(raw, "group", "rates")),
            "type": _normalize_type(_require(raw, "type", "rates")),
            "category": _normalize_category(_require(raw, "category", "rates")),
            "rate": _decimal(_require(raw, "rate", "rates"), "rate", 4),
        })

    return {
        "accounts": spark.createDataFrame(accounts, ACCOUNTS_SCHEMA),
        "xrefs": spark.createDataFrame(xrefs, XREFS_SCHEMA),
        "categories": spark.createDataFrame(categories, CATEGORIES_SCHEMA),
        "rates": spark.createDataFrame(rates, RATES_SCHEMA),
        "batch_date": batch_date,
    }


def _check_unique(frame, keys, label):
    duplicates = (frame.groupBy(*keys).count()
                  .where("count > 1").select(*keys).limit(5).collect())
    if duplicates:
        shown = ", ".join(str(tuple(row)) for row in duplicates)
        raise InputValidationError("DUPLICATE_KEY", f"duplicate {label} key(s): {shown}")


def _reject_offenders(frame, condition, kind, label):
    offenders = frame.where(condition).limit(5).collect()
    if offenders:
        shown = ", ".join(str(tuple(row))[:120] for row in offenders)
        raise InputValidationError(kind, f"{label}: {shown}")


def _shape(frame, column, pattern, label):
    # Null is a shape violation: SQL three-valued logic would let it through.
    _reject_offenders(frame,
                      F.col(column).isNull() | ~F.col(column).rlike(pattern),
                      "MALFORMED_INPUT", f"{label} violates shape {pattern}")


def _range(frame, column, limit, label):
    _reject_offenders(frame,
                      F.col(column).isNull()
                      | (F.abs(F.col(column)) >= F.lit(Decimal(limit))),
                      "UNSUPPORTED_RANGE", f"{label} exceeds capacity {limit}")


def _scale(frame, column, label):
    # Two decimal places max; a wider schema (e.g. DecimalType(11,3)) can carry
    # fractional cents that COBOL S9(n)V99 fields could never hold.
    hundred = F.col(column) * F.lit(100)
    _reject_offenders(frame,
                      F.col(column).isNull() | (hundred != F.floor(hundred)),
                      "MALFORMED_INPUT", f"{label} has more than 2 decimals")


def validate_shapes(frames):
    """Enforce COBOL field shapes and S9 picture ranges on the DataFrames.

    Independent of frames_from_case so callers that build frames differently
    (e.g. the Databricks entry point reading spark.table) get the same checks.
    Shapes raise MALFORMED_INPUT; range overflows raise UNSUPPORTED_RANGE.
    """
    accounts, xrefs = frames["accounts"], frames["xrefs"]
    categories, rates = frames["categories"], frames["rates"]
    _shape(accounts, "account_id", r"^\d{11}$", "accounts.account_id")
    _shape(accounts, "group", r"^.{1,10}$", "accounts.group")
    for column in ("current_balance", "cycle_credit", "cycle_debit",
                   "credit_limit", "cash_credit_limit"):
        _scale(accounts, column, f"accounts.{column}")
        _range(accounts, column, "10000000000", f"accounts.{column}")
    _shape(xrefs, "card_number", r"^.{16}$", "xrefs.card_number")
    _shape(xrefs, "customer_id", r"^\d{9}$", "xrefs.customer_id")
    _shape(xrefs, "account_id", r"^\d{11}$", "xrefs.account_id")
    _shape(categories, "account_id", r"^\d{11}$", "categories.account_id")
    _shape(categories, "type", r"^.{2}$", "categories.type")
    _shape(categories, "category", r"^\d{4}$", "categories.category")
    _scale(categories, "balance", "categories.balance")
    _range(categories, "balance", "1000000000", "categories.balance")
    _shape(rates, "group", r"^.{1,10}$", "rates.group")
    _shape(rates, "type", r"^.{2}$", "rates.type")
    _shape(rates, "category", r"^\d{4}$", "rates.category")
    _scale(rates, "rate", "rates.rate")
    _range(rates, "rate", "10000", "rates.rate")


def validate_inputs(frames):
    """Shared input validation: shapes, ranges and indexed-key uniqueness.

    Runs on the four DataFrames so every entry point (local CLI via
    frames_from_case, Databricks via spark.table) gets identical checks.
    The COBOL files are indexed: account primary key, xref primary card number
    plus a unique alternate account key (no DUPLICATES clause), category
    (account, type, category), rate (group, type, category). Duplicate keys
    would silently multiply or shadow rows, so they are a technical rejection.
    """
    validate_shapes(frames)
    _check_unique(frames["accounts"], ["account_id"], "accounts.account_id")
    _check_unique(frames["xrefs"], ["card_number"], "xrefs.card_number")
    _check_unique(frames["xrefs"], ["account_id"], "xrefs.account_id")
    _check_unique(frames["categories"], ["account_id", "type", "category"],
                  "categories(account_id,type,category)")
    _check_unique(frames["rates"], ["group", "type", "category"],
                  "rates(group,type,category)")


def compute(frames, batch_date, as_of=REFERENCE_CLOCK):
    """Run the migrated batch; return (accounts_out, transactions_out).

    All business logic executes as DataFrame operations; only single-row
    aggregates (first error, max bounds, row counts) reach the driver.
    """
    if len(str(batch_date)) != 10:
        raise InputValidationError("MALFORMED_INPUT",
                                   f"batch_date must have exactly 10 characters: {batch_date!r}")
    batch_date = str(batch_date)
    accounts, xrefs = frames["accounts"], frames["xrefs"]
    categories, rates = frames["categories"], frames["rates"]

    joined = (
        categories
        .join(accounts.select("account_id", "group").withColumn("account_present", F.lit(True)),
              "account_id", "left")
        .join(xrefs.select("account_id", "card_number").withColumn("xref_present", F.lit(True)),
              "account_id", "left")
        .join(rates.select("group", "type", "category", F.col("rate").alias("specific_rate")),
              ["group", "type", "category"], "left")
        .join(rates.where(F.col("group") == "DEFAULT")
                   .select("type", "category", F.col("rate").alias("default_rate")),
              ["type", "category"], "left")
        .withColumn("account_present", F.coalesce("account_present", F.lit(False)))
        .withColumn("xref_present", F.coalesce("xref_present", F.lit(False)))
        .withColumn("rate", F.coalesce("specific_rate", "default_rate"))
    )

    # Source precedence: account and xref are read at the account boundary
    # before any rate lookup for that account; rows are processed in key order,
    # so the first error in (account_id, type, category) order reproduces the
    # original abort order exactly.
    errored = joined.withColumn(
        "error_kind",
        F.when(~F.col("account_present"), F.lit("MISSING_ACCOUNT"))
         .when(~F.col("xref_present"), F.lit("MISSING_XREF"))
         .when(F.col("rate").isNull(), F.lit("MISSING_RATE")))
    first_error = (errored.where(F.col("error_kind").isNotNull())
                   .orderBy("account_id", "type", "category").limit(1).collect())
    if first_error:
        row = first_error[0]
        raise BusinessRejection(row["error_kind"], row["account_id"])

    # Exact integer arithmetic: COBOL truncates toward zero without ROUNDED.
    # Spark SQL `DIV` on integral operands truncates toward zero (verified for
    # negatives in tests/test_target.py); decimal division must not be used
    # here because it rounds HALF_UP at a higher intermediate scale.
    priced = (errored
              .withColumn("balance_cents", (F.col("balance") * 100).cast(T.LongType()))
              .withColumn("rate_bp", (F.col("rate") * 100).cast(T.LongType()))
              .withColumn("interest_cents",
                          F.expr("(balance_cents * rate_bp) DIV 120000").cast(T.LongType())))

    bounds = priced.agg(
        F.max(F.abs("interest_cents")).alias("max_interest_cents"),
        F.count(F.when(F.col("rate") != 0, 1)).alias("emitted"),
    ).first()
    if bounds["max_interest_cents"] is not None and \
            bounds["max_interest_cents"] >= MAX_INTEREST_CENTS:
        raise InputValidationError(
            "UNSUPPORTED_RANGE", "interest result exceeds S9(9)V99 capacity")
    if bounds["emitted"] is not None and bounds["emitted"] > MAX_SEQUENCE:
        raise InputValidationError(
            "UNSUPPORTED_RANGE", "transaction sequence exceeds the 6-digit suffix")

    emitted = (priced.where(F.col("rate") != 0)
               .withColumn("seq", F.row_number().over(
                   Window.orderBy("account_id", "type", "category")))
               .select(
                   F.concat(F.lit(batch_date), F.lpad("seq", 6, "0")).alias("transaction_id"),
                   F.lit("01").alias("type"),
                   F.lit("0005").alias("category"),
                   F.lit("System").alias("source"),
                   F.concat(F.lit("Int. for a/c "), F.col("account_id")).alias("description"),
                   (F.col("interest_cents").cast(T.DecimalType(20, 0))
                    * F.lit(Decimal("0.01"))).cast(T.DecimalType(11, 2)).alias("amount"),
                   F.col("card_number"),
                   F.lit(as_of).alias("original_timestamp"),
                   F.lit(as_of).alias("processing_timestamp"),
                   F.lit("000000000").alias("merchant_id"),
                   F.lit("").alias("merchant_name"),
                   F.lit("").alias("merchant_city"),
                   F.lit("").alias("merchant_zip"))
               .orderBy("seq"))

    totals = priced.groupBy("account_id").agg(
        F.sum("interest_cents").alias("total_cents"))
    # WS-TOTAL-INT is S9(9)V99 in the source: the per-account sum must fit too.
    max_total = totals.agg(F.max(F.abs("total_cents")).alias("m")).first()["m"]
    if max_total is not None and max_total >= MAX_INTEREST_CENTS:
        raise InputValidationError(
            "UNSUPPORTED_RANGE", "account interest total exceeds S9(9)V99 capacity")

    # The original rewrites the previous account when the next account's first
    # row is read; the last processed account is never rewritten (preserved EOF
    # defect). Accounts with zero-rate rows still count as processed and are
    # rewritten with total 0 and reset cycles.
    last_account = categories.select(F.max("account_id").alias("account_id")).first()["account_id"]
    updated = totals
    if PRESERVE_EOF_DEFECT:
        updated = updated.where(F.col("account_id") != F.lit(last_account))

    accounts_out = (
        accounts.join(updated, "account_id", "left")
        .withColumn("new_balance_cents",
                    F.when(F.col("total_cents").isNotNull(),
                           (F.col("current_balance") * 100).cast(T.LongType())
                           + F.col("total_cents")))
        .withColumn("current_balance",
                    F.when(F.col("new_balance_cents").isNull(), F.col("current_balance"))
                     .otherwise((F.col("new_balance_cents").cast(T.DecimalType(20, 0))
                                 * F.lit(Decimal("0.01"))).cast(T.DecimalType(12, 2))))
        .withColumn("cycle_credit",
                    F.when(F.col("total_cents").isNotNull(), F.lit(Decimal("0.00")))
                     .otherwise(F.col("cycle_credit")).cast(T.DecimalType(12, 2)))
        .withColumn("cycle_debit",
                    F.when(F.col("total_cents").isNotNull(), F.lit(Decimal("0.00")))
                     .otherwise(F.col("cycle_debit")).cast(T.DecimalType(12, 2)))
        .drop("total_cents")
        .orderBy("account_id"))

    over = accounts_out.where(F.col("new_balance_cents").isNotNull()).agg(
        F.max(F.abs("new_balance_cents")).alias("max_cents")).first()["max_cents"]
    if over is not None and over >= MAX_ACCOUNT_TOTAL_CENTS:
        raise InputValidationError(
            "UNSUPPORTED_RANGE", "account total exceeds S9(10)V99 capacity")
    accounts_out = accounts_out.drop("new_balance_cents")

    return accounts_out, emitted


def _money(value):
    amount = Decimal(str(value))
    return "0.00" if amount == 0 else format(amount, ".2f")


def to_canonical(accounts_df, transactions_df):
    """Collect the bounded final result into the canonical JSON field names."""
    accounts = [
        {"account_id": r["account_id"], "group": r["group"],
         "current_balance": _money(r["current_balance"]),
         "cycle_credit": _money(r["cycle_credit"]),
         "cycle_debit": _money(r["cycle_debit"]),
         "active": r["active"],
         "credit_limit": _money(r["credit_limit"]),
         "cash_credit_limit": _money(r["cash_credit_limit"]),
         "open_date": r["open_date"], "expiration_date": r["expiration_date"],
         "reissue_date": r["reissue_date"], "zip": r["zip"]}
        for r in accounts_df.orderBy("account_id").collect()
    ]
    transactions = [
        {"transaction_id": r["transaction_id"], "type": r["type"],
         "category": r["category"],
         "source": r["source"], "description": r["description"],
         "amount": _money(r["amount"]), "card_number": r["card_number"],
         "original_timestamp": r["original_timestamp"],
         "processing_timestamp": r["processing_timestamp"],
         "merchant_id": r["merchant_id"], "merchant_name": r["merchant_name"],
         "merchant_city": r["merchant_city"], "merchant_zip": r["merchant_zip"]}
        for r in transactions_df.orderBy("transaction_id").collect()
    ]
    return {"accounts": accounts, "transactions": transactions}


def run_case_frames(spark, case, as_of=REFERENCE_CLOCK):
    """Load, validate and compute one case; shared by CLI and job entry points.

    Returns (accounts_out_df, transactions_out_df) and lets
    InputValidationError / BusinessRejection propagate to the caller, which
    owns the classification policy for its environment.
    """
    frames = frames_from_case(spark, case)
    validate_inputs(frames)
    return compute(frames, frames["batch_date"], as_of)
