# Target implementation notes — PySpark CBACT04C

Local PySpark 3.5.6 implementation of the CBACT04C interest batch. Databricks
execution is prepared (`target/databricks/`) but **not run**. Reviewed against
upstream commit `59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`.

## COBOL paragraph → DataFrame mapping (`target/interest.py`)

| COBOL | Spark step |
|---|---|
| Main loop over TCATBALF in key order | `categories` drives the pipeline; explicit `orderBy(account_id, type, category)` everywhere order matters — input array order is ignored, matching indexed-file key order |
| `1100-GET-ACCT-DATA` / `1110-GET-XREF-DATA` at account boundary | Left joins of categories → accounts / xrefs on `account_id` produce `account_present` / `xref_present` flags per row |
| `1200-GET-INTEREST-RATE` + `1200-A` default fallback | Left join to rates on `(group, type, category)` as `specific_rate`, plus rates filtered to `group == 'DEFAULT'` on `(type, category)`; `rate = coalesce(specific_rate, default_rate)`. A present zero rate is authoritative and never falls back |
| Abort order (account → xref → rate, per key) | Per-row error kind; the first non-null error in key order is raised as `BusinessRejection`. This reproduces the original precedence exactly: boundary reads precede the account's first rate lookup, and processing follows key order |
| `1300-COMPUTE-INTEREST` | `interest_cents = (balance*100 × rate*100) DIV 120000` in integer cents |
| `DIS-INT-RATE NOT = 0` guard | Emitted transactions are rows with `rate != 0`; zero-rate rows still count toward the account total (0) |
| `1300-B-WRITE-TX` | `row_number()` sequence, `batch_date`-prefixed `transaction_id`, fixed type `01` / category `0005` / source `System` / merchant fields / `Int. for a/c <id>` description / `as_of` timestamps |
| `1050-UPDATE-ACCOUNT` at next-account boundary | `sum(interest_cents)` per processed account; updated accounts get `current_balance + total`, `cycle_credit = 0`, `cycle_debit = 0`; other fields unchanged |
| EOF defect: final account never flushed | `PRESERVE_EOF_DEFECT = True` excludes the maximum processed `account_id` from the rewrite set — matching the unreachable EOF flush branch in the source |
| `1400-COMPUTE-FEES` | Unimplemented upstream; deliberately not implemented |

## Arithmetic choice and the division hazard

COBOL truncates toward zero (no `ROUNDED`), per category, before summing. The
implementation multiplies to integer cents (`balance_cents × rate_bp / 120000`)
and uses Spark SQL `DIV` on `LongType`, which truncates toward zero
(verified for negatives in `test_spark_div_truncates_toward_zero`). Spark
decimal division is explicitly **not** used: it rounds `HALF_UP` at an
intermediate scale, e.g. 30.03 × 9.99 / 1200 = 0.2500025 → 0.25 where COBOL
yields 0.24, and 165.02 × 7.49 / 1200 = 1.0300… where COBOL yields 1.02.
Amounts are re-materialized as `interest_cents × 0.01` cast to `decimal(11,2)`
(exact); money never passes through binary float.

## Validation policy

**Technical errors** (`InputValidationError`, CLI exit 3) — invalid input
shapes the source could never see through its record layouts:

- `DUPLICATE_KEY`: duplicates on indexed keys — `accounts.account_id`,
  `xrefs.card_number`, `xrefs.account_id` (the COBOL xref has a unique
  alternate key, no DUPLICATES), `categories(account_id,type,category)`,
  `rates(group,type,category)`. Checked with Spark `groupBy`/`count`.
- `MALFORMED_INPUT`: identifier shape/width, >2 decimal places, non-numeric
  fields, `batch_date` not exactly 10 characters.
- `UNSUPPORTED_RANGE`: fields exceeding their COBOL `S9` picture capacity
  (category balance `S9(9)V99`, rate `S9(4)V99`, account amounts
  `S9(10)V99`), an `interest_cents` result ≥ 1e11, account totals ≥ 1e12,
  or more than 999999 emitted transactions.

**Business errors** (`BusinessRejection`, CLI exit 2) — the same classified
aborts the reference produces: `MISSING_ACCOUNT`, `MISSING_XREF`,
`MISSING_RATE` (missing account-specific *and* DEFAULT rate).

## Preserved EOF defect

`PRESERVE_EOF_DEFECT` is a module constant, not a CLI option. With it, the
largest processed `account_id` is never rewritten (transactions for it are
still emitted). A corrected mode would change observable output and needs a
separately reviewed contract, fixtures and evidence — see
`docs/MIGRATION-CONTRACT.md` and `reference/README.md`.

## Exit codes, output, retry

| Exit | Meaning |
|---|---|
| 0 | success; `error_kind: null`, full `accounts`/`transactions` arrays |
| 2 | classified business rejection; null arrays, `error_kind` + `detail` |
| 3 | technical input rejection; null arrays, `error_kind` + `detail` |
| 4 | unclassified failure (`UNCLASSIFIED_TARGET_ERROR`); traceback on stderr |

`run_job.py` writes JSON atomically (`<output>.tmp` then `os.replace`) and
writes a result on every classified path, so a stale success file can never
be mistaken for this run. `--explain` writes both formatted Spark plans to
`<output>.plan.txt`. Re-running with identical inputs produces byte-identical
JSON (tested).

## Databricks write/retry (prepared, not run)

`target/databricks/job_entry.py` reuses `validate_inputs` + `compute`.
Transactions append first (a `DELETE ... WHERE run_id = '<run_id>'` makes
re-runs idempotent), then the accounts snapshot is overwritten. A failure
between the two publishes nothing further; retry = same `run_id`. No atomic
cross-table commit is claimed. See `target/databricks/README.md`.

## Limits

Single `orderBy`/`row_number` window (single partition) — acceptable for this
bounded fixture domain, not for large volumes. Sequence exhaustion,
multi-partition determinism, real EBCDIC/VSAM inputs, and any input shape
outside the JSON fixture schema need separate reviewed expansion.
