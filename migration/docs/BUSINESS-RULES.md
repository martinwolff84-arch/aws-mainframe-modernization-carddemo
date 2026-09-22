# CBACT04C — migration review and test contract

Reviewed 22 September 2026. Upstream commit:
`59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`.

## Recommended bounded scope

Migrate the observable business behavior of **one interest-calculation batch** into
PySpark, with an eventual Databricks deployment path. Keep the complete public
CardDemo application outside scope. No production records, CICS screens, whole
mainframe retirement, fees implementation, or operational cutover are involved.

The preparation package executes the original batch without editing it. GnuCOBOL
and Berkeley DB replace the unavailable compiler/indexed-file runtime; a small
driver and abort stub replace JCL invocation and the IBM abort routine. This is a
local reference, not a mainframe certification. The PySpark implementation remains
Devin's task.

## Rules and migration traps

| Rule | Review implication |
|---|---|
| The category key comprises account, transaction type, and category. | Explicit deterministic key ordering is necessary; input row order is not authoritative. |
| Look up an account and its cross-reference before interest evaluation. | Do not silently remove unmatched rows with inner joins. |
| Try the account group rate, then `DEFAULT` for the same type/category if missing. | A present zero rate is authoritative; it must not trigger fallback. |
| Monthly amount is category balance × annual percentage rate ÷ 1200. | Preserve signed decimals and truncate each category to cents before summing. |
| A zero rate emits no transaction. | A nonzero rate on a zero balance still emits a zero-amount transaction. |
| Account balances and cycle fields are rewritten at the next account boundary. | Preserve the final-account defect in the initial parity contract. |
| Fees are unimplemented; account-active status is not checked. | Do not invent either feature during migration. |

The source sections relevant to this review are the main loop and paragraphs
`1050`, `1100`, `1110`, `1200`, `1200-A`, `1300`, `1300-B`, and `1400`.
[Pinned original program](https://github.com/aws-samples/aws-mainframe-modernization-carddemo/blob/59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e/app/cbl/CBACT04C.cbl).

COBOL truncates excess fractional digits when `ROUNDED` is absent. Negative
amounts need truncation toward zero, not floor. A Spark conversion must avoid
binary floating point and accidental rounding when reducing decimal scale.
[IBM arithmetic semantics](https://www.ibm.com/docs/en/cobol-zos/6.3?topic=operations-rounded-phrase).

## Reproduced final-account defect

`PERFORM UNTIL` defaults to testing the termination condition before entry.
The branch that would flush when EOF is already true therefore never executes;
once reading sets EOF, the loop exits before that branch can run.
[IBM loop semantics](https://www.ibm.com/docs/en/cobol-aix/5.1.0?topic=statement-perform-until-phrase).

This was **executed and reproduced**, not merely inferred. In the baseline:

- Ten transactions sum to `155.00`.
- Account balances increase by only `55.00`.
- The last processed account emits `100.00` interest, but its stored balance remains
  `900.00`, cycle credit `8.00`, and cycle debit `9.00`.
- With only one processed account, no account is rewritten at all.

The default migration must match the original, including this defect. Do not make
the migrated result look correct by comparing only transactions. Any final-flush
correction should be a separately approved change, with separate expected results
and a clear distinction between equivalence and improvement.

## Independent arithmetic anchors

These expected results were calculated separately from the executable reference
and are fixed in `fixtures/cases.json`; Python in the harness does not
calculate business outputs.

| Balance | Annual rate | Exact monthly result | Stored amount |
|---:|---:|---:|---:|
| 1200.00 | 12.00 | 12 | 12.00 |
| 100.50 | 12.00 | 1.005 | 1.00 |
| 100.99 | 12.00 | 1.0099 | 1.00 |
| −100.50 | 12.00 | −1.005 | −1.00 |
| 100.50 | −12.00 | −1.005 | −1.00 |
| −100.50 | −12.00 | 1.005 | 1.00 |
| 12345.67 | 9.99 | 102.77770275 | 102.77 |
| −12345.67 | 9.99 | −102.77770275 | −102.77 |
| 0.50, twice in separate categories | 12.00 | 0.005 each | 0.00 + 0.00 = 0.00 |

The final row distinguishes truncating each category first from summing raw
interest then truncating, which would incorrectly produce `0.01`.

## Baseline fixture, account by account

All rows are synthetic. Account IDs below are zero-padded to eleven digits.

| Account | Purpose | Emitted interest | Original stored balance after run |
|---|---|---:|---:|
| 1 | Exact amount plus positive fractions | 12.00 + 1.00 + 1.00 | 1000.00 → 1014.00 |
| 2 | Missing group, two type-specific default rates (18% and 24%) | 18.00 + 24.00 | 2000.00 → 2042.00 |
| 3 | Explicit zero rate despite nonzero default | No transaction | 500.00 → 500.00; cycles reset |
| 4 | Zero balance and negative balance | 0.00 − 1.00 | 100.00 → 99.00 |
| 5 | Two fractional-cent categories | 0.00 + 0.00 | 50.00 → 50.00; cycles reset |
| 6 | Final processed account / EOF defect | 100.00 | 900.00 → 900.00; cycles unchanged |
| 7 | Account with no category records | No transaction | 700.00 → 700.00; cycles unchanged |

Additional fixtures cover empty input, a single processed account, negative rates,
fractional rates, inactive accounts, reversed input order, missing account,
missing cross-reference, and missing default rate.

## Acceptance and explicit limits

1. Execute the original for fresh evidence, then run the independently implemented
   Spark target against the same fixture inputs. Target code must not read expected
   results, call the reference executable, or copy reference output files.
2. Compare every normalized account and transaction business field, stable ordering,
   monetary values, and typed failures. Seeded control date/time is deterministic.
3. Error cases must report the appropriate failure class. Partial-write recovery
   semantics are outside this comparison; no atomicity claim is justified.
4. Preserve uniqueness of indexed keys in the target data contract. Duplicate
   tabular join keys could multiply transactions; reject them or define a separately
   reviewed policy rather than silently choosing a row.
5. Overflow, malformed numeric bytes, six-digit sequence exhaustion, massive
   volumes, EBCDIC decoding, rerun idempotence and operational recovery require
   separate work. The supplied fixture domain is intentionally bounded.
6. Local PySpark parity is not a Databricks run. Deployment configuration may be
   prepared, but workspace execution, permissions, Delta persistence, performance
   and cost require later workspace evidence.

Verification at preparation time: **12 reference tests passed**, covering ten
scenarios, seven successful executions and three expected aborts. This is a test
count, not a percentage-coverage claim or proof of production completeness.
