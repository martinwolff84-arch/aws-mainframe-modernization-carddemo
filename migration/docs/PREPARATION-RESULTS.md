# Preparation results — 22 September 2026

This records preparation work performed before the Devin migration session. It is
not a Devin result, target-parity result, CI run or Databricks deployment.

## Observed checks

- Upstream commit: `59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`.
- Nine upstream source/licence files pass SHA-256 verification.
- The complete original CBACT04C source and copybooks compile without modification.
- Ten synthetic reference scenarios executed: seven normal batches, three expected
  classified aborts. Captured output is in `evidence/reference/`.
- `python -m pytest tests -q`: **27 passed**, comprising 12 reference checks and
  15 acceptance-tool checks. This is not percentage coverage or migration acceptance.
- `python scripts/spark_smoke.py`: **passed** on Python 3.11.14, Java 17.0.18 and
  PySpark 3.5.6. The smoke calculation adds exact decimal values; it does not migrate
  the business calculation. Evidence: `evidence/spark-smoke.json`.
- GnuCOBOL 3.2.0 with Berkeley DB on macOS ARM64 was used for preparation. The Linux
  setup helper has been syntax-checked but not executed in a Devin environment.
- Target code is intentionally absent. The comparator reports `NOT IMPLEMENTED`.

## Findings and fixes in the preparation

**Original-source defect, reproduced and preserved:** the last processed account
is not updated at EOF. In the baseline, it produces 100.00 interest but retains
balance 900.00 and cycle credit/debit 8.00/9.00. Source correction is a separate
reviewed change. See `docs/BUSINESS-RULES.md`.

**Spark environment issue, fixed:** Spark workers initially selected a different
Python version from the driver. The smoke and comparison launchers now explicitly
select the current interpreter for driver and workers. The subsequent smoke passed.

**Acceptance-tool issues, fixed after independent review:** an empty/reduced set
must not become a successful 0/0 report; all frozen case identities are mandatory.
Malformed nested outputs become controlled failures, and a new attempt removes
any previous report before checking results. The report describes output parity;
it does not claim to prove Spark execution. Code review and actual Spark plan/log
evidence are a separate acceptance gate. Regression checks cover malformed data,
reduced case sets, monetary differences, missing records and incorrect error types.

## Acceptance integrity and remaining work

`evidence/acceptance-manifest.json` pins 18 preparation files, including fixtures,
reference outputs, adapters, existing tests and comparison tooling. These hashes
detect accidental changes. Review the manifest itself and these files against the
actual preparation commit; hashes inside the repository are not a security boundary.

Devin still needs to create the target, run comparisons, add meaningful tests and
deliver a reviewable PR. No authenticated GitHub fork creation was confirmed;
browser policy verification blocked the attempt. No Devin session, target job,
GitHub CI or Databricks run occurred during preparation.

Production data, IBM runtime equivalence, EBCDIC/VSAM ingestion, partial-write
recovery, scale and operational integration remain outside this finite local demo.
