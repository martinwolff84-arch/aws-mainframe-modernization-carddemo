# Original COBOL reference

This harness calls the **unchanged** AWS CardDemo `CBACT04C` batch at commit
`59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`. It does not calculate interest in
Python and contains no PySpark migration.

## Run

From this preparation package's root (or `migration/` after unpacking it into a fork):

```sh
python reference/run_reference.py --all --output-dir artifacts/reference
python -m pytest tests/test_reference.py -q
```

Requirements: GnuCOBOL 3.2 with an indexed-file backend (Berkeley DB in the verified
macOS setup), C compiler, Python 3.11+, pytest. `COBC` can select an explicit compiler.
The runner compiles using `-std=ibm` and creates a fresh temporary indexed dataset
for every scenario. It never changes the source program or the copybooks.

`fixtures/cases.json` contains ten synthetic scenarios and manually derived
expected amounts. Frozen preparation exports are stored as `evidence/reference/<case>.json`; fresh runs go to `artifacts/reference/`.
Three scenarios deliberately fail. Their `status: error` and nonzero exit code are
expected findings, not broken setup.

## Adapter boundaries

- `driver.cbl` loads normalized fixture values into the original copybook layouts
  and indexed files, calls the original batch, then exports business fields.
- File mappings replace JCL/DD allocation. Berkeley DB is a local indexed-file
  backend, not a VSAM emulator and not evidence of an IBM mainframe deployment.
- `abend.cbl` supplies the unavailable IBM `CEE3ABD` routine as a logged exit 99.
  Error classes identify missing account, cross-reference, or default rate. This
  mapping is valid for these fixtures; unclassified failures must not pass parity.
- `COB_CURRENT_DATE` fixes the test clock to 22 September 2026 at 12:00 UTC;
  `REF_BATCH_DATE` controls the original ten-character transaction-ID prefix.
- Text export normalizes runtime LOW-VALUES padding in the transaction description
  to spaces. Numeric values are decimal strings; negative zero becomes `0.00`.
  Raw binary padding and storage representation are outside the comparison.
- All 12 named account fields and 13 named transaction fields are compared. The
  account filler and transaction filler are excluded. Cross-reference/category/rate
  files are input only. Accounts without categories remain in output unchanged.
- On expected aborts, canonical account/transaction outputs are `null`. The
  original may have partially written files before aborting. Recovery, atomicity,
  and partial-write equivalence are **not** demonstrated by this harness.

## Known source behavior to preserve

The original does not flush the final processed account after end of file. In the
baseline it emits a `100.00` interest transaction for account `00000000006`, but
leaves that account's balance at `900.00` and its cycle values at `8.00`/`9.00`.
The source was not fixed. A pure migration must match this behavior by default;
a correction needs a separately reviewed change and separate expected results.

The default comparator must not silently ignore this mismatch or compare only
transaction totals. Known-answer checks establish `155.00` total emitted interest
but only `55.00` of total account-balance increase in the baseline.

## What this evidence supports

It supports a reproducible local execution of one original COBOL batch using
synthetic fixtures. It does not establish production correctness, coverage of all
valid financial inputs, IBM compiler equivalence, whole-application migration,
Databricks execution, Delta transaction guarantees, scalability, cost savings, or
Allianz-system compatibility. Those require separate validation.
