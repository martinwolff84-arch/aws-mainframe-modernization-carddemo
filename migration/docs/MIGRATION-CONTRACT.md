# Bounded migration contract

## The work Devin will deliver

Implement the business behaviour of AWS CardDemo CBACT04C as a local PySpark job,
with an explicit later Databricks handover. This is an implementation task, not
only documentation or test generation. Keep the reference independent.

The source snapshot is byte-for-byte upstream commit
`59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`. `scripts/check_source.py` verifies it.
The reference executes that unchanged program through a documented GnuCOBOL
adapter. GnuCOBOL behaviour is not an IBM z/OS/Enterprise COBOL certification.

## Input and output boundary

The initial supported input is the JSON schema illustrated by `fixtures/cases.json`:
accounts, account/card cross-references, category balances, rate tables and a batch
date. Currency amounts and annual percentage rates are decimal strings. Source
identifiers retain leading zeroes. The source indexed-file ordering is account,
transaction type and category, regardless of input-array order.

Create `target/run_job.py` with this command-line contract:

```sh
python target/run_job.py --case baseline --fixtures fixtures/cases.json --output artifacts/target-baseline.json
```

Write a canonical result containing `case`, `status`, `error_kind`, `accounts` and
`transactions`. Success: exit0, `error_kind: null`, all output records. Expected
business rejection: nonzero exit, typed error and null output arrays. Exact account
and transaction field names/types are in `reference/run_reference.py` and the
generated examples under `evidence/reference/`. Include unchanged account fields
and all documented transaction fields, not only monetary totals. Additional
technical metadata is allowed. Use the controlled reference timestamp in tests.

The acceptance tool passes a temporary fixture containing only inputs (no
`expected` block). Do not consume prep expected values, execute COBOL, or copy
reference outputs from the target implementation. Use real Spark DataFrame/SQL
transformations for the business calculation, not Python loops collecting the
entire data set onto the driver.

## Semantics to preserve

- Account-specific rate lookup, DEFAULT only when the specific record is absent.
- A specific zero rate is valid and suppresses a transaction; a zero balance with
  nonzero rate still produces a zero-amount transaction.
- Per-category interest = balance × annual percentage rate /1200, truncated to
  cents as observed in the source before aggregation. Preserve signed values.
- Missing account, cross-reference or default rate is a classified business error.
- Deterministic transaction sequence, source transaction fields, account totals,
  credit/debit resets and unchanged fields within the supported cases.
- Existing account status does not act as a new eligibility filter.

**Known original defect:** the final processed account is not flushed at EOF.
The first migration preserves this documented behaviour for parity. In the
baseline fixture account `00000000006` produces a `100.00` transaction but remains
at balance `900.00`, credit `8.00`, debit `9.00`. Do not silently fix it or call it
a migration defect. A corrected business mode would require a separately reviewed
contract, separate expectations and separate evidence.

## Acceptance

1. Reproduce the source hash check, reference tests and Spark smoke check.
2. Preserve the frozen source, fixtures and existing expected results. Record
   justified apparatus changes separately, rather than weakening assertions.
3. Run `python scripts/compare_target.py` after the target exists: all supplied
   cases must match exact business fields or exact classified rejection. No
   floating-point tolerance for amounts. Include extra meaningful tests too.
4. Repeat target execution with identical inputs/parameters and demonstrate
   identical canonical results and the proposed local write/retry behaviour.
5. Separately inspect the target transformations and capture an executed Spark
   plan/log. JSON output parity alone cannot establish which engine calculated it.
6. Deliver a reviewable PR into `demo-baseline` in the user's fork, with actual
   test counts, commits, command logs and an explicit Databricks status.

The comparison script is a starting acceptance harness. Passing a finite fixture
set is evidence within that scope, not proof for arbitrary inputs or workload scale.
In the baseline package target implementation is intentionally absent.

## Boundaries

No production system, real customer data, mainframe migration, EBCDIC ingestion,
VSAM storage migration, CICS, full JCL orchestration or actual Databricks execution
is claimed. The harness exports comparable logical records and normalizes string
padding as described in `reference/README.md`. Partial writes after failed source
batches are not compared; the target should not publish success output on failure.
Numeric overflow, malformed records, duplicate keys, sequence exhaustion and large
volume behaviour need an explicit policy/test expansion before broader use.

## Frozen acceptance baseline

`evidence/acceptance-manifest.json` pins fixtures, reference adapter, supplied
reference results and acceptance tooling. The source check verifies these hashes.
A manifest is an integrity aid, not a security boundary: a reviewer must also
confirm these files and the manifest are unchanged against the actual
`demo-baseline` commit. Additional target tests go in new files. Any necessary
apparatus change requires a separate explanation and review. All ten original
case identities are mandatory; zero or reduced cases cannot count as a pass.
