# CardDemo: COBOL-to-PySpark migration starter

This package prepares one real migration task for Devin: the AWS CardDemo monthly
interest batch **CBACT04C**. Databricks is the intended platform; this preparation
uses local PySpark and no cloud account. It contains the executable legacy
reference, synthetic fixtures and acceptance tools. **The PySpark migration is
deliberately not implemented.** Devin will implement it in the session.

## Repository setup

Create your own fork of
https://github.com/aws-samples/aws-mainframe-modernization-carddemo .
The suggested name is `carddemo-databricks-migration`. Confirm the actual fork URL;
the preparation does not claim that a GitHub fork or branch already exists.

Start `demo-baseline` from upstream commit
`59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e`. Unpack this package's contents into a
new `migration/` directory in that fork. Preserve the original repository's `app/`
directory. Do not overwrite an existing baseline branch or an existing migration
directory without first inspecting its contents.

Commit preparation-only files to `demo-baseline`; the future migration PR should
target that branch **in your own fork**, not AWS. Generated environments, compiled
binaries and local caches stay out of Git. Keep the licence and upstream NOTICE.

## What is already prepared

- Unmodified upstream source snapshot and SHA-256 verification manifest.
- Executable full-batch reference with GnuCOBOL indexed files and a fixed clock.
- Ten synthetic scenarios: seven successful batches and three expected rejections.
- Independent known-answer reference tests, including the original EOF defect.
- Exact field comparison tooling for the future PySpark implementation.
- Local Spark environment smoke check, which is not a migrated business job.
- Scope, output contract, portability notes and later Databricks handover checklist.

Read `docs/MIGRATION-CONTRACT.md` and `reference/README.md` before implementation.
The observed original EOF issue is preserved in the first migration. A fix is a
separate business decision; do not silently change golden expectations.

## Environment

Validated preparation: macOS ARM64, Python 3.11.14, GnuCOBOL 3.2.0 with Berkeley DB,
Java 17.0.18, PySpark 3.5.6 and pytest 8.3.5. The full dependency pins are in
`requirements.txt`. Devin runs in a different environment: record actual versions
and rerun the checks, rather than citing supplied logs as a new result.

On a disposable Debian/Ubuntu session, `bash scripts/setup_linux.sh` is a setup
helper for compiler/JDK packages and an isolated Python environment. It requires
apt/sudo rights if packages are missing. Package-manager GnuCOBOL/Python versions
can differ from the validated macOS setup; inspect them and require the frozen
reference checks to pass. A build or baseline failure must be investigated before
claiming parity. This Linux bootstrap has not itself been executed in preparation.

With prerequisites installed:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
source .venv/bin/activate
java -version
cobc -V
python scripts/check_source.py
python -m pytest tests -q
python reference/run_reference.py --all --output-dir artifacts/reference
python scripts/spark_smoke.py
```

Point `JAVA_HOME` at Java17 if another Java version is selected. The smoke check
sets Spark's Python worker to the same interpreter as the driver. Local Spark
needs permission to open its local Python/Java communication ports.

`evidence/reference/` holds results actually produced during preparation. Fresh
runs should go to `artifacts/reference/` so the supplied evidence stays intact.
The three negative scenarios correctly produce `status: error`; their classified
rejections are successful test outcomes.

## The implementation and acceptance task

Devin creates `target/run_job.py`, real Spark transformations and meaningful tests,
following `docs/MIGRATION-CONTRACT.md`. After implementation:

```sh
python scripts/compare_target.py
```

Before implementation this exits with `NOT IMPLEMENTED`, intentionally. The
comparison reruns the actual COBOL program, supplies target inputs without the
known-answer block, and requires all business fields or classified errors to
match. It must detect monetary differences, missing records and wrong error types.
Devin must also test additional meaningful cases and repeatability. No GitHub CI,
Devin session, target parity or Databricks deployment has run in this preparation.

## What to show in the demo

1. The bounded business task and original COBOL rules.
2. The executable reference and the genuine final-account issue.
3. Devin's plan and the explicit decision to preserve source behaviour.
4. The actual migrated Spark code, successful comparisons and one meaningful test.
5. A reviewable PR, followed by the requirements for a real Databricks run.

The public AWS sample is not Allianz code. The demonstrated capability would be
moving a bounded batch while making preserved rules and differences inspectable.
No productivity, cost-saving, scalability or production-readiness result is implied.

## Attribution

Upstream AWS files: Apache-2.0, source repository and commit in
`upstream/manifest.json`; original NOTICE preserved. Preparation additions are
provided under Apache-2.0 as stated in `LICENSE`. The adapter, synthetic fixtures
and comparison tooling are preparation work; the target implementation must be
attributed to the tool/session that actually creates it.
