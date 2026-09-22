# Target execution evidence — PySpark CBACT04C migration

**Databricks execution: NOT RUN.** All evidence below is local PySpark 3.5.6
execution on Linux, captured 22 September 2026.

## Environment

| Tool | Version |
|---|---|
| Python (venv) | 3.11.0rc1 (Ubuntu 22.04 `python3.11`) |
| Java | OpenJDK 17.0.20.1 |
| Spark / PySpark | 3.5.6 |
| GnuCOBOL (reference only) | 3.1.2.0 |
| Upstream source | `59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e` |

## Contents

| File | Produced by |
|---|---|
| `parity-report.json` | `python scripts/compare_target.py` — **10/10 cases match** the unchanged COBOL reference field-for-field (7 success cases, 3 classified rejections) |
| `case-<name>.json` (10 files) | `python target/run_job.py --case <name> --fixtures fixtures/cases.json --output evidence/target/case-<name>.json` — 7 runs exit 0, `missing_account` / `missing_xref` / `missing_default` exit 2 with `error_kind` set and null arrays |
| `target-baseline.json` | `python target/run_job.py --case baseline --fixtures fixtures/cases.json --output evidence/target/target-baseline.json --explain` (exit 0; accounts/transactions equal `evidence/reference/baseline.json` exactly) |
| `target-baseline.json.plan.txt` | `--explain` output: formatted Spark plans for `accounts_out` and `transactions_out` |
| `target-baseline-rerun.json` | second identical `run_job.py` invocation |
| `pytest-full.txt` | `python -m pytest tests -q` — **94 passed** (27 preexisting + 67 target) |
| `spark-log.txt` | `run_job.py --case baseline --log-level INFO`, stderr filtered to `DAGScheduler`/`ResultStage`/`Job … finished`/`SparkContext` lines (first 200) — evidence the calculation executed on the Spark engine, including shuffle exchanges |

## Repeatability

`target-baseline.json` and `target-baseline-rerun.json` are **byte-identical**:

```
5116e740eb91abc72978a145e1c10d6a0d29ecb874fd6843463ea4dd043039f4  target-baseline.json
5116e740eb91abc72978a145e1c10d6a0d29ecb874fd6843463ea4dd043039f4  target-baseline-rerun.json
```

## Scope notes

Parity is established over the 10 supplied synthetic cases only — see
`docs/MIGRATION-CONTRACT.md` and `reference/README.md` for what this does and
does not demonstrate. The preserved EOF defect is intentional behaviour, not a
mismatch.
