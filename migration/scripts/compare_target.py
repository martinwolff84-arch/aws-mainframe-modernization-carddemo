"""Compare independently executed target output with real COBOL reference output.

Target CLI (to be implemented by Devin):
python target/run_job.py --case NAME --fixtures PATH --output PATH

One fixture per isolated job. The target writes canonical JSON and exits 0 for a
successful business case, nonzero for an expected rejected business case. The
comparison never accepts an arbitrary crash as a correct rejection.
"""
import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
sys.path.insert(0, str(ROOT))
from reference.run_reference import build_reference, load_cases, run_case


def canonical(result):
    if not isinstance(result, dict):
        raise ValueError("Result must be a JSON object")
    required = {"case", "status", "accounts", "transactions", "error_kind"}
    missing = required - result.keys()
    if missing:
        raise ValueError(f"Missing result fields: {sorted(missing)}")
    if not isinstance(result["case"], str):
        raise ValueError("Case identifier must be a string")
    if result["status"] not in ("success", "error"):
        raise ValueError("Unknown status")
    if result["status"] == "error":
        if result["error_kind"] not in ("MISSING_ACCOUNT", "MISSING_XREF", "MISSING_RATE"):
            raise ValueError("Unknown or unclassified failure is not an accepted rejection")
        if result["accounts"] is not None or result["transactions"] is not None:
            raise ValueError("Error outputs must be null; partial-write parity is outside this demo")
    else:
        if result["error_kind"] is not None:
            raise ValueError("Success must not carry an error")
        for name, key in (("accounts", "account_id"), ("transactions", "transaction_id")):
            records = result[name]
            if not isinstance(records, list):
                raise ValueError(f"{name} must be a list")
            if any(not isinstance(r, dict) or not isinstance(r.get(key), str) for r in records):
                raise ValueError(f"Every {name} record must have a string {key}")
            keys = [r[key] for r in records]
            if len(set(keys)) != len(keys):
                raise ValueError(f"Duplicate {name} keys")
    return {k: sorted(result[k], key=lambda r: r["account_id" if k == "accounts" else "transaction_id"])
            if isinstance(result[k], list) else result[k] for k in sorted(required)}


def compare(left, right, path=""):
    """Return a bounded exact diff; decimal strings have no float tolerance."""
    differences = []
    if type(left) is not type(right):
        return [{"path": path, "expected": left, "actual": right}]
    if isinstance(left, dict):
        for key in sorted(left.keys() | right.keys()):
            child = f"{path}.{key}" if path else key
            if key not in left or key not in right:
                differences.append({"path": child, "expected": left.get(key, "<missing>"), "actual": right.get(key, "<missing>")})
            else:
                differences.extend(compare(left[key], right[key], child))
    elif isinstance(left, list):
        if len(left) != len(right):
            differences.append({"path": path + ".length", "expected": len(left), "actual": len(right)})
        for i, (a, b) in enumerate(zip(left, right)):
            differences.extend(compare(a, b, f"{path}[{i}]"))
    elif left != right:
        differences.append({"path": path, "expected": left, "actual": right})
    return differences


def validate_cases(cases, expected_names):
    names = [c.get("name") for c in cases]
    if not names or len(names) != len(expected_names) or sorted(names) != sorted(expected_names):
        raise ValueError("All frozen acceptance cases must be present exactly once")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=Path, default=ROOT / "target/run_job.py")
    parser.add_argument("--report", type=Path, default=ROOT / "artifacts/parity-report.json")
    args = parser.parse_args()
    # A failed new attempt must not leave a previous green report behind.
    args.report.unlink(missing_ok=True)
    if not args.target.is_file():
        raise SystemExit("NOT IMPLEMENTED: Devin must create target/run_job.py. No migration result is being claimed.")
    subprocess.run([sys.executable, str(ROOT / "scripts/check_source.py")], check=True)
    executable = build_reference()
    cases = load_cases()
    acceptance = json.loads((ROOT / "evidence/acceptance-manifest.json").read_text())
    validate_cases(cases, acceptance["cases"])
    rows = []
    with tempfile.TemporaryDirectory(prefix="carddemo-parity-") as temp:
        for case in cases:
            expected = canonical(run_case(case, executable))
            output = Path(temp) / f"{case['name']}.json"
            # The expected checks in the prep fixture file are not passed to the target.
            clean_fixture = Path(temp) / f"{case['name']}-inputs.json"
            clean_fixture.write_text(json.dumps({"cases": [{k: v for k, v in case.items() if k != "expected"}]}))
            try:
                process = subprocess.run([sys.executable, str(args.target.resolve()), "--case", case["name"],
                                          "--fixtures", str(clean_fixture), "--output", str(output)],
                                         cwd=ROOT, capture_output=True, text=True, timeout=180)
                if not output.exists():
                    raise ValueError(f"No target JSON (exit {process.returncode}): {process.stderr[-1000:]}")
                actual = canonical(json.loads(output.read_text()))
                if (actual["status"] == "success") != (process.returncode == 0):
                    raise ValueError("Target process exit contradicts target JSON status")
                differences = compare(expected, actual)
                rows.append({"case": case["name"], "passed": not differences,
                             "account_count": len(expected["accounts"] or []),
                             "transaction_count": len(expected["transactions"] or []),
                             "differences": differences})
            except (ValueError, OSError, subprocess.TimeoutExpired) as exc:
                rows.append({"case": case["name"], "passed": False, "error": str(exc)})
    report = {"scope": "target output parity with unchanged COBOL under GnuCOBOL",
              "spark_execution_verified_by_this_comparator": False,
              "additional_gate": "Review real Spark transformations and executed Spark plan/logs; no Databricks or z/OS certification",
              "total": len(rows), "passed": sum(r["passed"] for r in rows), "results": rows}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{report['passed']}/{report['total']} cases match; report: {args.report}")
    raise SystemExit(0 if report["passed"] == report["total"] else 1)


if __name__ == "__main__":
    main()
