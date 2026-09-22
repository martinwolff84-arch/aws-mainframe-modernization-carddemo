"""Compile and run unchanged AWS CBACT04C against synthetic indexed files.

This is an I/O/runtime adapter, not a reimplementation of interest logic.
Amounts are supplied and returned as decimal strings; Python does not calculate
the migrated business result. Failure runs retain their log, not a success payload.
"""
from __future__ import annotations

import argparse
import json
import os
from decimal import Decimal
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_COMMIT = "59cc6c2fd7ebd7ef7925cad552a01a4b8b6e4d5e"


def load_cases():
    return json.loads((ROOT / "fixtures/cases.json").read_text())["cases"]


def build_reference(build_dir=None):
    compiler = os.environ.get("COBC") or shutil.which("cobc")
    if not compiler:
        raise RuntimeError("Install GnuCOBOL with indexed-file support (cobc).")
    build_dir = Path(build_dir or ROOT / ".build").resolve()
    build_dir.mkdir(parents=True, exist_ok=True)
    executable = build_dir / "carddemo-reference"
    sources = [ROOT / "reference/driver.cbl", ROOT / "upstream/app/cbl/CBACT04C.cbl",
               ROOT / "reference/abend.cbl"]
    command = [compiler, "-x", "-std=ibm", "-Wno-others", "-I",
               str(ROOT / "upstream/app/cpy"), "-o", str(executable)]
    result = subprocess.run(command + [str(p) for p in sources], capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"Reference compile failed:\n{result.stdout}\n{result.stderr}")
    return executable


def _money(value):
    amount = Decimal(value)
    return "0.00" if amount == 0 else format(amount, ".2f")


def _write_lines(path, rows):
    # This format is private to the adapter; fixture fields cannot contain pipes/newlines.
    for row in rows:
        if any(any(ch in str(v) for ch in "|\r\n") for v in row):
            raise ValueError("Fixture contains an invalid adapter delimiter")
    path.write_text("".join("|".join(map(str, row)) + "\n" for row in rows))


def _accounts(path):
    result = []
    for line in path.read_text().splitlines():
        v = line.split("|")
        if len(v) != 12:
            raise ValueError(f"Malformed account export: {line!r}")
        result.append(dict(account_id=v[0], group=v[1], current_balance=_money(v[2]),
                           cycle_credit=_money(v[3]), cycle_debit=_money(v[4]), active=v[5],
                           credit_limit=_money(v[6]), cash_credit_limit=_money(v[7]),
                           open_date=v[8], expiration_date=v[9], reissue_date=v[10], zip=v[11]))
    return result


def _transactions(path):
    result = []
    for line in path.read_text().splitlines():
        v = line.split("|")
        if len(v) != 13:
            raise ValueError(f"Malformed transaction export: {line!r}")
        result.append(dict(transaction_id=v[0], type=v[1], category=v[2], source=v[3],
                           description=v[4], amount=_money(v[5]), card_number=v[6],
                           original_timestamp=v[7], processing_timestamp=v[8], merchant_id=v[9],
                           merchant_name=v[10], merchant_city=v[11], merchant_zip=v[12]))
    return result


def run_case(case, executable=None, work_dir=None):
    executable = Path(executable or build_reference()).resolve()
    if work_dir is None:
        with tempfile.TemporaryDirectory(prefix="carddemo-reference-") as temporary:
            return run_case(case, executable, temporary)
    work = Path(work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env.update(REF_BATCH_DATE=case.get("batch_date", "2026092200"),
               COB_CURRENT_DATE="2026/09/22 12:00:00.00+00:00")
    if len(env["REF_BATCH_DATE"]) != 10:
        raise ValueError("batch_date must have exactly 10 characters")
    for dd in ("ACCTFILE", "XREFFILE", "TCATBALF", "DISCGRP", "TRANSACT"):
        env[f"DD_{dd}"] = str(work / dd)
    rows = {
        "ACCOUNTS": [[a["account_id"], a["group"], a["current_balance"],
                      a["cycle_credit"], a["cycle_debit"], a.get("active", "Y")]
                     for a in case["accounts"]],
        "XREFS": [[x["card_number"], x["customer_id"], x["account_id"]] for x in case["xrefs"]],
        "CATEGORIES": [[c["account_id"], c["type"], c["category"], c["balance"]]
                       for c in case["categories"]],
        "RATES": [[r["group"], r["type"], r["category"], r["rate"]] for r in case["rates"]],
    }
    for name, values in rows.items():
        input_path = work / f"{name.lower()}.txt"
        _write_lines(input_path, values)
        env[f"REF_{name}_INPUT"] = str(input_path)
    account_output, transaction_output = work / "accounts.out", work / "transactions.out"
    env.update(REF_ACCOUNTS_OUTPUT=str(account_output), REF_TRANSACTIONS_OUTPUT=str(transaction_output))
    seeded = subprocess.run([str(executable), "seed"], cwd=work, env=env, capture_output=True, text=True, timeout=60)
    if seeded.returncode:
        raise RuntimeError(f"Reference seed failed: {seeded.stdout}\n{seeded.stderr}")
    executed = subprocess.run([str(executable), "run"], cwd=work, env=env, capture_output=True, text=True, timeout=60)
    error_kind = None
    if executed.returncode:
        errors = {"ERROR READING ACCOUNT FILE": "MISSING_ACCOUNT",
                  "ERROR READING XREF FILE": "MISSING_XREF",
                  "ERROR READING DEFAULT DISCLOSURE GROUP": "MISSING_RATE"}
        error_kind = next((kind for message, kind in errors.items() if message in executed.stdout),
                          "UNCLASSIFIED_REFERENCE_ERROR")
    result = dict(case=case["name"], upstream_commit=UPSTREAM_COMMIT,
                  status="success" if executed.returncode == 0 else "error",
                  error_kind=error_kind,
                  return_code=executed.returncode, stdout=executed.stdout, stderr=executed.stderr,
                  accounts=None, transactions=None)
    if executed.returncode == 0:
        exported = subprocess.run([str(executable), "export"], cwd=work, env=env,
                                  capture_output=True, text=True, timeout=60)
        if exported.returncode:
            raise RuntimeError(f"Reference export failed: {exported.stdout}\n{exported.stderr}")
        result["accounts"] = _accounts(account_output)
        result["transactions"] = _transactions(transaction_output)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", default="baseline", help="Name from fixtures/cases.json")
    parser.add_argument("--all", action="store_true", help="Run every supplied case")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "evidence/reference")
    args = parser.parse_args()
    cases = [c for c in load_cases() if args.all or c["name"] == args.case]
    if not cases:
        parser.error(f"Unknown case: {args.case}")
    executable = build_reference()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for case in cases:
        result = run_case(case, executable)
        destination = args.output_dir / f"{case['name']}.json"
        destination.write_text(json.dumps(result, indent=2) + "\n")
        print(f"{case['name']}: {result['status']} (exit {result['return_code']}) -> {destination}")


if __name__ == "__main__":
    main()
