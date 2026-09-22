"""Independent known-answer checks for the executable original COBOL baseline."""
from decimal import Decimal

import pytest

from reference.run_reference import build_reference, load_cases, run_case


@pytest.fixture(scope="session")
def executable(tmp_path_factory):
    return build_reference(tmp_path_factory.mktemp("reference-build"))


@pytest.mark.parametrize("case", load_cases(), ids=lambda c: c["name"])
def test_original_matches_independent_known_answers(case, executable):
    result = run_case(case, executable)
    expected = case["expected"]
    assert result["status"] == expected["status"], result
    if expected["status"] == "error":
        assert result["return_code"] == 99
        assert expected["message"] in result["stdout"]
        assert result["error_kind"] == {
            "missing_account": "MISSING_ACCOUNT",
            "missing_xref": "MISSING_XREF",
            "missing_default": "MISSING_RATE",
        }[case["name"]]
        assert result["accounts"] is None
        assert result["transactions"] is None
        return
    assert result["return_code"] == 0
    assert result["error_kind"] is None
    assert result["stderr"] == ""
    accounts, transactions = result["accounts"], result["transactions"]
    assert [a["account_id"] for a in accounts] == sorted(a["account_id"] for a in case["accounts"])
    assert [a["current_balance"] for a in accounts] == expected["balances"]
    assert [a["cycle_credit"] for a in accounts] == expected["cycle_credit"]
    assert [a["cycle_debit"] for a in accounts] == expected["cycle_debit"]
    assert [t["amount"] for t in transactions] == expected["transaction_amounts"]
    inputs = {a["account_id"]: a for a in case["accounts"]}
    for account in accounts:
        before = inputs[account["account_id"]]
        assert account["group"] == before["group"]
        assert account["active"] == before.get("active", "Y")
        assert account["credit_limit"] == "10000.00"
        assert account["cash_credit_limit"] == "5000.00"
        assert account["open_date"] == "2020-01-01"
        assert account["expiration_date"] == "2030-01-01"
        assert account["reissue_date"] == "2025-01-01"
        assert account["zip"] == "12345"
    xrefs = {x["card_number"]: x["account_id"] for x in case["xrefs"]}
    for sequence, transaction in enumerate(transactions, 1):
        assert transaction["transaction_id"] == case["batch_date"] + f"{sequence:06d}"
        assert transaction["type"] == "01"
        assert transaction["category"] == "0005"
        assert transaction["source"] == "System"
        assert transaction["description"] == "Int. for a/c " + xrefs[transaction["card_number"]]
        assert transaction["original_timestamp"] == "2026-09-22-12.00.00.000000"
        assert transaction["processing_timestamp"] == transaction["original_timestamp"]
        assert transaction["merchant_id"] == "000000000"
        assert transaction["merchant_name"] == transaction["merchant_city"] == transaction["merchant_zip"] == ""


def test_known_eof_defect_is_not_silently_repaired(executable):
    case = load_cases()[0]
    result = run_case(case, executable)
    before = sum(Decimal(a["current_balance"]) for a in case["accounts"])
    after = sum(Decimal(a["current_balance"]) for a in result["accounts"])
    emitted = sum(Decimal(t["amount"]) for t in result["transactions"])
    assert emitted == Decimal("155.00")
    assert after - before == Decimal("55.00")
    assert emitted - (after - before) == Decimal("100.00")


def test_fresh_runs_are_reproducible(executable):
    case = load_cases()[0]
    first, second = run_case(case, executable), run_case(case, executable)
    assert first == second
