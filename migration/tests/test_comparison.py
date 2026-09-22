"""Acceptance-tool checks: catch wrong money, missing records and false success."""
from copy import deepcopy
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.compare_target import canonical, compare, validate_cases


def sample():
    return {"case": "example", "status": "success", "error_kind": None,
            "accounts": [{"account_id": "002", "current_balance": "10.01"},
                         {"account_id": "001", "current_balance": "3.00"}],
            "transactions": [{"transaction_id": "T1", "amount": "0.01"}]}


def test_one_cent_difference_is_rejected():
    left = sample()
    right = deepcopy(left)
    right["accounts"][0]["current_balance"] = "10.02"
    assert compare(canonical(left), canonical(right))


def test_missing_transaction_is_rejected():
    left = sample()
    right = deepcopy(left)
    right["transactions"] = []
    assert compare(canonical(left), canonical(right))


def test_unmodified_account_field_difference_is_rejected():
    left = sample()
    right = deepcopy(left)
    right["accounts"][0]["active"] = "N"
    assert compare(canonical(left), canonical(right))


def test_record_order_is_not_business_difference():
    left = sample()
    right = deepcopy(left)
    right["accounts"].reverse()
    assert not compare(canonical(left), canonical(right))


def test_duplicate_business_key_is_rejected():
    result = sample()
    result["accounts"].append(deepcopy(result["accounts"][0]))
    with pytest.raises(ValueError, match="Duplicate"):
        canonical(result)


def test_arbitrary_crash_is_not_correct_rejection():
    with pytest.raises(ValueError, match="unclassified"):
        canonical({"case": "missing-account", "status": "error", "error_kind": "IMPORT_ERROR",
                   "accounts": None, "transactions": None})


def test_different_business_error_is_rejected():
    left = {"case": "missing-account", "status": "error", "error_kind": "MISSING_ACCOUNT",
            "accounts": None, "transactions": None}
    right = {**left, "error_kind": "MISSING_RATE"}
    assert compare(canonical(left), canonical(right))


def test_failure_cannot_publish_successful_business_output():
    with pytest.raises(ValueError, match="partial-write"):
        canonical({"case": "missing-account", "status": "error", "error_kind": "MISSING_ACCOUNT",
                   "accounts": [], "transactions": []})


@pytest.mark.parametrize("bad_record", [None, {}, {"account_id": ["001"]}])
def test_malformed_record_is_a_controlled_failure(bad_record):
    result = sample()
    result["accounts"] = [bad_record]
    with pytest.raises(ValueError, match="Every accounts record"):
        canonical(result)


def test_non_object_output_is_rejected():
    with pytest.raises(ValueError, match="JSON object"):
        canonical([])


@pytest.mark.parametrize("names", [[], ["a"], ["a", "a"]])
def test_empty_reduced_or_duplicate_case_sets_are_rejected(names):
    with pytest.raises(ValueError, match="exactly once"):
        validate_cases([{"name": n} for n in names], ["a", "b"])
