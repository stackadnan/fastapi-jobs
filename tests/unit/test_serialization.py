from __future__ import annotations

import pytest

from fastapi_jobs.exceptions import JobSerializationError
from fastapi_jobs.serialization import decode_payload, decode_result, encode_payload, encode_result


def test_round_trips_args_and_kwargs():
    payload = encode_payload("t", (1, "two", [3, 4]), {"a": True, "b": None})
    args, kwargs = decode_payload(payload)
    assert args == (1, "two", [3, 4])
    assert kwargs == {"a": True, "b": None}


def test_rejects_non_serializable_args():
    with pytest.raises(JobSerializationError, match="task 'send_email'"):
        encode_payload("send_email", (object(),), {})


def test_result_round_trip():
    encoded = encode_result("t", {"ok": True, "count": 3})
    assert decode_result(encoded) == {"ok": True, "count": 3}


def test_none_result_is_not_json_encoded():
    assert encode_result("t", None) is None
    assert decode_result(None) is None


def test_rejects_non_serializable_result():
    with pytest.raises(JobSerializationError, match="task 't'"):
        encode_result("t", object())
