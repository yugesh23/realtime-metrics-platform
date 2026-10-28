"""
Unit tests for Wikimedia event parsing and validation in ingestor.py.
"""

from datetime import datetime, timezone
import pytest
from ingestor import parse_event


def test_parse_valid_event():
    raw_event = {
        "meta": {"id": "12345678-abcd"},
        "timestamp": 1730100000,
        "wiki": "enwiki",
        "type": "edit",
        "bot": False,
        "user": "Editor123",
        "title": "Artificial Intelligence",
        "length": {"old": 1500, "new": 1750},
        "comment": "Added reference section",
    }
    result = parse_event(raw_event)
    assert result is not None
    assert result[0] == "12345678-abcd"
    assert result[1] == "enwiki"
    assert result[2] == "edit"
    assert result[3] is False
    assert result[4] == "Editor123"
    assert result[5] == "Artificial Intelligence"
    assert result[6] == datetime.fromtimestamp(1730100000, tz=timezone.utc)
    assert result[7] == 1500
    assert result[8] == 1750
    assert result[9] == "Added reference section"


def test_parse_event_missing_required_fields():
    # Missing meta.id
    assert parse_event({"timestamp": 1730100000, "wiki": "enwiki"}) is None

    # Missing timestamp
    assert parse_event({"meta": {"id": "abc"}, "wiki": "enwiki"}) is None

    # Missing or empty wiki
    assert parse_event({"meta": {"id": "abc"}, "timestamp": 1730100000, "wiki": ""}) is None

    # Non-dict input
    assert parse_event("string_not_dict") is None
    assert parse_event(None) is None


def test_parse_event_invalid_timestamp():
    raw_event = {
        "meta": {"id": "abc"},
        "timestamp": "not_a_valid_timestamp",
        "wiki": "enwiki",
    }
    assert parse_event(raw_event) is None


def test_parse_event_bot_flag_and_missing_lengths():
    raw_event = {
        "meta": {"id": "bot-event-1"},
        "timestamp": 1730100050,
        "wiki": "wikidatawiki",
        "bot": True,
        # No length subfield
    }
    result = parse_event(raw_event)
    assert result is not None
    assert result[0] == "bot-event-1"
    assert result[1] == "wikidatawiki"
    assert result[3] is True  # is_bot
    assert result[7] is None  # len_old
    assert result[8] is None  # len_new


def test_parse_event_string_truncation():
    raw_event = {
        "meta": {"id": "trunc-test"},
        "timestamp": 1730100050,
        "wiki": "enwiki",
        "user": "U" * 500,
        "title": "T" * 1000,
        "comment": "C" * 2000,
    }
    result = parse_event(raw_event)
    assert result is not None
    assert len(result[4]) <= 255
    assert len(result[5]) <= 500
    assert len(result[9]) <= 1000
