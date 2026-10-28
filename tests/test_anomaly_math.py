"""
Unit tests for statistical anomaly calculations, zero-stddev handling,
sample count thresholds, and severity classification.
"""

import math
import pytest


def calculate_zscore(observed, mean, stddev):
    """Pure Python implementation of the anomaly scoring math for validation."""
    if stddev is None or stddev <= 0:
        return None
    return (observed - mean) / stddev


def classify_severity(zscore):
    """Classify anomaly severity based on zscore absolute value."""
    if zscore is None:
        return None
    abs_z = abs(zscore)
    if abs_z >= 6.0:
        return "critical"
    elif abs_z >= 4.0:
        return "high"
    elif abs_z >= 3.0:
        return "medium"
    return "low"


def test_zscore_calculation_normal():
    # Observed 100, historical mean 50, stddev 10 -> z = +5.0
    z = calculate_zscore(100, 50, 10)
    assert z == 5.0
    assert classify_severity(z) == "high"


def test_zscore_zero_or_negative_stddev():
    # Protected against division by zero
    assert calculate_zscore(100, 50, 0) is None
    assert calculate_zscore(100, 50, -2.5) is None
    assert calculate_zscore(100, 50, None) is None


def test_severity_levels():
    assert classify_severity(2.9) == "low"
    assert classify_severity(3.0) == "medium"
    assert classify_severity(3.99) == "medium"
    assert classify_severity(4.0) == "high"
    assert classify_severity(5.99) == "high"
    assert classify_severity(6.0) == "critical"
    assert classify_severity(10.5) == "critical"
    assert classify_severity(-7.2) == "critical"


def test_insufficient_sample_rejection():
    # Simulate criteria: sample_count >= 10 and edits >= 5
    def should_alert(sample_count, edits, zscore, min_sample=10, min_edits=5, z_threshold=3.0):
        if sample_count < min_sample:
            return False
        if edits < min_edits:
            return False
        if zscore is None or abs(zscore) < z_threshold:
            return False
        return True

    # High z-score but only 3 samples -> reject
    assert not should_alert(sample_count=3, edits=20, zscore=4.5)
    # High z-score, 15 samples, but only 2 edits -> reject (too noisy)
    assert not should_alert(sample_count=15, edits=2, zscore=3.8)
    # Valid alert
    assert should_alert(sample_count=20, edits=35, zscore=4.2)
