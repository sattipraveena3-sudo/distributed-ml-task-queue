import pytest

from app.core import TaskValidationError, execute_task, scaling_decision


def test_sentiment_task():
    result = execute_task("sentiment", {"text": "excellent helpful reliable"})
    assert result["label"] == "positive"
    assert result["confidence"] > 0.5


def test_vector_summary():
    result = execute_task("vector_summary", {"values": [1, 2, 3, 4]})
    assert result["count"] == 4
    assert result["mean"] == 2.5


def test_anomaly_score():
    result = execute_task("anomaly_score", {"values": [10, 10, 11, 9, 10], "value": 30, "threshold": 2})
    assert result["is_anomaly"] is True


def test_linear_predict():
    result = execute_task("linear_predict", {"features": [2, 4], "weights": [0.5, 0.25], "bias": 1})
    assert result["prediction"] == 3.0


def test_validation_and_scaling():
    with pytest.raises(TaskValidationError):
        execute_task("missing", {})
    assert scaling_decision(30, 1, max_workers=10, jobs_per_worker=5) == 6
    assert scaling_decision(0, 5, min_workers=1) == 1
