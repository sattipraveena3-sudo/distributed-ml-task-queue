from __future__ import annotations

import math
import statistics
from typing import Any, Callable

POSITIVE = {"good", "great", "excellent", "love", "fast", "helpful", "amazing", "reliable"}
NEGATIVE = {"bad", "poor", "hate", "slow", "broken", "awful", "terrible", "unreliable"}


class TaskValidationError(ValueError):
    """Raised when a task payload is invalid."""


def sentiment(payload: dict[str, Any]) -> dict[str, Any]:
    text = str(payload.get("text", "")).strip()
    if not text:
        raise TaskValidationError("sentiment requires a non-empty 'text' field")
    words = {word.strip(".,!?;:\"'()[]{}").lower() for word in text.split()}
    raw = len(words & POSITIVE) - len(words & NEGATIVE)
    label = "positive" if raw > 0 else "negative" if raw < 0 else "neutral"
    confidence = 0.5 if raw == 0 else 1 / (1 + math.exp(-abs(raw)))
    return {"label": label, "confidence": round(confidence, 4), "score": raw}


def vector_summary(payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("values")
    if not isinstance(values, list) or not values:
        raise TaskValidationError("vector_summary requires a non-empty 'values' list")
    try:
        nums = [float(value) for value in values]
    except (TypeError, ValueError) as exc:
        raise TaskValidationError("all vector values must be numeric") from exc
    mean = statistics.fmean(nums)
    stdev = statistics.pstdev(nums) if len(nums) > 1 else 0.0
    return {
        "count": len(nums),
        "mean": round(mean, 6),
        "min": min(nums),
        "max": max(nums),
        "stddev": round(stdev, 6),
        "l2_norm": round(math.sqrt(sum(value * value for value in nums)), 6),
    }


def anomaly_score(payload: dict[str, Any]) -> dict[str, Any]:
    values = payload.get("values")
    value = payload.get("value")
    if not isinstance(values, list) or len(values) < 2:
        raise TaskValidationError("anomaly_score requires at least two baseline 'values'")
    try:
        nums = [float(item) for item in values]
        target = float(value)
    except (TypeError, ValueError) as exc:
        raise TaskValidationError("anomaly_score inputs must be numeric") from exc
    mean = statistics.fmean(nums)
    stdev = statistics.pstdev(nums)
    z = 0.0 if stdev == 0 else (target - mean) / stdev
    threshold = float(payload.get("threshold", 3.0))
    return {
        "value": target,
        "baseline_mean": round(mean, 6),
        "z_score": round(z, 6),
        "is_anomaly": abs(z) >= threshold,
        "threshold": threshold,
    }


def linear_predict(payload: dict[str, Any]) -> dict[str, Any]:
    features = payload.get("features")
    weights = payload.get("weights")
    if not isinstance(features, list) or not isinstance(weights, list) or len(features) != len(weights):
        raise TaskValidationError("linear_predict requires equal-length 'features' and 'weights' lists")
    try:
        score = sum(float(x) * float(w) for x, w in zip(features, weights)) + float(payload.get("bias", 0.0))
    except (TypeError, ValueError) as exc:
        raise TaskValidationError("linear_predict inputs must be numeric") from exc
    return {"prediction": round(score, 6), "features": len(features)}


TASKS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "sentiment": sentiment,
    "vector_summary": vector_summary,
    "anomaly_score": anomaly_score,
    "linear_predict": linear_predict,
}


def execute_task(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    handler = TASKS.get(task_type)
    if handler is None:
        raise TaskValidationError(f"unknown task type '{task_type}'; choose from {', '.join(sorted(TASKS))}")
    return handler(payload)


def scaling_decision(queue_depth: int, workers: int, min_workers: int = 1, max_workers: int = 12, jobs_per_worker: int = 5) -> int:
    if workers < 0 or queue_depth < 0:
        raise ValueError("workers and queue_depth must be non-negative")
    desired = max(min_workers, math.ceil(queue_depth / max(1, jobs_per_worker)))
    return min(max_workers, max(min_workers, desired))
