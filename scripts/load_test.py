import argparse
import random
import time

import requests

SAMPLES = [
    {"task_type": "sentiment", "payload": {"text": "excellent fast reliable service"}},
    {"task_type": "sentiment", "payload": {"text": "slow broken unreliable service"}},
    {"task_type": "vector_summary", "payload": {"values": [1, 2, 3, 4, 5, 6]}},
    {"task_type": "anomaly_score", "payload": {"values": [10, 11, 9, 10, 10.5], "value": 24}},
    {"task_type": "linear_predict", "payload": {"features": [1.2, 3.4], "weights": [0.5, -0.2], "bias": 0.1}},
]


def main():
    parser = argparse.ArgumentParser(description="Generate burst traffic for the Distributed ML Task Queue")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--jobs", type=int, default=100)
    parser.add_argument("--delay", type=float, default=0.02)
    args = parser.parse_args()
    started = time.time()
    accepted = 0
    for index in range(args.jobs):
        sample = random.choice(SAMPLES)
        response = requests.post(f"{args.url}/api/jobs", json={**sample, "max_retries": 2}, timeout=5)
        response.raise_for_status()
        accepted += 1
        time.sleep(args.delay if index % 25 < 20 else args.delay * 10)
    elapsed = time.time() - started
    print(f"accepted={accepted} elapsed={elapsed:.2f}s submit_rate={accepted / max(elapsed, 0.001):.1f}/s")


if __name__ == "__main__":
    main()
