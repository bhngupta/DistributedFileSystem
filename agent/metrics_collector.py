import time
from typing import Dict

import psutil
import requests


class MetricsCollector:
    """Collect system metrics periodically"""

    def __init__(self, interval: int, endpoint: str):
        self.interval = interval
        self.endpoint = endpoint

    def collect_metrics(self) -> Dict:
        """Collect disk usage, CPU, and I/O stats"""
        metrics = {
            "disk_usage": psutil.disk_usage("/")._asdict(),
            "cpu_percent": psutil.cpu_percent(interval=None),
            "io_counters": psutil.disk_io_counters()._asdict(),
        }
        return metrics

    def send_metrics(self, metrics: Dict):
        """Send metrics to the FastAPI endpoint"""
        try:
            response = requests.post(self.endpoint, json=metrics)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Failed to send metrics: {e}")

    def start(self):
        """Start collecting and sending metrics periodically"""
        while True:
            metrics = self.collect_metrics()
            self.send_metrics(metrics)
            time.sleep(self.interval)


if __name__ == "__main__":
    collector = MetricsCollector(interval=60, endpoint="http://localhost:8000/metrics")
    collector.start()
