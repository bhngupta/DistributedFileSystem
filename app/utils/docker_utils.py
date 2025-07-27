import docker
import time
from typing import Dict, Any


def wait_for_container_healthy(container, max_wait=60):
    start_time = time.time()
    while time.time() - start_time < max_wait:
        container.reload()
        if container.status == 'running':
            return True
        time.sleep(2)
    return False


def get_container_logs(container, tail=50):
    try:
        return container.logs(tail=tail).decode('utf-8')
    except Exception:
        return "Unable to retrieve logs"


def validate_docker_connection():
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False
