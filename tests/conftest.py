import os
import subprocess
import time

import pytest
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

load_dotenv()


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "docker-compose.yml"
    )


@pytest.fixture(scope="session")
def services(docker_compose_file):
    """
    Spins up the services defined in the docker-compose.yml file.
    At the end of the tests, it tears down the services, including volumes.
    """

    subprocess.run(
        ["docker-compose", "-f", docker_compose_file, "down", "--volumes"],
        capture_output=True,
        text=True,
    )

    # Start the services
    result = subprocess.run(
        ["docker-compose", "-f", docker_compose_file, "up", "-d", "--build"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"Docker compose up failed: {result.stderr}")
        raise Exception(f"Failed to start services: {result.stderr}")

    # Wait for services to be ready
    print("Waiting for services to start...")
    time.sleep(30)  # Increased wait time

    # Check if controller is responding
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    for attempt in range(10):
        try:
            response = requests.get(
                f"http://{controller_host}:{controller_port}/health", timeout=5
            )
            if response.status_code == 200:
                print("Controller is ready!")
                break
        except requests.exceptions.RequestException:
            print(f"Attempt {attempt + 1}: Controller not ready yet, waiting...")
            time.sleep(5)
    else:
        # Print docker logs for debugging
        logs_result = subprocess.run(
            ["docker-compose", "-f", docker_compose_file, "logs"],
            capture_output=True,
            text=True,
        )
        print(f"Docker logs:\n{logs_result.stdout}")
        raise Exception("Controller failed to start after 10 attempts")

    yield

    # Cleanup
    subprocess.run(
        ["docker-compose", "-f", docker_compose_file, "down", "--volumes"],
        capture_output=True,
        text=True,
    )


@pytest.fixture(scope="session")
def controller_client():
    """
    Returns a requests session for interacting with the controller API.
    """
    client = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    client.mount("http://", HTTPAdapter(max_retries=retries))
    return client
