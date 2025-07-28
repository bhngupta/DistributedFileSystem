import os
import time

import pytest
import requests
from requests.adapters import HTTPAdapter, Retry


def test_controller_health(services, controller_client):
    """
    Tests if the controller service is healthy.
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")
    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/health"
    )
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "controller"}


def test_nodes_are_registered(services, controller_client):
    """
    Tests if the storage nodes have registered with the controller.
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    # Allow some time for nodes to register
    time.sleep(10)

    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/nodes"
    )
    assert response.status_code == 200

    nodes = response.json().get("nodes", [])
    print(f"Found {len(nodes)} registered nodes")

    # Check that we have at least one node
    assert len(nodes) >= 1, f"Expected at least 1 node, got {len(nodes)}"

    if len(nodes) > 0:
        node_ids = {node["node_id"] for node in nodes}
        print(f"Registered node IDs: {node_ids}")
