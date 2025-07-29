import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from test_helpers import E2ETestHelpers

test_state = {"uploaded_files": [], "test_node_count": 0}


# ============================================================================
# INFRASTRUCTURE TESTS - Test basic system setup and health
# ============================================================================


def test_infrastructure_bootstrap(services, controller_client, db_cursor):
    """
    Test complete infrastructure bootstrap process.

    Verifies:
    - Controller service health and readiness
    - Storage nodes register successfully
    - Database connectivity and schema
    - Network communication between components

    This is critical for Kubernetes - ensures pods start correctly.
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    helpers.verify_controller_health()
    nodes = helpers.wait_for_node_registration(2, timeout=60)
    assert len(nodes) >= 2, f"Expected at least 2 nodes, got {len(nodes)}"
    helpers.verify_database_connectivity()
    test_state["test_node_count"] = len(nodes)
    print(f"✓ Infrastructure bootstrapped with {len(nodes)} nodes")


# ============================================================================
# CORE FUNCTIONALITY TESTS - Test individual features in isolation
# ============================================================================


def test_file_lifecycle_operations(services, controller_client, db_cursor):
    """
    Test complete file lifecycle: upload → retrieve → delete.

    This replaces the separate upload/retrieve/delete tests and focuses on
    the core file storage functionality that must work regardless of orchestration.
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    test_file_path = helpers.create_test_file(
        "lifecycle_test.txt",
        "File lifecycle testing content\nUpload → Retrieve → Delete",
    )

    try:
        upload_result = helpers.upload_file(test_file_path, "lifecycle_test.txt")
        assert "file_id" in upload_result
        assert upload_result["filename"] == "lifecycle_test.txt"
        assert len(upload_result["nodes"]) >= 1
        file_id = upload_result["file_id"]

        helpers.verify_file_in_database(file_id, "lifecycle_test.txt")
        helpers.verify_file_locations(file_id, upload_result["nodes"])

        status_code, downloaded_content = helpers.retrieve_file(file_id)
        assert status_code == 200, f"Expected 200, got {status_code}"
        helpers.verify_file_content_integrity(downloaded_content, test_file_path)

        delete_result = helpers.delete_file(file_id)
        assert delete_result["status"] == "deleted"
        helpers.verify_file_in_database(
            file_id, "lifecycle_test.txt", should_exist=False
        )

        status_code, _ = helpers.retrieve_file(file_id)
        assert status_code in [404, 500]
        print(f"✓ File lifecycle test passed: {file_id}")
    finally:
        helpers.cleanup_test_file(test_file_path)


def test_multi_file_distribution_and_load_balancing(
    services, controller_client, db_cursor
):
    """
    Test file distribution across multiple nodes and load balancing.

    Critical for Kubernetes where nodes are dynamic and need proper distribution.
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    uploaded_files = helpers.upload_multiple_files(5, "distribution_test")

    try:
        all_nodes_used = helpers.verify_file_distribution(uploaded_files)
        available_nodes = helpers.get_nodes()
        if len(available_nodes) > 1:
            assert (
                len(all_nodes_used) > 1
            ), f"Poor distribution - only used: {all_nodes_used}"
        test_state["uploaded_files"].extend([f["file_id"] for f in uploaded_files])
        print(
            f"✓ Distribution test: {len(uploaded_files)} files across {len(all_nodes_used)} nodes"
        )
    finally:
        for file_info in uploaded_files:
            try:
                helpers.delete_file(file_info["file_id"])
            except:
                pass


# ============================================================================
# RESILIENCE TESTS - Test fault tolerance and recovery
# ============================================================================


def test_node_failure_detection_and_resilience(
    services, controller_client, db_cursor, docker_compose_file
):
    """
    Test system behavior during node failures.

    For Kubernetes: This tests pod failure scenarios and service resilience.
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    initial_nodes = helpers.get_nodes()
    if len(initial_nodes) < 2:
        pytest.skip("Need at least 2 nodes for failure testing")

    test_file_path = helpers.create_test_file(
        "resilience_test.txt", "Testing system resilience during node failure"
    )

    try:
        upload_result = helpers.upload_file(test_file_path, "resilience_test.txt")
        file_id = upload_result["file_id"]

        helpers.stop_node("storage-node-2", docker_compose_file)
        time.sleep(15)
        helpers.verify_controller_health()

        status_code, _ = helpers.retrieve_file(file_id)
        assert status_code == 200, "File should remain accessible during node failure"

        new_file_path = helpers.create_test_file(
            "during_failure.txt", "Upload during failure"
        )
        try:
            new_upload = helpers.upload_file(new_file_path, "during_failure.txt")
            assert "file_id" in new_upload
            helpers.delete_file(new_upload["file_id"])
        finally:
            helpers.cleanup_test_file(new_file_path)

        helpers.delete_file(file_id)
        helpers.start_node("storage-node-2", docker_compose_file)
        time.sleep(10)
        print("✓ Node failure resilience test passed")
    finally:
        helpers.cleanup_test_file(test_file_path)


def test_orchestration_health_monitoring(services, controller_client, db_cursor):
    """
    Test health monitoring and node status reporting.

    Critical for Kubernetes integration - tests the health endpoints that
    Kubernetes will use for readiness/liveness probes.
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    helpers.verify_controller_health()

    health_status = helpers.get_node_health()
    assert "active_nodes" in health_status
    assert "min_required" in health_status
    assert isinstance(health_status["active_nodes"], int)
    assert health_status["active_nodes"] >= 0

    nodes = helpers.get_nodes()
    for node in nodes:
        assert "node_id" in node
        assert "url" in node
        assert "last_heartbeat" in node

    print(f"✓ Health monitoring: {health_status['active_nodes']} active nodes")


# ============================================================================
# INTEGRATION TEST - One comprehensive test for full workflow
# ============================================================================


def test_end_to_end_workflow_integration(
    services, controller_client, db_cursor, docker_compose_file
):
    """
    Comprehensive integration test that validates the complete system workflow.

    This test simulates a real-world scenario and validates that all components
    work together correctly. Perfect for validating Kubernetes deployments.

    Workflow: Upload files → Verify distribution → Simulate failure →
             Verify recovery → Cleanup
    """
    helpers = E2ETestHelpers(controller_client, db_cursor)
    uploaded_files = []

    try:
        print("\n=== Integration Test: End-to-End Workflow ===")
        helpers.verify_controller_health()
        initial_nodes = helpers.wait_for_node_registration(2)
        print(f"✓ System ready with {len(initial_nodes)} nodes")

        test_files = [
            ("integration_doc.txt", "Document content for integration testing"),
            ("integration_config.json", '{"test": "integration", "nodes": 2}'),
            ("integration_data.csv", "id,name\n1,test1\n2,test2"),
        ]

        for filename, content in test_files:
            test_file_path = helpers.create_test_file(filename, content)
            upload_result = helpers.upload_file(test_file_path, filename)
            uploaded_files.append(
                {
                    "file_id": upload_result["file_id"],
                    "filename": filename,
                    "path": test_file_path,
                    "nodes": upload_result["nodes"],
                }
            )
        print(f"✓ Uploaded {len(uploaded_files)} files")

        if len(initial_nodes) >= 2:
            helpers.stop_node("storage-node-2", docker_compose_file)
            time.sleep(10)

            accessible_count = 0
            for file_info in uploaded_files:
                try:
                    status_code, _ = helpers.retrieve_file(file_info["file_id"])
                    if status_code == 200:
                        accessible_count += 1
                except:
                    pass

            print(
                f"✓ Resilience: {accessible_count}/{len(uploaded_files)} files accessible during failure"
            )
            helpers.start_node("storage-node-2", docker_compose_file)
            time.sleep(10)

        for file_info in uploaded_files:
            helpers.comprehensive_file_verification(
                file_info["file_id"], file_info["path"], file_info["filename"]
            )
        print("✓ All files verified for integrity")

        for file_info in uploaded_files:
            helpers.delete_file(file_info["file_id"])
        print("✓ Integration workflow completed successfully")
    finally:
        for file_info in uploaded_files:
            if "path" in file_info:
                helpers.cleanup_test_file(file_info["path"])


# ============================================================================
# KUBERNETES-SPECIFIC TESTS (to be added later)
# ============================================================================


@pytest.mark.skip(reason="Kubernetes orchestration not yet implemented")
def test_kubernetes_pod_scaling():
    """
    Test Kubernetes horizontal pod autoscaling.

    Will test:
    - Storage node pods scale up under load
    - Scale down when load decreases
    - Controller maintains consistency during scaling
    """
    pass


@pytest.mark.skip(reason="Kubernetes orchestration not yet implemented")
def test_kubernetes_persistent_volumes():
    """
    Test Kubernetes persistent volume handling.

    Will test:
    - PVs are properly mounted to storage pods
    - Data persists through pod restarts
    - Volume cleanup on pod deletion
    """
    pass


@pytest.mark.skip(reason="Kubernetes orchestration not yet implemented")
def test_kubernetes_service_discovery():
    """
    Test Kubernetes service discovery and networking.

    Will test:
    - Storage nodes discover controller via K8s services
    - Load balancing across storage pod replicas
    - Network policies and security
    """
    pass
