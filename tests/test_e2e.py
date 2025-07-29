import os
import time

import pytest
import requests
from requests.adapters import HTTPAdapter, Retry

test_file_id = None


def test_controller_health(services, controller_client):
    """Test controller service health check"""
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")
    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/health"
    )
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "service": "controller"}


def test_nodes_are_registered(services, controller_client):
    """
    Test storage node registration with the controller

    Verifies:
    - Storage nodes successfully register with controller
    - At least one storage node is available
    - Node registration information is accessible via API
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    time.sleep(10)

    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/nodes"
    )
    assert response.status_code == 200

    nodes = response.json().get("nodes", [])
    assert len(nodes) >= 1, f"Expected at least 1 node, got {len(nodes)}"


def test_upload_file_and_check_storage(services, controller_client, db_cursor):
    """
    Test file upload

    Verifies:
    - File is successfully uploaded via API
    - File metadata is correctly stored in PostgreSQL database
    - File locations are properly recorded for all storage nodes
    - API response matches database state
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    test_file_path = os.path.join(os.path.dirname(__file__), "test_file.txt")

    with open(test_file_path, "rb") as f:
        files = {"uploaded_file": ("test_file.txt", f, "text/plain")}
        response = controller_client.post(
            f"http://{controller_host}:{controller_port}/files/upload", files=files
        )

    assert (
        response.status_code == 200
    ), f"Upload failed: {response.text if response.status_code != 200 else ''}"
    upload_result = response.json()

    assert "file_id" in upload_result
    assert "filename" in upload_result
    assert "size" in upload_result
    assert "nodes" in upload_result
    assert upload_result["filename"] == "test_file.txt"
    assert len(upload_result["nodes"]) >= 1

    file_id = upload_result["file_id"]

    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/files"
    )
    assert response.status_code == 200

    files_list = response.json().get("files", [])
    uploaded_file = next((f for f in files_list if f["file_id"] == file_id), None)

    assert uploaded_file is not None, f"File {file_id} not found in files list"
    assert uploaded_file["filename"] == "test_file.txt"

    db_cursor.execute(
        "SELECT file_id, filename, size, checksum, is_deleted FROM files WHERE file_id = %s",
        (file_id,),
    )
    file_record = db_cursor.fetchone()

    assert file_record is not None, f"File {file_id} not found in PostgreSQL database"
    assert file_record[0] == file_id
    assert file_record[1] == "test_file.txt"
    assert file_record[2] == 150
    assert file_record[4] == False

    db_cursor.execute(
        "SELECT file_id, node_id FROM file_locations WHERE file_id = %s", (file_id,)
    )
    location_records = db_cursor.fetchall()

    assert (
        len(location_records) >= 1
    ), f"No file locations found for {file_id} in PostgreSQL"
    stored_nodes = [record[1] for record in location_records]
    api_nodes = upload_result["nodes"]

    assert set(stored_nodes) == set(
        api_nodes
    ), f"Database nodes {stored_nodes} don't match API nodes {api_nodes}"

    global test_file_id
    test_file_id = file_id


def test_retrieve_file_and_check_integrity(services, controller_client):
    """
    Test file retrieval

    Verifies:
    - File can be successfully downloaded using file ID
    - Downloaded content matches original file exactly
    - HTTP headers are correctly set for file download
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    file_id = globals().get("test_file_id")
    if not file_id:
        pytest.skip(
            "No file_id from upload test - run test_upload_file_and_check_storage first"
        )

    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/files/{file_id}"
    )
    assert response.status_code == 200

    expected_content = open(
        os.path.join(os.path.dirname(__file__), "test_file.txt"), "rb"
    ).read()
    downloaded_content = response.content

    assert (
        downloaded_content == expected_content
    ), "Downloaded file content doesn't match original"
    assert "Content-Disposition" in response.headers
    assert "test_file.txt" in response.headers["Content-Disposition"]


def test_delete_file_and_verify_removal(services, controller_client, db_cursor):
    """
    Test file deletion

    Verifies:
    - File is successfully deleted via API
    - File is marked as deleted in PostgreSQL database
    - File is removed from API file listings
    - File becomes inaccessible for download after deletion
    - Database maintains audit trail with deletion flag
    """
    controller_host = "localhost"
    controller_port = os.getenv("CONTROLLER_PORT", "8000")

    file_id = globals().get("test_file_id")
    if not file_id:
        pytest.skip(
            "No file_id from upload test - run test_upload_file_and_check_storage first"
        )

    response = controller_client.delete(
        f"http://{controller_host}:{controller_port}/files/{file_id}"
    )
    assert response.status_code == 200

    delete_result = response.json()
    assert "file_id" in delete_result
    assert "status" in delete_result
    assert delete_result["file_id"] == file_id
    assert delete_result["status"] == "deleted"

    response = controller_client.get(
        f"http://{controller_host}:{controller_port}/files"
    )
    assert response.status_code == 200

    files_list = response.json().get("files", [])
    deleted_file = next((f for f in files_list if f["file_id"] == file_id), None)
    assert (
        deleted_file is None
    ), f"File {file_id} still found in files list after deletion"

    db_cursor.execute("SELECT is_deleted FROM files WHERE file_id = %s", (file_id,))
    file_record = db_cursor.fetchone()

    assert (
        file_record is not None
    ), f"File {file_id} completely removed from database (should be marked deleted)"
    assert file_record[0] == True, f"File {file_id} not marked as deleted in PostgreSQL"

    db_cursor.execute(
        "SELECT COUNT(*) FROM file_locations WHERE file_id = %s", (file_id,)
    )
    location_count = db_cursor.fetchone()[0]

    import requests

    try:
        response = requests.get(
            f"http://{controller_host}:{controller_port}/files/{file_id}", timeout=5
        )
        assert response.status_code in [
            404,
            500,
        ], f"File should return 404 or 500 after deletion, got {response.status_code}"
    except requests.exceptions.RequestException:
        pass

    global test_file_id
    test_file_id = None
