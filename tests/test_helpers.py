import os
import subprocess
import time
from typing import Dict, List, Tuple

import requests


class E2ETestHelpers:

    def __init__(self, controller_client, db_cursor):
        self.controller_client = controller_client
        self.db_cursor = db_cursor
        self.controller_host = "localhost"
        self.controller_port = os.getenv("CONTROLLER_PORT", "8000")
        self.base_url = f"http://{self.controller_host}:{self.controller_port}"

    def create_test_file(self, filename: str, content: str) -> str:
        test_file_path = os.path.join(os.path.dirname(__file__), filename)
        with open(test_file_path, "w") as f:
            f.write(content)
        return test_file_path

    def cleanup_test_file(self, file_path: str):
        if os.path.exists(file_path):
            os.remove(file_path)

    def upload_file(self, file_path: str, filename: str) -> Dict:
        with open(file_path, "rb") as f:
            files = {"uploaded_file": (filename, f, "text/plain")}
            response = self.controller_client.post(
                f"{self.base_url}/files/upload", files=files
            )

        assert response.status_code == 200, f"Upload failed: {response.text}"
        return response.json()

    def retrieve_file(self, file_id: str) -> Tuple[int, bytes]:
        try:
            response = self.controller_client.get(
                f"{self.base_url}/files/{file_id}", timeout=30
            )
            return response.status_code, response.content
        except Exception as e:
            print(f"Error retrieving file {file_id}: {e}")
            return 500, b""

    def delete_file(self, file_id: str) -> Dict:
        response = self.controller_client.delete(f"{self.base_url}/files/{file_id}")
        assert response.status_code == 200, f"Delete failed: {response.text}"
        return response.json()

    def get_nodes(self) -> List[Dict]:
        response = self.controller_client.get(f"{self.base_url}/nodes")
        assert response.status_code == 200, "Failed to get nodes"
        return response.json().get("nodes", [])

    def get_node_health(self) -> Dict:
        response = self.controller_client.get(f"{self.base_url}/nodes/health")
        assert response.status_code == 200, "Failed to get node health"
        return response.json()

    def get_files_list(self) -> List[Dict]:
        response = self.controller_client.get(f"{self.base_url}/files")
        assert response.status_code == 200, "Failed to get files list"
        return response.json().get("files", [])

    def verify_file_in_database(
        self, file_id: str, filename: str, should_exist: bool = True
    ):
        self.db_cursor.execute(
            "SELECT file_id, filename, size, is_deleted FROM files WHERE file_id = %s",
            (file_id,),
        )
        file_record = self.db_cursor.fetchone()

        if should_exist:
            assert file_record is not None, f"File {file_id} not found in database"
            assert file_record[1] == filename, f"Filename mismatch in database"
            assert file_record[3] == False, f"File {file_id} marked as deleted"
        else:
            if file_record is not None:
                assert (
                    file_record[3] == True
                ), f"File {file_id} should be marked as deleted"

    def verify_file_locations(self, file_id: str, expected_nodes: List[str]):
        self.db_cursor.execute(
            "SELECT node_id FROM file_locations WHERE file_id = %s", (file_id,)
        )
        db_nodes = [record[0] for record in self.db_cursor.fetchall()]
        assert set(db_nodes) == set(
            expected_nodes
        ), f"Database nodes {db_nodes} don't match expected {expected_nodes}"

    def verify_file_content_integrity(
        self, downloaded_content: bytes, expected_file_path: str
    ):
        with open(expected_file_path, "rb") as f:
            expected_content = f.read()
        assert (
            downloaded_content == expected_content
        ), "File content integrity check failed"

    def stop_node(self, node_name: str, docker_compose_file: str):
        result = subprocess.run(
            ["docker-compose", "-f", docker_compose_file, "stop", node_name],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Failed to stop {node_name}: {result.stderr}"

    def start_node(self, node_name: str, docker_compose_file: str):
        result = subprocess.run(
            ["docker-compose", "-f", docker_compose_file, "up", "-d", node_name],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"Failed to start {node_name}: {result.stderr}"

    def verify_controller_health(self):
        response = self.controller_client.get(f"{self.base_url}/health")
        assert response.status_code == 200, "Controller health check failed"
        health = response.json()
        assert health["status"] == "healthy", "Controller not healthy"
        assert health["service"] == "controller", "Wrong service type"

    def upload_multiple_files(
        self, count: int, base_filename: str = "test_file"
    ) -> List[Dict]:
        uploaded_files = []
        test_files = []

        for i in range(count):
            filename = f"{base_filename}_{i}.txt"
            content = (
                f"This is test file number {i}\nContent for testing.\nFile ID: {i}"
            )
            file_path = self.create_test_file(filename, content)
            test_files.append(file_path)

        for i, file_path in enumerate(test_files):
            filename = f"{base_filename}_{i}.txt"
            upload_result = self.upload_file(file_path, filename)
            uploaded_files.append(upload_result)

        for file_path in test_files:
            self.cleanup_test_file(file_path)

        return uploaded_files

    def verify_file_distribution(self, uploaded_files: List[Dict]):
        all_nodes_used = set()

        for upload_result in uploaded_files:
            file_id = upload_result["file_id"]
            filename = upload_result["filename"]
            nodes_for_file = set(upload_result["nodes"])

            self.verify_file_in_database(file_id, filename)
            self.verify_file_locations(file_id, list(nodes_for_file))

            all_nodes_used.update(nodes_for_file)

        available_nodes = self.get_nodes()
        if len(available_nodes) > 1:
            assert (
                len(all_nodes_used) > 1
            ), f"Files not distributed - only used: {all_nodes_used}"

        return all_nodes_used

    def comprehensive_file_verification(
        self, file_id: str, original_file_path: str, filename: str
    ):
        files_list = self.get_files_list()
        api_file = next((f for f in files_list if f["file_id"] == file_id), None)
        assert api_file is not None, f"File {file_id} not in API files list"
        assert api_file["filename"] == filename, "Filename mismatch in API"

        self.verify_file_in_database(file_id, filename)

        status_code, content = self.retrieve_file(file_id)
        assert status_code == 200, "File retrieval failed"
        self.verify_file_content_integrity(content, original_file_path)

    def verify_database_connectivity(self):
        try:
            self.db_cursor.execute("SELECT 1")
            result = self.db_cursor.fetchone()
            assert result[0] == 1, "Database connectivity test failed"

            self.db_cursor.execute(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name IN ('files', 'file_locations', 'storage_nodes')
            """
            )
            tables = [row[0] for row in self.db_cursor.fetchall()]
            expected_tables = ["files", "file_locations", "storage_nodes"]

            for table in expected_tables:
                assert (
                    table in tables
                ), f"Required table '{table}' not found in database"

        except Exception as e:
            raise AssertionError(f"Database connectivity check failed: {e}")

    def wait_for_node_registration(
        self, expected_count: int, timeout: int = 30
    ) -> List[Dict]:
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                nodes = self.get_nodes()
                if len(nodes) >= expected_count:
                    print(f"✓ {len(nodes)} nodes registered")
                    return nodes
                time.sleep(2)
            except:
                time.sleep(2)

        nodes = self.get_nodes()
        assert (
            len(nodes) >= expected_count
        ), f"Only {len(nodes)} nodes registered after {timeout}s, expected {expected_count}"
        return nodes
