import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Set up test environment before importing app
os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from controller.main import app


class TestControllerAPI:
    @patch("controller.main.node_svc")
    def test_list_nodes(self, mock_node_svc):
        """Test node listing endpoint"""
        mock_node_svc.get_active_nodes = AsyncMock(
            return_value=[
                {
                    "node_id": "test-node",
                    "url": "http://localhost:8001",
                    "capacity": 1000,
                }
            ]
        )

        client = TestClient(app)
        response = client.get("/nodes")
        assert response.status_code == 200
        data = response.json()
        assert "nodes" in data
        assert isinstance(data["nodes"], list)

    @patch("controller.main.file_svc")
    def test_upload_endpoint_exists(self, mock_file_svc):
        """Test upload endpoint with proper file format"""
        mock_file_svc.store_file = AsyncMock(
            return_value={
                "success": True,
                "file_id": "test-123",
                "nodes": ["node1", "node2"],
            }
        )

        client = TestClient(app)
        files = {"uploaded_file": ("test.txt", b"test content", "text/plain")}
        response = client.post("/files/upload", files=files)
        assert response.status_code == 200
        data = response.json()
        assert "file_id" in data

    @patch("controller.main.file_svc")
    def test_file_list_endpoint(self, mock_file_svc):
        """Test file listing endpoint"""
        mock_file_svc.list_files = AsyncMock(
            return_value=[{"file_id": "test-123", "filename": "test.txt", "size": 12}]
        )

        client = TestClient(app)
        response = client.get("/files")
        assert response.status_code == 200
        data = response.json()
        assert "files" in data
        assert isinstance(data["files"], list)
