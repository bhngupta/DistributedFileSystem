from datetime import datetime

import pytest

from controller.database import FileLocation, FileMetadata, StorageNode


class TestDatabaseModels:
    def test_file_metadata_creation(self, test_db):
        """Test creating file metadata record"""
        file_record = FileMetadata(
            file_id="test-123",
            filename="my_document.pdf",
            size=1024,
            checksum="abc123def456",
        )

        test_db.add(file_record)
        test_db.commit()

        # Query it back
        retrieved = (
            test_db.query(FileMetadata)
            .filter(FileMetadata.file_id == "test-123")
            .first()
        )

        assert retrieved is not None
        assert retrieved.filename == "my_document.pdf"
        assert retrieved.size == 1024

    def test_storage_node_creation(self, test_db):
        """Test storage node model"""
        node = StorageNode(
            node_id="storage_node_1",
            url="http://localhost:8001",
            capacity=1000000,
            used_space=50000,
        )

        test_db.add(node)
        test_db.commit()

        found = (
            test_db.query(StorageNode)
            .filter(StorageNode.node_id == "storage_node_1")
            .first()
        )

        assert found.url == "http://localhost:8001"
        assert found.capacity == 1000000

    def test_file_location_tracking(self, test_db):
        """Test file location relationships"""
        # Create a file and node first
        file_rec = FileMetadata(
            file_id="file-456", filename="test.txt", size=100, checksum="hash123"
        )

        node_rec = StorageNode(
            node_id="node-1", url="http://node1:8000", capacity=999999
        )

        test_db.add(file_rec)
        test_db.add(node_rec)
        test_db.commit()

        # Now create location
        location = FileLocation(file_id="file-456", node_id="node-1")

        test_db.add(location)
        test_db.commit()

        # Verify
        loc = (
            test_db.query(FileLocation)
            .filter(FileLocation.file_id == "file-456")
            .first()
        )

        assert loc.node_id == "node-1"
