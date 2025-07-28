import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Set test environment variable before importing
os.environ["STORAGE_PATH"] = "/tmp/test_storage_pytest"

from storage_node.agent import StorageAgent


class TestStorageAgent:
    def test_storage_agent_init(self):
        """Test storage agent initializes properly"""
        agent = StorageAgent()

        # Debug what we actually have
        print(f"Agent methods: {[m for m in dir(agent) if not m.startswith('_')]}")
        print(f"Has node_id: {hasattr(agent, 'node_id')}")

        assert hasattr(
            agent, "node_id"
        ), f"Agent missing node_id. Available: {dir(agent)}"
        assert agent.node_id is not None
        assert hasattr(agent, "storage_dir")
        assert hasattr(agent, "controller_url")

    def test_file_storage_and_retrieval(self):
        """Test basic file storage functionality"""

        async def async_test():
            with tempfile.TemporaryDirectory() as tmp_dir:
                agent = StorageAgent()
                agent.storage_dir = Path(tmp_dir)

                test_content = b"Hello world from storage test"
                file_id = "test_file_123"

                # Test storing
                success = await agent.save_file_locally(file_id, test_content)
                assert success is True

                # Test retrieving
                retrieved = await agent.load_file_locally(file_id)
                assert retrieved == test_content

        asyncio.run(async_test())

    def test_file_deletion(self):
        """Test file deletion works"""

        async def async_test():
            with tempfile.TemporaryDirectory() as tmp_dir:
                agent = StorageAgent()
                agent.storage_dir = Path(tmp_dir)

                file_id = "delete_me"
                test_data = b"temporary data"

                # Store then delete
                await agent.save_file_locally(file_id, test_data)
                success = await agent.remove_file_locally(file_id)

                assert success is True

                # Should be gone now
                file_path = agent.storage_dir / file_id
                assert not file_path.exists()

        asyncio.run(async_test())

    def test_storage_capacity_calculation(self):
        """Test storage capacity methods don't crash"""
        agent = StorageAgent()

        capacity = agent.calculate_storage_capacity()
        used_space = agent.calculate_used_space()

        assert isinstance(capacity, int)
        assert isinstance(used_space, int)
        assert capacity >= 0
        assert used_space >= 0
