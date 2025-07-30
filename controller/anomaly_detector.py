import logging
from datetime import datetime, timedelta
from typing import Dict, List

from .database import DatabaseSession, FileLocation, FileMetadata, NodeMetrics

logger = logging.getLogger(__name__)


class AnomalyDetector:
    def __init__(self):
        self.disk_usage_threshold = 0  # Disk usage dropping to 0
        self.usage_spike_factor = 10  # Sudden spike in usage
        self.inactivity_minutes = 30  # Node reporting no activity for X minutes

    def detect_anomalies(self) -> List[str]:
        anomalies = []

        try:
            with DatabaseSession() as db:
                # Detect disk usage dropping to 0
                anomalies.extend(self._detect_disk_usage_anomalies(db))

                # Detect sudden spikes in usage
                anomalies.extend(self._detect_usage_spikes(db))

                # Detect node inactivity
                anomalies.extend(self._detect_inactive_nodes(db))

                # Detect file corruption or orphan data
                anomalies.extend(self._detect_file_anomalies(db))

        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")

        return anomalies

    def _detect_disk_usage_anomalies(self, db) -> List[str]:
        anomalies = []
        nodes = db.query(NodeMetrics).all()

        for node in nodes:
            if node.used_storage_bytes == self.disk_usage_threshold:
                anomalies.append(f"Node {node.node_id} has disk usage dropping to 0.")

        return anomalies

    def _detect_usage_spikes(self, db) -> List[str]:
        anomalies = []
        nodes = db.query(NodeMetrics).all()

        for node in nodes:
            if (
                node.upload_ops_count + node.download_ops_count + node.delete_ops_count
                > self.usage_spike_factor * node.files_count
            ):
                anomalies.append(f"Node {node.node_id} has a sudden spike in usage.")

        return anomalies

    def _detect_inactive_nodes(self, db) -> List[str]:
        anomalies = []
        cutoff_time = datetime.utcnow() - timedelta(minutes=self.inactivity_minutes)
        inactive_nodes = (
            db.query(NodeMetrics).filter(NodeMetrics.timestamp < cutoff_time).all()
        )

        for node in inactive_nodes:
            anomalies.append(
                f"Node {node.node_id} has been inactive for more than {self.inactivity_minutes} minutes."
            )

        return anomalies

    def _detect_file_anomalies(self, db) -> List[str]:
        anomalies = []

        # Detect files in DB but missing on nodes
        files_in_db = db.query(FileMetadata).all()
        for file in files_in_db:
            locations = (
                db.query(FileLocation)
                .filter(FileLocation.file_id == file.file_id)
                .all()
            )
            if not locations:
                anomalies.append(
                    f"File {file.file_id} exists in DB but is missing on all nodes."
                )

        # Detect files on nodes but missing in DB
        files_on_nodes = db.query(FileLocation).all()
        for file_location in files_on_nodes:
            file_in_db = (
                db.query(FileMetadata)
                .filter(FileMetadata.file_id == file_location.file_id)
                .first()
            )
            if not file_in_db:
                anomalies.append(
                    f"File {file_location.file_id} exists on node {file_location.node_id} but has no DB entry."
                )

        return anomalies
