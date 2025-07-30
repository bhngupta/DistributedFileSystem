import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from .database import DatabaseSession, NodeMetrics, get_db_session

logger = logging.getLogger(__name__)


class MonitoringService:
    def __init__(self):
        self.metrics_cache = {}

    def record_node_metrics(self, node_id: str, metrics: Dict) -> bool:
        """Record metrics for a specific node"""
        try:
            with DatabaseSession() as db:
                node_metrics = NodeMetrics(
                    node_id=node_id,
                    total_storage_bytes=metrics.get("total_storage_bytes", 0),
                    used_storage_bytes=metrics.get("used_storage_bytes", 0),
                    available_storage_bytes=metrics.get("available_storage_bytes", 0),
                    files_count=metrics.get("files_count", 0),
                    upload_ops_count=metrics.get("upload_ops_count", 0),
                    download_ops_count=metrics.get("download_ops_count", 0),
                    delete_ops_count=metrics.get("delete_ops_count", 0),
                    avg_response_time_ms=metrics.get("avg_response_time_ms", 0.0),
                    is_healthy=metrics.get("is_healthy", True),
                    cpu_usage_percent=metrics.get("cpu_usage_percent", 0.0),
                    memory_usage_percent=metrics.get("memory_usage_percent", 0.0),
                )
                db.add(node_metrics)
                db.commit()
                logger.info(f"Recorded metrics for node {node_id}")
                return True
        except Exception as e:
            logger.error(f"Failed to record metrics for node {node_id}: {e}")
            return False

    def get_node_metrics_history(self, node_id: str, hours: int = 24) -> List[Dict]:
        """Get historical metrics for a node"""
        try:
            with DatabaseSession() as db:
                since = datetime.utcnow() - timedelta(hours=hours)
                metrics = (
                    db.query(NodeMetrics)
                    .filter(
                        NodeMetrics.node_id == node_id, NodeMetrics.timestamp >= since
                    )
                    .order_by(NodeMetrics.timestamp.desc())
                    .all()
                )

                return [
                    {
                        "timestamp": m.timestamp.isoformat(),
                        "total_storage_bytes": m.total_storage_bytes,
                        "used_storage_bytes": m.used_storage_bytes,
                        "available_storage_bytes": m.available_storage_bytes,
                        "files_count": m.files_count,
                        "upload_ops_count": m.upload_ops_count,
                        "download_ops_count": m.download_ops_count,
                        "delete_ops_count": m.delete_ops_count,
                        "avg_response_time_ms": m.avg_response_time_ms,
                        "is_healthy": m.is_healthy,
                        "cpu_usage_percent": m.cpu_usage_percent,
                        "memory_usage_percent": m.memory_usage_percent,
                    }
                    for m in metrics
                ]
        except Exception as e:
            logger.error(f"Failed to get metrics history for node {node_id}: {e}")
            return []

    def get_cluster_overview(self) -> Dict:
        """Get cluster-wide metrics overview"""
        try:
            with DatabaseSession() as db:
                # Get latest metrics for each node
                latest_metrics = {}
                nodes = db.query(NodeMetrics.node_id).distinct().all()

                for (node_id,) in nodes:
                    latest = (
                        db.query(NodeMetrics)
                        .filter(NodeMetrics.node_id == node_id)
                        .order_by(NodeMetrics.timestamp.desc())
                        .first()
                    )

                    if latest:
                        latest_metrics[node_id] = {
                            "timestamp": latest.timestamp.isoformat(),
                            "total_storage_bytes": latest.total_storage_bytes,
                            "used_storage_bytes": latest.used_storage_bytes,
                            "available_storage_bytes": latest.available_storage_bytes,
                            "files_count": latest.files_count,
                            "is_healthy": latest.is_healthy,
                            "total_ops": latest.upload_ops_count
                            + latest.download_ops_count
                            + latest.delete_ops_count,
                            "avg_response_time_ms": latest.avg_response_time_ms,
                        }

                # Calculate cluster totals
                total_storage = sum(
                    m["total_storage_bytes"] for m in latest_metrics.values()
                )
                total_used = sum(
                    m["used_storage_bytes"] for m in latest_metrics.values()
                )
                total_files = sum(m["files_count"] for m in latest_metrics.values())
                healthy_nodes = sum(
                    1 for m in latest_metrics.values() if m["is_healthy"]
                )

                return {
                    "cluster_summary": {
                        "total_nodes": len(latest_metrics),
                        "healthy_nodes": healthy_nodes,
                        "total_storage_bytes": total_storage,
                        "total_used_bytes": total_used,
                        "total_available_bytes": total_storage - total_used,
                        "total_files": total_files,
                        "storage_utilization_percent": (
                            (total_used / total_storage * 100)
                            if total_storage > 0
                            else 0
                        ),
                    },
                    "nodes": latest_metrics,
                }
        except Exception as e:
            logger.error(f"Failed to get cluster overview: {e}")
            return {"cluster_summary": {}, "nodes": {}}

    def cleanup_old_metrics(self, days: int = 7) -> int:
        """Clean up metrics older than specified days"""
        try:
            with get_db_session() as db:
                cutoff = datetime.utcnow() - timedelta(days=days)
                deleted = (
                    db.query(NodeMetrics)
                    .filter(NodeMetrics.timestamp < cutoff)
                    .delete()
                )
                db.commit()
                logger.info(f"Cleaned up {deleted} old metric records")
                return deleted
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {e}")
            return 0
