from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from enum import Enum


class ClusterType(Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MONGODB = "mongodb"


class ClusterStatus(Enum):
    RUNNING = "running"
    STOPPED = "stopped"
    STARTING = "starting"
    ERROR = "error"


@dataclass
class ContainerInfo:
    container_id: str
    name: str
    status: str
    port: Optional[str] = None
    role: str = "primary"


@dataclass
class ClusterConfig:
    database: str
    username: str
    password: str
    image: str
    ha_enabled: bool = False
    replication_user: Optional[str] = None
    replication_password: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'database': self.database,
            'username': self.username,
            'password': self.password,
            'image': self.image,
            'ha_enabled': self.ha_enabled,
            'replication_user': self.replication_user,
            'replication_password': self.replication_password
        }


@dataclass
class Cluster:
    name: str
    cluster_type: ClusterType
    config: ClusterConfig
    primary: ContainerInfo
    replica: Optional[ContainerInfo] = None
    status: ClusterStatus = ClusterStatus.RUNNING
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            'cluster_name': self.name,
            'type': self.cluster_type.value,
            'status': self.status.value,
            'primary': {
                'container_id': self.primary.container_id,
                'name': self.primary.name,
                'port': self.primary.port,
                'status': self.primary.status
            },
            'config': {
                'database': self.config.database,
                'username': self.config.username,
                'host': 'localhost',
                'port': self.primary.port
            }
        }
        
        if self.replica:
            result['replica'] = {
                'container_id': self.replica.container_id,
                'name': self.replica.name,
                'status': self.replica.status
            }
        
        return result
