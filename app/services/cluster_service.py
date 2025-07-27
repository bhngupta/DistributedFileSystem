import docker
from typing import Dict, Any, List
from flask import current_app


class ClusterService:
    def __init__(self):
        self.client = docker.from_env()
        self.network_name = current_app.config.get('DOCKER_NETWORK', 'autoprovisioner-network')
        self._ensure_network()

    # TODO: Integrate app.models when adding persistence
    # Currently using Docker API directly, future: Cluster/ContainerInfo dataclasses
        
    def _ensure_network(self):
        try:
            self.client.networks.get(self.network_name)
        except docker.errors.NotFound:
            self.client.networks.create(
                self.network_name,
                driver="bridge"
            )
    
    def provision_cluster(self, cluster_type: str, cluster_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        db_configs = current_app.config.get('DATABASE_CONFIGS', {})
        if cluster_type not in db_configs:
            raise ValueError(f"Unsupported cluster type: {cluster_type}")
        
        db_config = db_configs[cluster_type].copy()
        db_config.update(config)
        
        # Start containers based on cluster type
        if cluster_type == 'postgresql':
            return self._provision_postgresql(cluster_name, db_config)
        elif cluster_type == 'mysql':
            return self._provision_mysql(cluster_name, db_config)
        elif cluster_type == 'mongodb':
            return self._provision_mongodb(cluster_name, db_config)
        else:
            raise ValueError(f"Provisioning not implemented for {cluster_type}")


    def _provision_postgresql(self, cluster_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        primary_name = f"{cluster_name}-primary"
        replica_name = f"{cluster_name}-replica"
        
        env = {
            'POSTGRES_DB': config['database'],
            'POSTGRES_USER': config['username'],
            'POSTGRES_PASSWORD': config['password'],
            'POSTGRES_REPLICATION_USER': config['replication_user'],
            'POSTGRES_REPLICATION_PASSWORD': config['replication_password']
        }
        
        # Start primary 
        primary = self.client.containers.run(
            config['image'],
            name=primary_name,
            environment=env,
            ports={'5432/tcp': None},
            network=self.network_name,
            detach=True,
            command=[
                'postgres',
                '-c', 'wal_level=replica',
                '-c', 'max_wal_senders=3',
                '-c', 'max_replication_slots=3',
                '-c', 'hot_standby=on'
            ]
        )
        
        # Wait for primary to be ready
        import time
        time.sleep(5)
        
        # Start replica if HA enabled
        replica = None
        if config.get('ha_enabled', False):
            replica_env = env.copy()
            replica_env['PGUSER'] = config['replication_user']
            replica_env['POSTGRES_MASTER_SERVICE'] = primary_name
            
            replica = self.client.containers.run(
                config['image'],
                name=replica_name,
                environment=replica_env,
                network=self.network_name,
                detach=True,
                command=[
                    'bash', '-c',
                    f'pg_basebackup -h {primary_name} -D /var/lib/postgresql/data -U {config["replication_user"]} -v -P -W'
                ]
            )
        
        # Get port mappings
        primary.reload()
        primary_port = primary.ports['5432/tcp'][0]['HostPort']
        
        result = {
            'cluster_name': cluster_name,
            'type': 'postgresql',
            'primary': {
                'container_id': primary.id,
                'name': primary_name,
                'port': primary_port,
                'status': primary.status
            },
            'config': {
                'database': config['database'],
                'username': config['username'],
                'host': 'localhost',
                'port': primary_port
            }
        }
        
        if replica:
            result['replica'] = {
                'container_id': replica.id,
                'name': replica_name,
                'status': replica.status
            }
        
        return result
    
    def _provision_mysql(self, cluster_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        primary_name = f"{cluster_name}-primary"
        
        env = {
            'MYSQL_ROOT_PASSWORD': config['password'],
            'MYSQL_DATABASE': config['database'],
            'MYSQL_USER': config['username'],
            'MYSQL_PASSWORD': config['password']
        }
        
        primary = self.client.containers.run(
            config['image'],
            name=primary_name,
            environment=env,
            ports={'3306/tcp': None},
            network=self.network_name,
            detach=True
        )
        
        primary.reload()
        primary_port = primary.ports['3306/tcp'][0]['HostPort']
        
        return {
            'cluster_name': cluster_name,
            'type': 'mysql',
            'primary': {
                'container_id': primary.id,
                'name': primary_name,
                'port': primary_port,
                'status': primary.status
            },
            'config': {
                'database': config['database'],
                'username': config['username'],
                'host': 'localhost',
                'port': primary_port
            }
        }
    
    def _provision_mongodb(self, cluster_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Provision MongoDB cluster"""
        primary_name = f"{cluster_name}-primary"
        
        env = {
            'MONGO_INITDB_ROOT_USERNAME': config['username'],
            'MONGO_INITDB_ROOT_PASSWORD': config['password'],
            'MONGO_INITDB_DATABASE': config['database']
        }
        
        primary = self.client.containers.run(
            config['image'],
            name=primary_name,
            environment=env,
            ports={'27017/tcp': None},
            network=self.network_name,
            detach=True
        )
        
        primary.reload()
        primary_port = primary.ports['27017/tcp'][0]['HostPort']
        
        return {
            'cluster_name': cluster_name,
            'type': 'mongodb',
            'primary': {
                'container_id': primary.id,
                'name': primary_name,
                'port': primary_port,
                'status': primary.status
            },
            'config': {
                'database': config['database'],
                'username': config['username'],
                'host': 'localhost',
                'port': primary_port
            }
        }


    def get_cluster_status(self, cluster_name: str) -> Dict[str, Any]:
        containers = self.client.containers.list(
            all=True,
            filters={'name': cluster_name}
        )
        
        if not containers:
            raise ValueError(f"No cluster found with name: {cluster_name}")
        
        status = {
            'cluster_name': cluster_name,
            'containers': []
        }
        
        for container in containers:
            status['containers'].append({
                'name': container.name,
                'status': container.status,
                'image': container.image.tags[0] if container.image.tags else 'unknown'
            })
        
        return status
    
    def get_admin_info(self, cluster_name: str) -> Dict[str, Any]:
        containers = self.client.containers.list(
            filters={'name': f"{cluster_name}-primary"}
        )
        
        if not containers:
            raise ValueError(f"No primary container found for cluster: {cluster_name}")
        
        container = containers[0]
        container.reload()
        
        #  Database type from image
        image_name = container.image.tags[0] if container.image.tags else ''
        
        if 'postgres' in image_name:
            port_key = '5432/tcp'
            db_type = 'postgresql'
        elif 'mysql' in image_name:
            port_key = '3306/tcp'
            db_type = 'mysql'
        elif 'mongo' in image_name:
            port_key = '27017/tcp'
            db_type = 'mongodb'
        else:
            raise ValueError(f"Unknown database type for cluster: {cluster_name}")
        
        if port_key in container.ports:
            host_port = container.ports[port_key][0]['HostPort']
        else:
            raise ValueError(f"Port not exposed for cluster: {cluster_name}")
        
        return {
            'cluster_name': cluster_name,
            'type': db_type,
            'host': 'localhost',
            'port': host_port,
            'container_name': container.name
        }


    def delete_cluster(self, cluster_name: str) -> Dict[str, Any]:
        containers = self.client.containers.list(
            all=True,
            filters={'name': cluster_name}
        )
        
        if not containers:
            raise ValueError(f"No cluster found with name: {cluster_name}")
        
        deleted_containers = []
        for container in containers:
            container.stop()
            container.remove()
            deleted_containers.append(container.name)
        
        return {
            'cluster_name': cluster_name,
            'deleted_containers': deleted_containers,
            'status': 'deleted'
        }

    def list_clusters(self) -> List[Dict[str, Any]]:
        """List all clusters"""
        containers = self.client.containers.list(all=True)
        clusters = {}
        
        for container in containers:
            parts = container.name.split('-')
            if len(parts) >= 2:
                cluster_name = '-'.join(parts[:-1])
                role = parts[-1]
                
                if cluster_name not in clusters:
                    clusters[cluster_name] = {
                        'name': cluster_name,
                        'containers': []
                    }
                
                clusters[cluster_name]['containers'].append({
                    'name': container.name,
                    'role': role,
                    'status': container.status,
                    'image': container.image.tags[0] if container.image.tags else 'unknown'
                })
        
        return list(clusters.values())
