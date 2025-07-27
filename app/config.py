import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    DEBUG = False
    TESTING = False
    
    DOCKER_NETWORK = os.environ.get('DOCKER_NETWORK', 'autoprovisioner-network')
    
    DATABASE_CONFIGS = {
        'postgresql': {
            'image': os.environ.get('POSTGRES_IMAGE', 'postgres:15'),
            'port': int(os.environ.get('POSTGRES_PORT', '5432')),
            'database': os.environ.get('POSTGRES_DB', 'postgres'),
            'username': os.environ.get('POSTGRES_USER', 'postgres'),
            'password': os.environ.get('POSTGRES_PASSWORD', 'postgres123'),
            'replication_user': os.environ.get('POSTGRES_REPLICATION_USER', 'replicator'),
            'replication_password': os.environ.get('POSTGRES_REPLICATION_PASSWORD', 'replicator123'),
            'ha_enabled': os.environ.get('POSTGRES_HA_ENABLED', 'false').lower() == 'true'
        },
        'mysql': {
            'image': os.environ.get('MYSQL_IMAGE', 'mysql:8.0'),
            'port': int(os.environ.get('MYSQL_PORT', '3306')),
            'database': os.environ.get('MYSQL_DATABASE', 'testdb'),
            'username': os.environ.get('MYSQL_USER', 'mysql'),
            'password': os.environ.get('MYSQL_PASSWORD', 'mysql123'),
            'ha_enabled': os.environ.get('MYSQL_HA_ENABLED', 'false').lower() == 'true'
        },
        'mongodb': {
            'image': os.environ.get('MONGO_IMAGE', 'mongo:7'),
            'port': int(os.environ.get('MONGO_PORT', '27017')),
            'database': os.environ.get('MONGO_DATABASE', 'testdb'),
            'username': os.environ.get('MONGO_USER', 'mongo'),
            'password': os.environ.get('MONGO_PASSWORD', 'mongo123'),
            'ha_enabled': os.environ.get('MONGO_HA_ENABLED', 'false').lower() == 'true'
        }
    }


class DevelopmentConfig(Config):
    DEBUG = True
    SECRET_KEY = 'dev-secret-key'


class ProductionConfig(Config):
    DEBUG = False
    SECRET_KEY = os.environ.get('SECRET_KEY')


class TestingConfig(Config):
    TESTING = True
    DEBUG = True
    SECRET_KEY = 'test-secret-key'
