import os
from flask import Flask
from dotenv import load_dotenv


def create_app(config_name=None):
    load_dotenv()
    
    app = Flask(__name__)
    
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    from app.config import DevelopmentConfig, ProductionConfig, TestingConfig
    
    config = {
        'development': DevelopmentConfig,
        'production': ProductionConfig,
        'testing': TestingConfig,
        'default': DevelopmentConfig
    }
    
    app.config.from_object(config.get(config_name, config['default']))
    
    from app.api.cluster import cluster_bp
    from app.api.health import health_bp
    
    app.register_blueprint(health_bp)
    app.register_blueprint(cluster_bp, url_prefix='/api/v1')
    
    return app
