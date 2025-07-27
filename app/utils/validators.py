import re
from typing import Dict, Any, List


def validate_cluster_name(name: str) -> bool:
    if not name or len(name) < 3 or len(name) > 50:
        return False
    # Allow alphanumeric and hyphens, but not starting/ending with hyphen
    pattern = r'^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$'
    return bool(re.match(pattern, name))


def validate_cluster_config(config: Dict[str, Any], cluster_type: str) -> List[str]:
    errors = []
    
    required_fields = ['database', 'username', 'password']
    for field in required_fields:
        if not config.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Database name validation
    if config.get('database'):
        db_name = config['database']
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', db_name):
            errors.append("Database name must start with letter and contain only alphanumeric characters and underscores")
    
    # Username validation
    if config.get('username'):
        username = config['username']
        if len(username) < 3 or len(username) > 32:
            errors.append("Username must be between 3 and 32 characters")
    
    # Password validation
    if config.get('password'):
        password = config['password']
        if len(password) < 8:
            errors.append("Password must be at least 8 characters long")
    
    return errors


def validate_port_available(port: int) -> bool:
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', port))
            return True
    except OSError:
        return False
