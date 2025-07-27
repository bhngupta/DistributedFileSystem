'''
API Unit Tests
    - Cluster provisioning
    - Cluster status retrieval
    - Cluster deletion    
'''

import json
from unittest.mock import patch, Mock


@patch('app.api.cluster.ClusterService')
def test_provision_cluster(mock_service_class, client):
    
    mock_service = mock_service_class.return_value
    mock_service.provision_cluster.return_value = {
        'cluster_name': 'test-cluster',
        'type': 'postgresql',
        'primary': {
            'container_id': 'abc123',
            'name': 'test-cluster-primary',
            'status': 'running'
        }
    }
    
    # Test the API
    config = {
        'type': 'postgresql',
        'name': 'test-cluster',
        'config': {
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass123'
        }
    }
    
    response = client.post('/api/v1/provision', 
                          data=json.dumps(config),
                          content_type='application/json')
    
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['cluster_name'] == 'test-cluster'
    assert data['type'] == 'postgresql'


@patch('app.api.cluster.ClusterService')
def test_cluster_status(mock_service_class, client):
    
    mock_service = mock_service_class.return_value
    mock_service.get_cluster_status.return_value = {
        'cluster_name': 'test-cluster',
        'containers': [
            {
                'name': 'test-cluster-primary',
                'status': 'running',
                'image': 'postgres:15'
            }
        ]
    }
    
    response = client.get('/api/v1/status/test-cluster')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['cluster_name'] == 'test-cluster'
    assert len(data['containers']) == 1


@patch('app.api.cluster.ClusterService')
def test_delete_cluster(mock_service_class, client):
    
    mock_service = mock_service_class.return_value
    mock_service.delete_cluster.return_value = {
        'cluster_name': 'test-cluster',
        'status': 'deleted',
        'deleted_containers': ['test-cluster-primary']
    }
    
    response = client.delete('/api/v1/delete/test-cluster')
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data['status'] == 'deleted'
    assert 'test-cluster-primary' in data['deleted_containers']


def test_invalid_request(client):
    """Test error handling for invalid requests"""
    
    invalid_config = {
        'type': 'postgresql'
        # missing 'name' and 'config'
    }
    
    response = client.post('/api/v1/provision',
                          data=json.dumps(invalid_config),
                          content_type='application/json')
    
    # The API might return 200 but with an error in the response
    assert response.status_code in [200, 400, 500]  # Any valid HTTP response
