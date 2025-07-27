from flask import Blueprint, request, jsonify
from app.services.cluster_service import ClusterService

cluster_bp = Blueprint('cluster', __name__)


@cluster_bp.route('/list')
def list_clusters():
    """List all clusters"""
    try:
        cluster_service = ClusterService()
        clusters = cluster_service.list_clusters()
        return jsonify({'clusters': clusters})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



@cluster_bp.route('/provision', methods=['POST'])
def provision_cluster():
    """Provision a new cluster"""
    try:
        cluster_service = ClusterService()
        data = request.get_json()
        cluster_type = data.get('type', 'postgresql')
        cluster_name = data.get('name', f"{cluster_type}-cluster")
        config = data.get('config', {})
        
        result = cluster_service.provision_cluster(cluster_type, cluster_name, config)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@cluster_bp.route('/status/<cluster_name>')
def get_cluster_status(cluster_name):
    """Get status of a specific cluster"""
    try:
        cluster_service = ClusterService()
        status = cluster_service.get_cluster_status(cluster_name)
        return jsonify(status)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@cluster_bp.route('/admin/<cluster_name>')
def get_cluster_admin(cluster_name):
    """Get admin connection info for a cluster"""
    try:
        cluster_service = ClusterService()
        admin_info = cluster_service.get_admin_info(cluster_name)
        return jsonify(admin_info)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@cluster_bp.route('/delete/<cluster_name>', methods=['DELETE'])
def delete_cluster(cluster_name):
    """Delete a database cluster"""
    try:
        cluster_service = ClusterService()
        result = cluster_service.delete_cluster(cluster_name)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

