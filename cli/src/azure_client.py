#!/usr/bin/env python3
"""
Azure Blob Storage client for Commerce Logs Pipeline operations
"""
from azure.storage.blob import BlobServiceClient
import config
import logging

def get_blob_service_client(subscription_id, environment):
    """Get authenticated Azure Blob Service client"""
    storage_config = config.get_storage_account(subscription_id, environment)
    if not storage_config:
        raise ValueError(f"No storage configuration found for {subscription_id}/{environment}")
    
    account_name = storage_config['account_name']
    access_key = storage_config['access_key']
    
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=access_key)

def test_connection(subscription_id, environment):
    """Test connection to Azure storage"""
    try:
        client = get_blob_service_client(subscription_id, environment)
        # Try to list containers as a connection test
        containers = list(client.list_containers())
        if len(containers) > 1:
            containers = containers[:1]  # Limit to 1 for testing
        return True, "Connection successful"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

def list_containers(subscription_id, environment):
    """List all containers in the storage account"""
    try:
        client = get_blob_service_client(subscription_id, environment)
        containers = []
        for container in client.list_containers():
            containers.append({
                'name': container.name,
                'last_modified': container.last_modified,
                'public_access': getattr(container, 'public_access', None)
            })
        return containers
    except Exception as e:
        logging.error(f"Failed to list containers: {e}")
        return []

def list_blobs(subscription_id, environment, container_name, prefix=None):
    """List blobs in a container with optional prefix filter"""
    try:
        client = get_blob_service_client(subscription_id, environment)
        container_client = client.get_container_client(container_name)
        
        blobs = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            content_type = getattr(blob, 'content_type', None)
            if not content_type:
                content_type = getattr(getattr(blob, 'content_settings', None), 'content_type', None)

            blobs.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified,
                'content_type': content_type,
                'etag': blob.etag
            })
        return blobs
    except Exception as e:
        logging.error(f"Failed to list blobs: {e}")
        return [] 