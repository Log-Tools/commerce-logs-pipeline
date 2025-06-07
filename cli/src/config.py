#!/usr/bin/env python3
"""
Configuration loader for secure credential handling
"""
import yaml
import os
from pathlib import Path

def mask_credential(value, visible_chars=4):
    """Mask credential keeping only first few characters visible"""
    if not value or len(value) <= visible_chars:
        return "*" * 8
    return value[:visible_chars] + "*" * (len(value) - visible_chars)

def get_config_paths():
    """Get possible config file locations, starting with most secure"""
    paths = [
        # Outside workspace - most secure
        Path.home() / ".commerce-logs-pipeline" / "config.yaml",
        Path.home() / ".config" / "commerce-logs-pipeline" / "config.yaml", 
        # Environment variable override
        os.environ.get('COMMERCE_LOGS_CONFIG'),
        # Local (less secure, but convenient for dev)
        "config.yaml"
    ]
    return [p for p in paths if p is not None]

def load_config(config_path=None):
    """Load configuration from YAML file, return None if not found"""
    if config_path:
        paths = [config_path]
    else:
        paths = get_config_paths()
    
    for path in paths:
        if path and os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    config = yaml.safe_load(f)
                    print(f"✓ Loaded config from: {path}")
                    return config
            except Exception as e:
                print(f"Error loading config from {path}: {e}")
                continue
    
    print("✗ No valid configuration found")
    print("Try one of these locations:")
    for path in get_config_paths()[:3]:  # Skip the local one in suggestions
        print(f"  {path}")
    return None

def has_credentials():
    """Check if credentials are configured"""
    config = load_config()
    return config is not None

def list_subscriptions():
    """List available subscriptions"""
    config = load_config()
    if not config or 'subscriptions' not in config:
        return []
    
    return list(config['subscriptions'].keys())

def list_environments(subscription_id):
    """List environments for a subscription"""
    config = load_config()
    if not config or 'subscriptions' not in config:
        return []
    
    sub = config['subscriptions'].get(subscription_id, {})
    return list(sub.get('environments', {}).keys())

def get_storage_account(subscription_id, environment):
    """Get storage account details for an environment - WITH CREDENTIALS"""
    config = load_config()
    if not config:
        return None
    
    try:
        env_config = config['subscriptions'][subscription_id]['environments'][environment]
        return env_config['storage_account']
    except KeyError:
        return None

def get_storage_account_safe(subscription_id, environment):
    """Get storage account details with masked credentials for safe display"""
    storage = get_storage_account(subscription_id, environment)
    if not storage:
        return None
    
    # Create a copy with masked credentials
    safe_storage = storage.copy()
    if 'access_key' in safe_storage:
        safe_storage['access_key'] = mask_credential(safe_storage['access_key'])
    if 'connection_string' in safe_storage:
        safe_storage['connection_string'] = mask_credential(safe_storage['connection_string'])
    
    return safe_storage

def test_config_safe():
    """Test configuration without exposing credentials"""
    print("Configuration Test (Safe Mode)")
    print("=" * 40)
    
    if not has_credentials():
        print("✗ No credentials found")
        return False
    
    subs = list_subscriptions()
    print(f"✓ Found {len(subs)} subscription(s): {subs}")
    
    for sub in subs:
        envs = list_environments(sub)
        print(f"✓ Subscription '{sub}' has {len(envs)} environment(s): {envs}")
        
        for env in envs:
            storage = get_storage_account_safe(sub, env)
            if storage:
                print(f"  ✓ {sub}/{env}: {storage['account_name']} (key: {storage['access_key']})")
            else:
                print(f"  ✗ {sub}/{env}: No storage account found")
    
    return True

def validate_config():
    """Validate configuration structure without exposing credentials"""
    config = load_config()
    if not config:
        return False, "No configuration found"
    
    issues = []
    
    if 'subscriptions' not in config:
        issues.append("Missing 'subscriptions' section")
        return False, issues
    
    for sub_id, sub_config in config['subscriptions'].items():
        if 'environments' not in sub_config:
            issues.append(f"Subscription '{sub_id}' missing 'environments'")
            continue
            
        for env_id, env_config in sub_config['environments'].items():
            if 'storage_account' not in env_config:
                issues.append(f"{sub_id}/{env_id} missing 'storage_account'")
                continue
                
            storage = env_config['storage_account']
            if 'account_name' not in storage:
                issues.append(f"{sub_id}/{env_id} missing 'account_name'")
            if 'access_key' not in storage:
                issues.append(f"{sub_id}/{env_id} missing 'access_key'")
    
    if issues:
        return False, issues
    else:
        return True, "Configuration is valid" 