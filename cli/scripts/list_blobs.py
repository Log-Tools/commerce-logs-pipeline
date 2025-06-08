#!/usr/bin/env python3
"""
List blobs from Azure storage containers across all configured environments
"""
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import config
import azure_client

def format_size(size_bytes):
    """Format size in bytes to human readable format"""
    if size_bytes is None:
        return "Unknown"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

def list_blobs_for_environment(subscription_id, environment, container_name, prefix=None, max_blobs=None):
    """List blobs for a specific environment with optional filtering"""
    print(f"\n🌍 Environment: {subscription_id}/{environment}")
    print(f"📦 Container: {container_name}")
    if prefix:
        print(f"🔍 Filter: {prefix}")
    print("-" * 50)
    
    try:
        blobs = azure_client.list_blobs(subscription_id, environment, container_name, prefix)
        
        if not blobs:
            print("   📁 No blobs found")
            return 0
        
        # Sort by last modified (newest first)
        blobs.sort(key=lambda x: x['last_modified'], reverse=True)
        
        # Limit results if specified
        if max_blobs and len(blobs) > max_blobs:
            total_count = len(blobs)
            blobs = blobs[:max_blobs]
            print(f"   📋 Showing {max_blobs} most recent of {total_count} total blobs")
        else:
            print(f"   📋 Found {len(blobs)} blobs:")
        
        for blob in blobs:
            # Format last modified date
            mod_date = blob['last_modified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            size_str = format_size(blob['size'])
            
            print(f"   📄 {blob['name']}")
            print(f"      📅 Modified: {mod_date}")
            print(f"      📏 Size: {size_str}")
            if blob['content_type']:
                print(f"      🏷️  Type: {blob['content_type']}")
            print()
        
        return len(blobs)
        
    except Exception as e:
        print(f"   ✗ Error listing blobs: {type(e).__name__}: {e}")
        return 0

def list_all_blobs(prefix=None, max_blobs_per_env=None):
    """List blobs from all configured environments"""
    print("📂 Commerce Logs Pipeline - Blob Listing")
    print("=" * 60)
    
    if not config.has_credentials():
        print("✗ No configuration found. Run setup-config first.")
        return
    
    total_blobs = 0
    subs = config.list_subscriptions()
    
    for sub in subs:
        envs = config.list_environments(sub)
        
        for env in envs:
            # Test connection first
            success, message = azure_client.test_connection(sub, env)
            if not success:
                print(f"\n🌍 Environment: {sub}/{env}")
                print(f"   ✗ {message}")
                continue
            
            # List containers and their blobs
            containers = azure_client.list_containers(sub, env)
            
            for container in containers:
                blob_count = list_blobs_for_environment(
                    sub, env, container['name'], prefix, max_blobs_per_env
                )
                total_blobs += blob_count
    
    print("=" * 60)
    print(f"🎯 Total blobs processed: {total_blobs}")

def main():
    parser = argparse.ArgumentParser(
        description='List blobs from commerce cloud storage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  list-blobs                                    # List all blobs
  list-blobs --env P1                          # List for specific environment
  list-blobs --date 20250601                   # Filter by date
  list-blobs --service backgroundprocessing    # Filter by service
  list-blobs --max-per-env 10                  # Limit results
        """
    )
    
    parser.add_argument('--prefix', help='Custom filter prefix')
    parser.add_argument('--date', help='Filter by date (YYYYMMDD format, e.g., 20250601)')
    parser.add_argument('--service', help='Filter by service name (e.g., backgroundprocessing, backoffice, apache2)')
    parser.add_argument('--max-per-env', type=int, help='Maximum blobs to show per environment')
    parser.add_argument('--env', help='Specific environment (e.g., D1, S1, P1)')
    parser.add_argument('--subscription', default='cp2', help='Subscription ID (default: cp2)')
    
    args = parser.parse_args()
    
    # Build efficient prefix filter
    filter_prefix = None
    if args.prefix:
        filter_prefix = args.prefix
    elif args.date or args.service:
        filter_prefix = "kubernetes/"
        if args.date:
            filter_prefix += args.date
            if args.service:
                filter_prefix += f".{args.service}"
        elif args.service:
            # No date specified, but service specified - this is less efficient
            print("⚠️  Warning: Filtering by service without date is less efficient")
            filter_prefix = "kubernetes/"
    
    if args.env:
        # List blobs for specific environment
        title = f"📂 Commerce Logs Pipeline - Blob Listing ({args.subscription}/{args.env})"
        if filter_prefix:
            title += f" [Filter: {filter_prefix}]"
        print(title)
        print("=" * 60)
        
        success, message = azure_client.test_connection(args.subscription, args.env)
        if not success:
            print(f"✗ {message}")
            return
        
        containers = azure_client.list_containers(args.subscription, args.env)
        total_blobs = 0
        
        for container in containers:
            # Apply additional service filtering if needed
            if args.service and not args.date:
                # Need to filter by service name in blob names
                blobs = azure_client.list_blobs(args.subscription, args.env, container['name'], filter_prefix)
                service_blobs = [b for b in blobs if f".{args.service}" in b['name']]
                # Manually handle the filtering and display
                print(f"\n🌍 Environment: {args.subscription}/{args.env}")
                print(f"📦 Container: {container['name']}")
                print(f"🔍 Filter: Service '{args.service}'")
                print("-" * 50)
                
                if not service_blobs:
                    print("   📁 No blobs found")
                    continue
                
                # Sort by last modified (newest first)
                service_blobs.sort(key=lambda x: x['last_modified'], reverse=True)
                
                # Limit results if specified
                if args.max_per_env and len(service_blobs) > args.max_per_env:
                    total_count = len(service_blobs)
                    service_blobs = service_blobs[:args.max_per_env]
                    print(f"   📋 Showing {args.max_per_env} most recent of {total_count} total blobs")
                else:
                    print(f"   📋 Found {len(service_blobs)} blobs:")
                
                for blob in service_blobs:
                    mod_date = blob['last_modified'].strftime('%Y-%m-%d %H:%M:%S UTC')
                    size_str = format_size(blob['size'])
                    
                    print(f"   📄 {blob['name']}")
                    print(f"      📅 Modified: {mod_date}")
                    print(f"      📏 Size: {size_str}")
                    if blob['content_type']:
                        print(f"      🏷️  Type: {blob['content_type']}")
                    print()
                
                total_blobs += len(service_blobs)
            else:
                blob_count = list_blobs_for_environment(
                    args.subscription, args.env, container['name'], filter_prefix, args.max_per_env
                )
                total_blobs += blob_count
        
        print("=" * 60)
        print(f"🎯 Total blobs processed: {total_blobs}")
    else:
        # List blobs for all environments
        list_all_blobs(filter_prefix, args.max_per_env)

if __name__ == '__main__':
    main() 