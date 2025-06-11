#!/usr/bin/env python3
"""
Inspect blob properties to determine what creation fields are available
"""
import sys
import argparse
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import config
import azure_client

def inspect_blob_properties(subscription_id, environment, container_name, blob_name):
    """Inspect detailed properties of a specific blob"""
    try:
        client = azure_client.get_blob_service_client(subscription_id, environment)
        blob_client = client.get_blob_client(container=container_name, blob=blob_name)
        
        # Get blob properties
        props = blob_client.get_blob_properties()
        
        print(f"🔍 Blob Properties for: {blob_name}")
        print("=" * 80)
        
        # Basic properties
        print(f"Name: {props.name}")
        print(f"Size: {props.size} bytes")
        print(f"Last Modified: {props.last_modified}")
        print(f"ETag: {props.etag}")
        print(f"Content Type: {props.content_settings.content_type if props.content_settings else 'Unknown'}")
        
        # Check for creation time
        if hasattr(props, 'creation_time'):
            print(f"Creation Time: {props.creation_time}")
        elif hasattr(props, 'created_on'):
            print(f"Created On: {props.created_on}")
        elif hasattr(props, 'created'):
            print(f"Created: {props.created}")
        else:
            print("❌ No creation time property found")
        
        # Check other time-related properties
        print("\n🕒 All time-related properties:")
        for attr in dir(props):
            if 'time' in attr.lower() or 'date' in attr.lower() or 'created' in attr.lower():
                try:
                    value = getattr(props, attr)
                    if not callable(value):
                        print(f"  {attr}: {value}")
                except:
                    print(f"  {attr}: <error accessing>")
        
        # Print all properties for debugging
        print("\n📋 All properties:")
        for attr in sorted(dir(props)):
            if not attr.startswith('_') and not callable(getattr(props, attr)):
                try:
                    value = getattr(props, attr)
                    print(f"  {attr}: {value}")
                except:
                    print(f"  {attr}: <error accessing>")
                    
    except Exception as e:
        print(f"❌ Error inspecting blob: {e}")

def find_recent_blob(subscription_id, environment, container_name, service_filter=None):
    """Find a recent blob to inspect"""
    try:
        blobs = azure_client.list_blobs(subscription_id, environment, container_name, "kubernetes/")
        if service_filter:
            blobs = [b for b in blobs if service_filter in b['name']]
        
        if not blobs:
            print("❌ No blobs found")
            return None
            
        # Sort by last modified, get most recent
        blobs.sort(key=lambda x: x['last_modified'], reverse=True)
        recent_blob = blobs[0]
        
        print(f"🎯 Found recent blob: {recent_blob['name']}")
        print(f"   Size: {recent_blob['size']} bytes")
        print(f"   Last Modified: {recent_blob['last_modified']}")
        
        return recent_blob['name']
        
    except Exception as e:
        print(f"❌ Error finding blob: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(
        description='Inspect Azure blob properties to find creation time fields',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  inspect-blob-properties --find-recent --service api
  inspect-blob-properties --blob kubernetes/20250611.api-xxx.gz
        """
    )
    
    parser.add_argument('--subscription', default='cp2', help='Subscription ID')
    parser.add_argument('--env', default='D1', help='Environment')
    parser.add_argument('--container', default='logs', help='Container name')
    parser.add_argument('--blob', help='Specific blob name to inspect')
    parser.add_argument('--find-recent', action='store_true', help='Find and inspect most recent blob')
    parser.add_argument('--service', help='Filter by service name when finding recent blob')
    
    args = parser.parse_args()
    
    if not config.has_credentials():
        print("❌ No configuration found. Run setup-config first.")
        return
    
    if args.blob:
        inspect_blob_properties(args.subscription, args.env, args.container, args.blob)
    elif args.find_recent:
        blob_name = find_recent_blob(args.subscription, args.env, args.container, args.service)
        if blob_name:
            print()
            inspect_blob_properties(args.subscription, args.env, args.container, blob_name)
    else:
        print("❌ Either --blob or --find-recent is required")

if __name__ == '__main__':
    main() 