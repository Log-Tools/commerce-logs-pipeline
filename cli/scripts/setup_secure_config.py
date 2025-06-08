#!/usr/bin/env python3
"""
Setup script to move config to secure location outside workspace
"""
import os
import shutil
from pathlib import Path
import sys

def setup_secure_config():
    """Move config to secure location outside workspace"""
    
    # Secure location
    secure_dir = Path.home() / ".commerce-logs-pipeline"
    secure_config = secure_dir / "config.yaml"
    local_config = Path("config.yaml")
    
    print("Commerce Logs Pipeline - Secure Configuration Setup")
    print("=" * 60)
    
    # Create secure directory
    secure_dir.mkdir(exist_ok=True)
    print(f"✓ Created secure directory: {secure_dir}")
    
    # SAFETY CHECK: If secure config already exists, be very careful
    if secure_config.exists():
        print(f"⚠️  EXISTING CONFIG FOUND: {secure_config}")
        print("⚠️  This may contain your real credentials!")
        print()
        
        # Check if it's a template (has placeholder values)
        try:
            with open(secure_config, 'r') as f:
                content = f.read()
                is_template = 'your_access_key_here' in content or 'your_storage_account_here' in content
        except:
            is_template = False
        
        if is_template:
            print("🔍 Detected template values in existing config")
            response = input("Replace template with new template? (y/N): ")
            if response.lower() != 'y':
                print("Setup cancelled. Existing config preserved.")
                return
        else:
            print("🔐 Detected real credentials in existing config")
            print("❌ REFUSING to overwrite existing config with real credentials")
            print("   If you need to reset, please manually delete:")
            print(f"   rm {secure_config}")
            print("   Then run this script again.")
            return
    
    # Check if local config exists
    if local_config.exists():
        print(f"Found local config: {local_config}")
        
        # Copy config
        shutil.copy2(local_config, secure_config)
        print(f"✓ Copied config to: {secure_config}")
        
        # Set secure permissions
        os.chmod(secure_config, 0o600)
        print("✓ Set secure permissions (600)")
        
        # Remove local config
        response = input(f"Remove local config.yaml? (recommended for security) (y/n): ")
        if response.lower() == 'y':
            local_config.unlink()
            print("✓ Removed local config.yaml")
        else:
            print("⚠ Local config.yaml still exists - may be read by development tools")
    
    else:
        # Copy template from CLI config directory
        template = Path(__file__).parent.parent / "config" / "config_template.yaml"
        if template.exists():
            shutil.copy2(template, secure_config)
            print(f"✓ Copied template to: {secure_config}")
            print(f"⚠ Please edit {secure_config} with your actual credentials")
        else:
            # Create a basic template
            template_content = """subscriptions:
  cp2:
    name: "Commerce Cloud Subscription"
    environments:
      P1:
        name: "Production Environment 1"
        storage_account:
          account_name: "your_storage_account_here"
          access_key: "your_access_key_here"
      D1:
        name: "Development Environment 1"
        storage_account:
          account_name: "your_storage_account_here"
          access_key: "your_access_key_here"
"""
            with open(secure_config, 'w') as f:
                f.write(template_content)
            print(f"✓ Created template at: {secure_config}")
            print(f"⚠ Please edit {secure_config} with your actual credentials")
        
        # Set secure permissions
        os.chmod(secure_config, 0o600)
        print("✓ Set secure permissions (600)")
    
    print("\nSetup complete!")
    print(f"Config location: {secure_config}")
    print("This location is outside your workspace and secure.")

def check_existing_config():
    """Check status of existing config without making changes"""
    secure_dir = Path.home() / ".commerce-logs-pipeline"
    secure_config = secure_dir / "config.yaml"
    
    print("Commerce Logs Pipeline - Configuration Status")
    print("=" * 50)
    
    if not secure_config.exists():
        print("❌ No configuration found")
        print(f"   Expected location: {secure_config}")
        print("   Run setup to create initial config")
        return
    
    # Check if it's a template
    try:
        with open(secure_config, 'r') as f:
            content = f.read()
            is_template = 'your_access_key_here' in content or 'your_storage_account_here' in content
            
        if is_template:
            print("📝 Template configuration found")
            print("   Contains placeholder values - needs real credentials")
        else:
            print("🔐 Real configuration found")
            print("   Contains actual credentials - ready for use")
            
        print(f"   Location: {secure_config}")
        
        # Check permissions
        stat = os.stat(secure_config)
        perms = oct(stat.st_mode)[-3:]
        if perms == '600':
            print(f"✓ Secure permissions: {perms}")
        else:
            print(f"⚠ Insecure permissions: {perms} (should be 600)")
            
    except Exception as e:
        print(f"❌ Error reading config: {e}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--check':
        check_existing_config()
    else:
        setup_secure_config()

if __name__ == '__main__':
    main() 