#!/usr/bin/env python3
"""
Comprehensive integration validation script.
Run from repository root: python3 verify_integration.py
"""

import json
import os
import sys

def check_file_exists(filepath):
    """Check if a file exists."""
    exists = os.path.exists(filepath)
    symbol = "✓" if exists else "✗"
    print(f"  {symbol} {filepath}")
    return exists

def check_json_valid(filepath):
    """Check if JSON file is valid."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        print(f"  ✓ {filepath} is valid JSON")
        return True, data
    except Exception as e:
        print(f"  ✗ {filepath} has JSON error: {e}")
        return False, None

def main():
    print("=== Luxpower Modbus Integration Validation ===\n")
    
    base_path = "custom_components/lxp_modbus"
    errors = []
    
    # Check 1: Required files exist
    print("1. Checking required files...")
    required_files = [
        f"{base_path}/__init__.py",
        f"{base_path}/manifest.json",
        f"{base_path}/const.py",
        f"{base_path}/config_flow.py",
        f"{base_path}/strings.json",
        f"{base_path}/translations/en.json",
    ]
    
    for filepath in required_files:
        if not check_file_exists(filepath):
            errors.append(f"Missing required file: {filepath}")
    
    # Check 2: Validate JSON files
    print("\n2. Validating JSON files...")
    
    # Check manifest.json
    valid, manifest = check_json_valid(f"{base_path}/manifest.json")
    if valid:
        print(f"     Domain: {manifest.get('domain')}")
        print(f"     Name: {manifest.get('name')}")
        print(f"     Version: {manifest.get('version')}")
        
        if manifest.get('domain') != 'lxp_modbus':
            errors.append(f"Domain mismatch in manifest.json: {manifest.get('domain')}")
    
    # Check strings.json
    valid, strings = check_json_valid(f"{base_path}/strings.json")
    if valid:
        # Check for required keys
        if 'config' in strings and 'step' in strings['config']:
            steps = strings['config']['step'].keys()
            print(f"     Config steps: {', '.join(steps)}")
            
            required_steps = ['user', 'tcp', 'rtu']
            for step in required_steps:
                if step not in steps:
                    errors.append(f"Missing config step in strings.json: {step}")
    
    # Check 3: Validate Python syntax
    print("\n3. Checking Python syntax...")
    python_files = [
        f"{base_path}/__init__.py",
        f"{base_path}/const.py",
        f"{base_path}/config_flow.py",
    ]
    
    for filepath in python_files:
        result = os.system(f"python3 -m py_compile {filepath} 2>/dev/null")
        if result == 0:
            print(f"  ✓ {filepath}")
        else:
            print(f"  ✗ {filepath} has syntax errors")
            errors.append(f"Syntax error in {filepath}")
    
    # Check 4: Validate imports
    print("\n4. Testing imports...")
    sys.path.insert(0, 'custom_components')
    
    try:
        from lxp_modbus import const
        print(f"  ✓ const.py imports successfully")
        print(f"     DOMAIN = '{const.DOMAIN}'")
        
        # Check domain consistency
        if valid and manifest.get('domain') != const.DOMAIN:
            errors.append(f"Domain mismatch: manifest.json has '{manifest.get('domain')}' but const.py has '{const.DOMAIN}'")
    except Exception as e:
        print(f"  ✗ Failed to import const.py: {e}")
        errors.append(f"Cannot import const.py: {e}")
    
    try:
        from lxp_modbus import config_flow
        print(f"  ✓ config_flow.py imports successfully")
        
        # Check classes exist
        if hasattr(config_flow, 'LxpModbusConfigFlow'):
            print(f"  ✓ LxpModbusConfigFlow class found")
        else:
            errors.append("LxpModbusConfigFlow class not found")
            
        if hasattr(config_flow, 'LxpModbusOptionsFlow'):
            print(f"  ✓ LxpModbusOptionsFlow class found")
        else:
            errors.append("LxpModbusOptionsFlow class not found")
            
    except Exception as e:
        print(f"  ✗ Failed to import config_flow.py: {e}")
        errors.append(f"Cannot import config_flow.py: {e}")
        import traceback
        traceback.print_exc()
    
    # Summary
    print("\n" + "="*50)
    if errors:
        print("❌ VALIDATION FAILED")
        print("\nErrors found:")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
        return 1
    else:
        print("✅ VALIDATION PASSED")
        print("\nIntegration is ready to use in Home Assistant!")
        return 0

if __name__ == "__main__":
    sys.exit(main())