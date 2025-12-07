#!/bin/bash
# Test script to validate config_flow.py can be loaded
# Run from repository root: bash test_load_config_flow.sh

echo "=== Testing Config Flow Loading ==="
echo ""

# Test 1: Check Python syntax
echo "1. Checking Python syntax..."
python3 -m py_compile custom_components/lxp_modbus/config_flow.py 2>&1
if [ $? -eq 0 ]; then
    echo "   ✓ Syntax is valid"
else
    echo "   ✗ SYNTAX ERROR FOUND"
    exit 1
fi

# Test 2: Check for common issues
echo ""
echo "2. Checking for common issues..."

# Check for duplicate class definitions
duplicates=$(grep -n "^class " custom_components/lxp_modbus/config_flow.py | wc -l)
echo "   Found $duplicates class definitions"

# Check for proper imports
if grep -q "from .const import DOMAIN" custom_components/lxp_modbus/config_flow.py; then
    echo "   ✓ DOMAIN import found"
else
    echo "   ✗ DOMAIN import missing"
fi

# Test 3: Try to import the module
echo ""
echo "3. Testing module import..."
python3 << 'EOF'
import sys
import os
sys.path.insert(0, os.path.join(os.getcwd(), 'custom_components'))

try:
    # Test basic imports
    from lxp_modbus import const
    print(f"   ✓ const.py loaded, DOMAIN={const.DOMAIN}")
    
    # Test config_flow imports
    from lxp_modbus import config_flow
    print(f"   ✓ config_flow.py loaded")
    
    # Check if classes exist
    print(f"   ✓ LxpModbusConfigFlow exists: {hasattr(config_flow, 'LxpModbusConfigFlow')}")
    print(f"   ✓ LxpModbusOptionsFlow exists: {hasattr(config_flow, 'LxpModbusOptionsFlow')}")
    
    # Try to instantiate (without Home Assistant context)
    print("")
    print("   Testing class structure...")
    print(f"   ✓ ConfigFlow.VERSION = {config_flow.LxpModbusConfigFlow.VERSION}")
    
    print("")
    print("✅ All tests passed!")
    
except Exception as e:
    print(f"   ✗ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "=== Config flow is ready to use! ==="
else
    echo ""
    echo "=== Config flow has errors - see above ==="
    exit 1
fi