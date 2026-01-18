#!/usr/bin/env python3
"""Test Botasaurus API"""

from botasaurus import Request

# Test 1: Basic request
print("Testing Botasaurus Request...")
try:
    req = Request()
    result = req.get('https://httpbin.org/html')
    print(f"Success! Got {len(result.text)} characters")
    print(f"First 200 chars: {result.text[:200]}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
