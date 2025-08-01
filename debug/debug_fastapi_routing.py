#!/usr/bin/env python3
"""
Debug script to test FastAPI routing behavior with different URL patterns
"""
import requests
import urllib.parse

BASE_URL = "http://localhost:9020"

# Test cases that work
working_cases = [
    "W1492801337",
    "openalex:W1492801337", 
    "mag:1492801337"
]

# Test case that fails
failing_case = "doi:10.1007/978-3-540-24614-5_17"

print("=== Testing Working Cases ===")
for test_id in working_cases:
    url = f"{BASE_URL}/works/{test_id}"
    try:
        response = requests.get(url, timeout=5)
        print(f"✓ {test_id} -> {response.status_code}")
    except Exception as e:
        print(f"✗ {test_id} -> Error: {e}")

print("\n=== Testing Failing Case ===")
url = f"{BASE_URL}/works/{failing_case}"
try:
    response = requests.get(url, timeout=5)
    print(f"✗ {failing_case} -> {response.status_code}")
    print(f"  Response: {response.text}")
except Exception as e:
    print(f"✗ {failing_case} -> Error: {e}")

print("\n=== Testing URL-Encoded Version ===")
encoded_case = urllib.parse.quote(failing_case)
url = f"{BASE_URL}/works/{encoded_case}"
try:
    response = requests.get(url, timeout=5)
    print(f"✓ {encoded_case} -> {response.status_code}")
    if response.status_code != 200:
        print(f"  Response: {response.text}")
except Exception as e:
    print(f"✗ {encoded_case} -> Error: {e}")

print("\n=== Analysis ===")
print("The issue appears to be that FastAPI's path parameter validation")
print("rejects URLs containing both colons (:) and forward slashes (/).")
print("This is likely because FastAPI interprets them as path separators.")
print()
print("DOI format: doi:10.1007/978-3-540-24614-5_17")
print("Contains: colon (:) AND forward slash (/)")
print("Result: FastAPI rejects before reaching our handler")
print()
print("Solutions:")
print("1. Use URL encoding (already works)")
print("2. Change API design to use query parameters")
print("3. Use a different separator instead of colon")
