#!/usr/bin/env python3
"""
Test specific DOI variations to understand FastAPI routing
"""
import requests
import urllib.parse

BASE_URL = "http://localhost:9020"

# Test different DOI encoding strategies
test_cases = [
    # Original (should fail due to path separator)
    "doi:10.1007/978-3-540-24614-5_17",
    
    # URL encode the entire string
    urllib.parse.quote("doi:10.1007/978-3-540-24614-5_17"),
    
    # URL encode just the colon
    "doi%3A10.1007/978-3-540-24614-5_17",
    
    # URL encode just the slash
    "doi:10.1007%2F978-3-540-24614-5_17",
    
    # URL encode both colon and slash
    "doi%3A10.1007%2F978-3-540-24614-5_17",
    
    # Double encode (what browser might do)
    urllib.parse.quote(urllib.parse.quote("doi:10.1007/978-3-540-24614-5_17")),
]

print("=== Testing DOI Encoding Variations ===")
for i, test_case in enumerate(test_cases):
    url = f"{BASE_URL}/works/{test_case}"
    try:
        response = requests.get(url, timeout=10)
        print(f"{i+1}. {test_case} -> {response.status_code}")
        if response.status_code != 200:
            print(f"   Response: {response.text[:100]}...")
    except Exception as e:
        print(f"{i+1}. {test_case} -> Error: {e}")

print("\n=== Working Cases for Comparison ===")
working_cases = ["W1492801337", "openalex:W1492801337"]
for case in working_cases:
    url = f"{BASE_URL}/works/{case}"
    try:
        response = requests.get(url, timeout=5)
        print(f"✓ {case} -> {response.status_code}")
    except Exception as e:
        print(f"✗ {case} -> Error: {e}")
