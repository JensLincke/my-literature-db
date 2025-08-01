#!/usr/bin/env python3
import requests
import urllib.parse

BASE_URL = "http://localhost:9020"

# Test the exact cases
test_cases = [
    "W1492801337",
    "openalex:W1492801337", 
    "doi:10.1007/978-3-540-24614-5_17",
    "mag:1492801337"
]

print("Testing ID shortcut functionality via HTTP requests:")
print("=" * 60)

for test_id in test_cases:
    # Test both direct and URL-encoded versions
    for use_encoding in [False, True]:
        if use_encoding and ":" in test_id:
            # URL encode the ID
            encoded_id = urllib.parse.quote(test_id, safe='')
            url = f"{BASE_URL}/works/{encoded_id}"
            test_label = f"{test_id} (encoded: {encoded_id})"
        else:
            url = f"{BASE_URL}/works/{test_id}"
            test_label = test_id
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                title = data.get('title', 'No title')[:50]
                print(f"✓ {test_label}: {response.status_code} - {title}")
            else:
                print(f"✗ {test_label}: {response.status_code} - {response.text[:100]}")
        except Exception as e:
            print(f"✗ {test_label}: ERROR - {e}")

print("=" * 60)
