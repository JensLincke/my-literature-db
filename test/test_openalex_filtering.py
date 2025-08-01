#!/usr/bin/env python3
"""
Test script to verify OpenAlex ID filtering works correctly
"""
import sys
import os

# Add the bin directory to the Python path
project_root = os.path.dirname(__file__)
bin_path = os.path.join(project_root, 'bin')
sys.path.insert(0, bin_path)

from filter_utils import parse_filter_param, build_mongodb_query

def test_short_id_filtering():
    """Test that short OpenAlex IDs are converted to full URLs"""
    
    # Test individual build_mongodb_query function
    print("Testing build_mongodb_query function:")
    result = build_mongodb_query("ids.openalex", "eq", "W1491178396")
    expected = {"ids.openalex": {"$eq": "https://openalex.org/W1491178396"}}
    print(f"Input: field='ids.openalex', operation='eq', value='W1491178396'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test full pipeline with parse_filter_param
    print("Testing parse_filter_param function:")
    filter_param = "ids.openalex:W1491178396"
    result = parse_filter_param(filter_param)
    expected = {"ids.openalex": {"$eq": "https://openalex.org/W1491178396"}}
    print(f"Input filter parameter: '{filter_param}'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test that full URLs pass through unchanged
    print("Testing full URL pass-through:")
    filter_param = "ids.openalex:https://openalex.org/W1491178396"
    result = parse_filter_param(filter_param)
    expected = {"ids.openalex": {"$eq": "https://openalex.org/W1491178396"}}
    print(f"Input filter parameter: '{filter_param}'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test multiple entity types
    print("Testing different OpenAlex entity types:")
    test_cases = [
        ("ids.openalex:W1491178396", "Works"),
        ("ids.openalex:A5023888391", "Authors"),
        ("ids.openalex:C41008148", "Concepts"),
        ("ids.openalex:I136199984", "Institutions"),
        ("ids.openalex:S2764455424", "Sources"),
        ("ids.openalex:T10047", "Topics"),
        ("ids.openalex:F2764455424", "Fields"),
        ("ids.openalex:P4310319900", "Publishers"),
    ]
    
    for filter_param, entity_type in test_cases:
        result = parse_filter_param(filter_param)
        short_id = filter_param.split(":")[1]
        expected_url = f"https://openalex.org/{short_id}"
        print(f"{entity_type}: {short_id} → {result['ids.openalex']['$eq']}")
        assert result == {"ids.openalex": {"$eq": expected_url}}
    
    print("\n✅ All tests passed! Short OpenAlex IDs are correctly converted to full URLs.")

if __name__ == "__main__":
    test_short_id_filtering()
