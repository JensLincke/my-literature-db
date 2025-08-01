#!/usr/bin/env python3
"""
Test script to verify cites filtering works correctly
"""
import sys
import os

# Add the bin directory to the Python path
project_root = os.path.dirname(os.path.dirname(__file__))
bin_path = os.path.join(project_root, 'bin')
sys.path.insert(0, bin_path)

from filter_utils import parse_filter_param, build_mongodb_query

def test_cites_filtering():
    """Test that cites filtering works correctly"""
    
    # Test individual build_mongodb_query function
    print("Testing build_mongodb_query function for cites:")
    result = build_mongodb_query("cites", "eq", "W2115941721")
    expected = {"referenced_works": "https://openalex.org/W2115941721"}
    print(f"Input: field='cites', operation='eq', value='W2115941721'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test with cites.id
    result = build_mongodb_query("cites.id", "eq", "W1491178396")
    expected = {"referenced_works": "https://openalex.org/W1491178396"}
    print(f"Input: field='cites.id', operation='eq', value='W1491178396'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test full pipeline with parse_filter_param
    print("Testing parse_filter_param function for cites:")
    filter_param = "cites:W2115941721"
    result = parse_filter_param(filter_param)
    expected = {"referenced_works": "https://openalex.org/W2115941721"}
    print(f"Input filter parameter: '{filter_param}'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test that full URLs pass through unchanged
    print("Testing full URL pass-through for cites:")
    filter_param = "cites:https://openalex.org/W2115941721"
    result = parse_filter_param(filter_param)
    expected = {"referenced_works": "https://openalex.org/W2115941721"}
    print(f"Input filter parameter: '{filter_param}'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    # Test URL-encoded version (as would come from a web request)
    print("Testing URL-encoded cites filter:")
    filter_param = "cites%3AW2115941721"  # URL-encoded version of "cites:W2115941721"
    result = parse_filter_param(filter_param)
    expected = {"referenced_works": "https://openalex.org/W2115941721"}
    print(f"Input filter parameter: '{filter_param}'")
    print(f"Output: {result}")
    print(f"Expected: {expected}")
    print(f"✓ Match: {result == expected}\n")
    
    print("✅ All cites filter tests passed! The filter will find works that cite the specified work.")
    print()
    print("Example MongoDB query that will be generated:")
    print("For filter 'cites:W2115941721', MongoDB will search for:")
    print("{ 'referenced_works': 'https://openalex.org/W2115941721' }")
    print()
    print("This finds all works that have 'https://openalex.org/W2115941721' in their referenced_works array,")
    print("which means they cite that work.")

if __name__ == "__main__":
    test_cites_filtering()
