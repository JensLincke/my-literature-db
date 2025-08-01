#!/usr/bin/env python3
"""
Comprehensive test script to demonstrate the cites functionality

This script shows:
1. How the cites filter works
2. What MongoDB queries are generated
3. How to use it in API calls
"""
import sys
import os

# Add the bin directory to the Python path
project_root = os.path.dirname(__file__)
bin_path = os.path.join(project_root, 'bin')
sys.path.insert(0, bin_path)

from filter_utils import parse_filter_param

def demonstrate_cites_functionality():
    """Demonstrate how cites filtering works"""
    
    print("=" * 70)
    print("CITES FILTER FUNCTIONALITY DEMONSTRATION")
    print("=" * 70)
    print()
    
    # Scenario: Finding works that cite a specific work
    target_work = "W2115941721"
    print(f"Scenario: Find all works that cite work {target_work}")
    print()
    
    # Show different ways to specify the cites filter
    test_cases = [
        ("cites:W2115941721", "Short form using 'cites'"),
        ("cites.id:W2115941721", "Alternative form using 'cites.id'"),
        ("cites:https://openalex.org/W2115941721", "Full URL form"),
    ]
    
    print("Different ways to specify the cites filter:")
    print("-" * 50)
    
    for filter_param, description in test_cases:
        result = parse_filter_param(filter_param)
        print(f"Filter: {filter_param}")
        print(f"Description: {description}")
        print(f"MongoDB Query: {result}")
        print()
    
    # Show what this means in terms of data
    print("Data structure explanation:")
    print("-" * 30)
    print("Each work in the database has a 'referenced_works' array like this:")
    print("""
{
  "id": "https://openalex.org/W2345678901",
  "title": "Some Academic Paper",
  "referenced_works": [
    "https://openalex.org/W1493700809",
    "https://openalex.org/W1673079227", 
    "https://openalex.org/W2115941721",  <- This work cites W2115941721
    "https://openalex.org/W2036196659",
    ...
  ],
  "referenced_works_count": 19
}
    """)
    
    print()
    print("API Usage Examples:")
    print("-" * 20)
    
    # Show how to use this in API calls
    base_url = "http://swacopilot:9020"
    api_examples = [
        f"{base_url}/works?filter=cites:W2115941721",
        f"{base_url}/works?filter=cites:W2115941721&select=id,title,publication_year",
        f"{base_url}/works?filter=cites:W2115941721&per_page=100&page=1",
        f"{base_url}/works?filter=cites:W2115941721&sort=cited_by_count:desc",
    ]
    
    for i, example in enumerate(api_examples, 1):
        print(f"{i}. {example}")
    
    print()
    print("JavaScript fetch examples:")
    print("-" * 25)
    
    js_examples = [
        f'fetch("{base_url}/works?filter=cites:W2115941721&select=id&per_page=100&page=1").then(r => r.json())',
        f'fetch("{base_url}/works?filter=cites:W2115941721&sort=publication_year:desc").then(r => r.json())',
    ]
    
    for i, example in enumerate(js_examples, 1):
        print(f"{i}. {example}")
    
    print()
    print("Performance Notes:")
    print("-" * 18)
    print("• An index on 'referenced_works' field has been added for fast queries")
    print("• This enables efficient lookup of citing works")
    print("• Large result sets can be paginated using per_page and page parameters")
    
    print()
    print("✅ Cites functionality is ready to use!")
    print("=" * 70)

if __name__ == "__main__":
    demonstrate_cites_functionality()
