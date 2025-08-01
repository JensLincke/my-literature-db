#!/usr/bin/env python3
import asyncio
import sys
import os

# Add the bin directory to the Python path
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

from motor.motor_asyncio import AsyncIOMotorClient
from handlers import BaseEntityHandler

async def test_id_formats():
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    
    handler = BaseEntityHandler(db.works, 'work')
    
    # Test different ID formats
    test_ids = [
        'W1492801337',  # Original format
        'openalex:W1492801337',  # OpenAlex format
        'doi:10.1007/978-3-540-24614-5_17',  # DOI format
        'mag:1492801337'  # MAG format
    ]
    
    results = []
    for test_id in test_ids:
        try:
            print(f"Testing {test_id}...")
            work = await handler.get_entity(test_id)
            title = work.get('title', 'No title')[:50]  # Truncate title
            result = f'✓ {test_id}: Found work "{title}"'
            results.append(result)
            print(result)
        except Exception as e:
            result = f'✗ {test_id}: {str(e)}'
            results.append(result)
            print(result)
        
        # Force flush output
        sys.stdout.flush()
    
    client.close()
    return results

if __name__ == '__main__':
    try:
        print("Starting ID format tests...")
        results = asyncio.run(test_id_formats())
        
        # Write results to a file
        with open('id_test_results.txt', 'w') as f:
            f.write('\n'.join(results))
        
        print(f"\nResults written to id_test_results.txt")
        print("Test completed successfully!")
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
