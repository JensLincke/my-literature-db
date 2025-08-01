#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

async def test_url_encoding():
    from motor.motor_asyncio import AsyncIOMotorClient
    from handlers import BaseEntityHandler
    import urllib.parse
    
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    handler = BaseEntityHandler(db.works, 'work')
    
    # Test URL-encoded DOI format (simulating what the server receives)
    test_ids = [
        'doi%3A10.1007/978-3-540-24614-5_17',  # URL-encoded version
        'doi:10.1007/978-3-540-24614-5_17',    # Direct version
        'openalex%3AW1492801337',              # URL-encoded version
        'openalex:W1492801337'                 # Direct version
    ]
    
    results = []
    for test_id in test_ids:
        try:
            print(f"Testing {test_id}...")
            work = await handler.get_entity(test_id)
            title = work.get('title', 'No title')[:50]
            result = f'✓ {test_id}: Found work "{title}"'
            results.append(result)
            print(result)
        except Exception as e:
            result = f'✗ {test_id}: {str(e)}'
            results.append(result)
            print(result)
    
    client.close()
    
    # Write results to file
    with open('url_encoding_test_result.txt', 'w') as f:
        f.write('\n'.join(results))
    
    print("URL encoding test completed.")

if __name__ == '__main__':
    asyncio.run(test_url_encoding())
