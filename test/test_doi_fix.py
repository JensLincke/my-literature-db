#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

async def test_doi_fix():
    from motor.motor_asyncio import AsyncIOMotorClient
    from handlers import BaseEntityHandler
    
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    handler = BaseEntityHandler(db.works, 'work')
    
    # Test DOI format specifically
    test_id = 'doi:10.1007/978-3-540-24614-5_17'
    
    try:
        print(f"Testing {test_id}...")
        work = await handler.get_entity(test_id)
        title = work.get('title', 'No title')[:50]
        print(f'✓ {test_id}: Found work "{title}"')
        
        # Also check what the actual DOI field contains
        print(f"DOI field in database: {work.get('ids', {}).get('doi', 'Not found')}")
        
        with open('doi_test_result.txt', 'w') as f:
            f.write(f'SUCCESS: {test_id} -> {title}\n')
            f.write(f'DOI field: {work.get("ids", {}).get("doi", "Not found")}\n')
        
    except Exception as e:
        print(f'✗ {test_id}: {str(e)}')
        with open('doi_test_result.txt', 'w') as f:
            f.write(f'ERROR: {test_id} -> {str(e)}\n')
    
    client.close()
    print("DOI test completed.")

if __name__ == '__main__':
    asyncio.run(test_doi_fix())
