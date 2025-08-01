#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

async def test_single_id():
    from motor.motor_asyncio import AsyncIOMotorClient
    from handlers import BaseEntityHandler
    
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    handler = BaseEntityHandler(db.works, 'work')
    
    # Test MAG format specifically
    test_id = 'mag:1492801337'
    
    try:
        print(f"Testing {test_id}...")
        work = await handler.get_entity(test_id)
        title = work.get('title', 'No title')[:50]
        print(f'✓ {test_id}: Found work "{title}"')
        
        # Write result
        with open('mag_test_result.txt', 'w') as f:
            f.write(f'SUCCESS: {test_id} -> {title}\n')
        
    except Exception as e:
        print(f'✗ {test_id}: {str(e)}')
        with open('mag_test_result.txt', 'w') as f:
            f.write(f'ERROR: {test_id} -> {str(e)}\n')
    
    client.close()
    print("Test completed.")

if __name__ == '__main__':
    asyncio.run(test_single_id())
