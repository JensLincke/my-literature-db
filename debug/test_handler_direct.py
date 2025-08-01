#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

async def test_handler_directly():
    from motor.motor_asyncio import AsyncIOMotorClient
    from handlers import BaseEntityHandler
    
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    
    # Create the handler exactly like the server does
    handler = BaseEntityHandler(db.works, 'work')
    
    # Test the exact scenarios
    test_cases = [
        'W1492801337',  # Direct ID
        'doi:10.1007/978-3-540-24614-5_17',  # Direct DOI
        'doi%3A10.1007/978-3-540-24614-5_17',  # URL-encoded DOI (what server receives)
    ]
    
    results = []
    for test_id in test_cases:
        try:
            print(f"Testing handler with: '{test_id}'")
            work = await handler.get_entity(test_id)
            title = work.get('title', 'No title')[:50]
            result = f'✓ {test_id}: Found "{title}"'
            results.append(result)
            print(f"  {result}")
        except Exception as e:
            result = f'✗ {test_id}: {str(e)}'
            results.append(result)
            print(f"  {result}")
            # Print the full exception for debugging
            import traceback
            print(f"  Exception details: {traceback.format_exc()}")
    
    client.close()
    
    # Write results
    with open('handler_test_results.txt', 'w') as f:
        f.write('\n'.join(results))
    
    print(f"\nAll tests completed. Results written to handler_test_results.txt")

if __name__ == '__main__':
    asyncio.run(test_handler_directly())
