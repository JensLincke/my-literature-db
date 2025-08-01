#!/usr/bin/env python3
import asyncio
import sys
sys.path.insert(0, '/home/jlincke/lively4/my-literature-db/bin')

async def debug_doi_search():
    from motor.motor_asyncio import AsyncIOMotorClient
    import urllib.parse
    
    client = AsyncIOMotorClient('mongodb://localhost:27017')
    db = client.openalex
    
    # Test the exact DOI you mentioned
    target_doi = "https://doi.org/10.1007/978-3-540-24614-5_17"
    short_doi = "10.1007/978-3-540-24614-5_17"
    
    print(f"Looking for DOI: {target_doi}")
    print(f"Short DOI: {short_doi}")
    
    # 1. First, let's verify the document exists
    work = await db.works.find_one({"ids.doi": target_doi})
    if work:
        print(f"✓ Found work directly: {work.get('title', 'No title')}")
        print(f"  Work ID: {work.get('_id')}")
        print(f"  DOI in database: {work.get('ids', {}).get('doi')}")
    else:
        print("✗ Could not find work with direct DOI search")
    
    # 2. Test our URL decoding logic
    test_encoded_id = "doi%3A10.1007/978-3-540-24614-5_17"
    decoded_id = urllib.parse.unquote(test_encoded_id)
    print(f"\nURL decoding test:")
    print(f"  Encoded: {test_encoded_id}")
    print(f"  Decoded: {decoded_id}")
    
    # 3. Test the prefix splitting logic
    if ":" in decoded_id:
        prefix, actual_id = decoded_id.split(":", 1)
        print(f"  Prefix: '{prefix}'")
        print(f"  Actual ID: '{actual_id}'")
        
        # 4. Test the search patterns we use
        search_patterns = [
            f"https://doi.org/{actual_id}",
            f"http://dx.doi.org/{actual_id}",
        ]
        
        for pattern in search_patterns:
            print(f"\nTesting pattern: {pattern}")
            result = await db.works.find_one({"ids.doi": pattern})
            if result:
                print(f"  ✓ Found with pattern: {result.get('title', 'No title')}")
            else:
                print(f"  ✗ Not found with pattern")
        
        # 5. Test regex pattern
        regex_pattern = f"/{actual_id}$"
        print(f"\nTesting regex: {regex_pattern}")
        result = await db.works.find_one({"ids.doi": {"$regex": regex_pattern}})
        if result:
            print(f"  ✓ Found with regex: {result.get('title', 'No title')}")
        else:
            print(f"  ✗ Not found with regex")
    
    # 6. Let's also check if there are any DOIs that contain this pattern
    print(f"\nSearching for any DOI containing '{short_doi}':")
    async for doc in db.works.find({"ids.doi": {"$regex": short_doi}}).limit(5):
        print(f"  Found: {doc.get('ids', {}).get('doi')} -> {doc.get('title', 'No title')[:50]}")
    
    client.close()

if __name__ == '__main__':
    asyncio.run(debug_doi_search())
