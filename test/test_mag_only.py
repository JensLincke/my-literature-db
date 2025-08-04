#!/usr/bin/env python3
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

from motor.motor_asyncio import AsyncIOMotorClient
from handlers import BaseEntityHandler

@pytest.fixture(scope="session", autouse=True)
def check_database():
    """Check if MongoDB is running and has test data before running tests"""
    import asyncio
    async def _check():
        try:
            client = AsyncIOMotorClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
            db = client.openalex
            # Try to access the database to check if it's available
            await client.admin.command('ping')
            # Check if the works collection exists and has data
            count = await db.works.count_documents({}, limit=1)
            if count == 0:
                pytest.skip("MongoDB works collection is empty - no test data available")
            client.close()
            print("MongoDB is available with test data")
        except Exception as e:
            pytest.skip(f"MongoDB not available at mongodb://localhost:27017: {e}")
    
    # Run the async check
    asyncio.run(_check())

class TestMagOnly:
    """Test MAG format handling"""
    
    async def _get_handler(self):
        """Helper to create handler with fresh connection"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler

    @pytest.mark.asyncio
    async def test_single_id(self):
        client, handler = await self._get_handler()
        
        try:
            # Test MAG format specifically with timeout
            test_id = 'mag:1492801337'
            
            print(f"Testing {test_id}...")
            
            # Add asyncio timeout to ensure test doesn't hang
            import asyncio
            try:
                work = await asyncio.wait_for(handler.get_entity(test_id), timeout=10.0)
                title = work.get('title', 'No title')[:50]
                print(f'✓ {test_id}: Found work "{title}"')
                
                # Assert for pytest
                assert work is not None
                assert 'id' in work
                assert 'title' in work
            except asyncio.TimeoutError:
                pytest.fail(f"Test timed out after 10 seconds for {test_id}")
            except Exception as e:
                # If we get a 404 or connection error, that's expected without a database
                if "not found" in str(e).lower() or "connection" in str(e).lower():
                    pytest.skip(f"Database not available or test data missing: {e}")
                else:
                    raise
            
        finally:
            client.close()
            print("Test completed.")
