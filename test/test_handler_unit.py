#!/usr/bin/env python3
"""
Unit tests for the BaseEntityHandler ID shortcut functionality

These tests directly test the handler logic without going through HTTP.
For comprehensive ID testing, see test_id_handling.py
"""
import pytest
import sys
import os

# Add the bin directory to the path
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
            await client.admin.command('ping')
            count = await db.works.count_documents({}, limit=1)
            if count == 0:
                pytest.skip("MongoDB works collection is empty")
            client.close()
        except Exception as e:
            pytest.skip(f"MongoDB not available: {e}")
    asyncio.run(_check())


class TestHandlerDirectly:
    """Test the handler logic directly without HTTP layer"""
    
    async def _get_handler(self):
        """Create a handler instance for testing"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler
    
    @pytest.mark.asyncio
    async def test_basic_id_formats(self):
        """Test basic ID format handling"""
        client, handler = await self._get_handler()
        
        try:
            # Test a few key ID formats
            test_cases = [
                'W1492801337',
                'openalex:W1492801337',
                'doi:10.1007/978-3-540-24614-5_17',
                'mag:1492801337'
            ]
            
            for test_id in test_cases:
                work = await handler.get_entity(test_id)
                assert work is not None
                assert 'id' in work
                assert 'title' in work
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_invalid_id_format(self):
        """Test that invalid IDs raise appropriate exceptions"""
        client, handler = await self._get_handler()
        
        try:
            with pytest.raises(Exception):  # Should raise 404/not found
                await handler.get_entity('nonexistent:W9999999999')
        finally:
            client.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
