#!/usr/bin/env python3
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

from motor.motor_asyncio import AsyncIOMotorClient
from handlers import BaseEntityHandler

class TestDoiFix:
    """Test DOI format handling"""
    
    async def _get_handler(self):
        """Helper to create handler with fresh connection"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler

    @pytest.mark.asyncio
    async def test_doi_fix(self):
        client, handler = await self._get_handler()
        
        try:
            # Test DOI format specifically
            test_id = 'doi:10.1007/978-3-540-24614-5_17'
            
            print(f"Testing {test_id}...")
            work = await handler.get_entity(test_id)
            title = work.get('title', 'No title')[:50]
            print(f'✓ {test_id}: Found work "{title}"')
            
            # Also check what the actual DOI field contains
            print(f"DOI field in database: {work.get('ids', {}).get('doi', 'Not found')}")
            
            # Assert for pytest
            assert work is not None
            assert 'id' in work
            assert 'title' in work
            
        finally:
            client.close()
            print("DOI test completed.")
