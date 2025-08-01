#!/usr/bin/env python3
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

from motor.motor_asyncio import AsyncIOMotorClient
from handlers import BaseEntityHandler
import urllib.parse

class TestUrlEncoding:
    """Test URL encoding handling"""
    
    async def _get_handler(self):
        """Helper to create handler with fresh connection"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler

    @pytest.mark.asyncio
    async def test_url_encoding(self):
        client, handler = await self._get_handler()
        
        try:
            # Test URL-encoded DOI format (simulating what the server receives)
            test_ids = [
                'doi%3A10.1007/978-3-540-24614-5_17',  # URL-encoded version
                'doi:10.1007/978-3-540-24614-5_17',    # Direct version
                'openalex%3AW1492801337',              # URL-encoded version
                'openalex:W1492801337'                 # Direct version
            ]
            
            for test_id in test_ids:
                print(f"Testing {test_id}...")
                work = await handler.get_entity(test_id)
                title = work.get('title', 'No title')[:50]
                print(f'✓ {test_id}: Found work "{title}"')
                
                # Assert for pytest
                assert work is not None
                assert 'id' in work
                assert 'title' in work
            
            print("URL encoding test completed.")
            
        finally:
            client.close()
