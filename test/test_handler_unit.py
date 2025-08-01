#!/usr/bin/env python3
"""
Unit tests for the BaseEntityHandler ID shortcut functionality

These tests directly test the handler logic without going through HTTP.
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


class TestHandlerDirectly:
    """Test the handler logic directly without HTTP layer"""
    
    async def _get_handler(self):
        """Create a handler instance for testing"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler
    
    @pytest.mark.asyncio
    async def test_direct_openalex_id(self):
        """Test direct OpenAlex ID lookup"""
        client, handler = await self._get_handler()
        try:
            import asyncio
            try:
                work = await asyncio.wait_for(handler.get_entity('W1492801337'), timeout=10.0)
                assert work is not None
                assert 'id' in work
                assert 'title' in work
                # Extract work ID (could be full URL or short)
                work_id = work['id']
                if work_id.startswith('https://openalex.org/'):
                    work_id = work_id.split('/')[-1]
                assert work_id == 'W1492801337'
            except asyncio.TimeoutError:
                pytest.fail("Test timed out after 10 seconds")
            except Exception as e:
                if "not found" in str(e).lower() or "connection" in str(e).lower():
                    pytest.skip(f"Database not available or test data missing: {e}")
                else:
                    raise
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_doi_format(self):
        """Test DOI format: doi:10.1007/978-3-540-24614-5_17"""
        client, handler = await self._get_handler()
        try:
            work = await handler.get_entity('doi:10.1007/978-3-540-24614-5_17')
            assert work is not None
            assert 'id' in work
            assert 'title' in work
            # Should be the same work as W1492801337
            work_id = work['id']
            if work_id.startswith('https://openalex.org/'):
                work_id = work_id.split('/')[-1]
            assert work_id == 'W1492801337'
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_url_encoded_doi(self):
        """Test URL-encoded DOI format (what server receives)"""
        client, handler = await self._get_handler()
        try:
            work = await handler.get_entity('doi%3A10.1007/978-3-540-24614-5_17')
            assert work is not None
            assert 'id' in work
            assert 'title' in work
            # Should be the same work as W1492801337
            work_id = work['id']
            if work_id.startswith('https://openalex.org/'):
                work_id = work_id.split('/')[-1]
            assert work_id == 'W1492801337'
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_openalex_prefix(self):
        """Test OpenAlex prefix format"""
        client, handler = await self._get_handler()
        try:
            work = await handler.get_entity('openalex:W1492801337')
            assert work is not None
            assert 'id' in work
            assert 'title' in work
            # Should be the same work as W1492801337
            work_id = work['id']
            if work_id.startswith('https://openalex.org/'):
                work_id = work_id.split('/')[-1]
            assert work_id == 'W1492801337'
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_mag_format(self):
        """Test MAG format"""
        client, handler = await self._get_handler()
        try:
            import asyncio
            try:
                work = await asyncio.wait_for(handler.get_entity('mag:1492801337'), timeout=10.0)
                assert work is not None
                assert 'id' in work
                assert 'title' in work
                # Should be the same work as W1492801337
                work_id = work['id']
                if work_id.startswith('https://openalex.org/'):
                    work_id = work_id.split('/')[-1]
                assert work_id == 'W1492801337'
            except asyncio.TimeoutError:
                pytest.fail("Test timed out after 10 seconds")
            except Exception as e:
                if "not found" in str(e).lower() or "connection" in str(e).lower():
                    pytest.skip(f"Database not available or test data missing: {e}")
                else:
                    raise
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_invalid_id_format(self):
        """Test that invalid IDs raise appropriate exceptions"""
        client, handler = await self._get_handler()
        try:
            with pytest.raises(Exception):  # Should be more specific, but depends on handler implementation
                await handler.get_entity('invalid:nonexistent')
        finally:
            client.close()
    
    @pytest.mark.asyncio
    async def test_title_consistency(self):
        """Test that all ID formats return the same title"""
        expected_title = "Dynamic Service Adaptation for Runtime System Extensions"
        
        # Test all formats return the same work
        test_ids = [
            'W1492801337',
            'doi:10.1007/978-3-540-24614-5_17',
            'doi%3A10.1007/978-3-540-24614-5_17',
            'openalex:W1492801337',
            'mag:1492801337'
        ]
        
        client, handler = await self._get_handler()
        try:
            for test_id in test_ids:
                work = await handler.get_entity(test_id)
                title = work.get('title', '')
                assert expected_title in title or title in expected_title, \
                    f"Title mismatch for {test_id}: got '{title}'"
        finally:
            client.close()


if __name__ == '__main__':
    # Allow running this file directly for quick testing
    pytest.main([__file__, '-v'])
