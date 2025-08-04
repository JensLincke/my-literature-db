#!/usr/bin/env python3
"""
Comprehensive tests for ID format handling

This test suite validates that all ID shortcut formats work correctly.
Tests both direct handler access and API endpoints.

Consolidates functionality from:
- test_id_formats.py
- test_id_shortcuts.py  
- test_handler_unit.py
- test_mag_only.py
- test_doi_fix.py
"""

import pytest
import requests
import json
import urllib.parse
import sys
import os
from typing import Dict, Any

# Add bin directory to path for handler imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))

from motor.motor_asyncio import AsyncIOMotorClient
from handlers import BaseEntityHandler


@pytest.fixture(scope="session", autouse=True)
def check_dependencies():
    """Check if server and database are available before running tests"""
    import asyncio
    
    async def _check_db():
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
    
    def _check_server():
        try:
            response = requests.get("http://localhost:9020/", timeout=5)
        except requests.exceptions.RequestException:
            pytest.skip("API server not running on localhost:9020")
    
    asyncio.run(_check_db())
    _check_server()


class TestIDHandling:
    """Test all ID format handling comprehensively"""
    
    # Test data - using a work that has all ID types
    TEST_WORK_ID = "W1492801337"
    TEST_DOI = "10.1007/978-3-540-24614-5_17"
    TEST_MAG_ID = "1492801337"
    EXPECTED_TITLE = "Dynamic Service Adaptation for Runtime System Extensions"
    BASE_URL = "http://localhost:9020"
    
    async def _get_handler(self):
        """Helper to create handler with fresh connection"""
        client = AsyncIOMotorClient('mongodb://localhost:27017')
        db = client.openalex
        handler = BaseEntityHandler(db.works, 'work')
        return client, handler
    
    def _make_api_request(self, path: str, expected_status: int = 200) -> Dict[str, Any]:
        """Make an API request and return JSON response"""
        url = f"{self.BASE_URL}{path}"
        response = requests.get(url, timeout=10)
        assert response.status_code == expected_status, \
            f"Expected {expected_status}, got {response.status_code} for {url}"
        return response.json()
    
    def _validate_work(self, work: Dict[str, Any]):
        """Validate that response is the expected test work"""
        assert 'id' in work and 'title' in work
        
        # Extract work ID (handle both URL and plain ID formats)
        work_id = work['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        
        assert work_id == self.TEST_WORK_ID
        
        # Validate title
        title = work.get('title', '')
        assert self.EXPECTED_TITLE in title or title in self.EXPECTED_TITLE
    
    # Test 1: Direct handler access (unit tests)
    @pytest.mark.asyncio
    async def test_handler_id_formats(self):
        """Test all ID formats work through direct handler access"""
        client, handler = await self._get_handler()
        
        test_cases = [
            ('W1492801337', 'Direct OpenAlex ID'),
            ('openalex:W1492801337', 'OpenAlex prefix format'),
            ('doi:10.1007/978-3-540-24614-5_17', 'DOI format'),
            ('mag:1492801337', 'MAG format'),
            ('doi%3A10.1007/978-3-540-24614-5_17', 'URL-encoded DOI'),
        ]
        
        try:
            for test_id, description in test_cases:
                work = await handler.get_entity(test_id)
                self._validate_work(work)
        finally:
            client.close()
    
    @pytest.mark.asyncio 
    async def test_handler_consistency(self):
        """Test that all ID formats return the same work"""
        client, handler = await self._get_handler()
        
        test_ids = [
            'W1492801337',
            'doi:10.1007/978-3-540-24614-5_17', 
            'openalex:W1492801337',
            'mag:1492801337'
        ]
        
        try:
            works = []
            for test_id in test_ids:
                work = await handler.get_entity(test_id)
                works.append(work)
                self._validate_work(work)
            
            # All should have same title
            titles = [work.get('title', '') for work in works]
            assert all(title == titles[0] for title in titles), \
                "All ID formats should return the same work"
        finally:
            client.close()
    
    # Test 2: API endpoint access (integration tests)
    def test_api_id_formats(self):
        """Test all ID formats work through API endpoints"""
        test_cases = [
            f"/works/{self.TEST_WORK_ID}",
            f"/works/openalex:{self.TEST_WORK_ID}",
            f"/works/doi:{self.TEST_DOI}",
            f"/works/mag:{self.TEST_MAG_ID}",
        ]
        
        for path in test_cases:
            work = self._make_api_request(path)
            self._validate_work(work)
    
    def test_api_url_encoded_formats(self):
        """Test URL-encoded ID formats (what browsers send)"""
        test_cases = [
            (f"openalex:{self.TEST_WORK_ID}", "OpenAlex format"),
            (f"doi:{self.TEST_DOI}", "DOI format"),
            (f"mag:{self.TEST_MAG_ID}", "MAG format"),
        ]
        
        for raw_id, description in test_cases:
            encoded_id = urllib.parse.quote(raw_id)
            work = self._make_api_request(f"/works/{encoded_id}")
            self._validate_work(work)
    
    def test_api_field_selection(self):
        """Test field selection works with ID shortcuts"""
        test_cases = [
            f"/works/doi:{self.TEST_DOI}?select=id,title,ids",
            f"/works/openalex:{self.TEST_WORK_ID}?select=id,title,publication_year",
        ]
        
        for path in test_cases:
            work = self._make_api_request(path)
            self._validate_work(work)
    
    # Test 3: Error cases
    def test_invalid_ids(self):
        """Test that invalid IDs return 404"""
        invalid_cases = [
            "/works/unknown:12345",
            "/works/doi:invalid.doi.format", 
            "/works/mag:invalid_mag_id",
            "/works/openalex:W9999999999",
        ]
        
        for path in invalid_cases:
            response = requests.get(f"{self.BASE_URL}{path}")
            assert response.status_code == 404
    
    def test_case_sensitivity(self):
        """Test that prefixes are case-sensitive"""
        case_sensitive_tests = [
            "/works/DOI:10.1007/978-3-540-24614-5_17",
            "/works/OpenAlex:W1492801337",
            "/works/MAG:1492801337",
        ]
        
        for path in case_sensitive_tests:
            response = requests.get(f"{self.BASE_URL}{path}")
            assert response.status_code == 404, f"Should be case-sensitive: {path}"
    
    # Test 4: Performance
    def test_performance(self):
        """Test that all ID formats respond quickly"""
        import time
        
        test_cases = [
            f"/works/{self.TEST_WORK_ID}",
            f"/works/openalex:{self.TEST_WORK_ID}",
            f"/works/doi:{self.TEST_DOI}",
            f"/works/mag:{self.TEST_MAG_ID}",
        ]
        
        for path in test_cases:
            start_time = time.time()
            work = self._make_api_request(path)
            duration = time.time() - start_time
            
            self._validate_work(work)
            assert duration < 5.0, f"Request took {duration:.2f}s, should be < 5s"


class TestDocumentationExamples:
    """Test the exact examples from documentation"""
    
    BASE_URL = "http://localhost:9020"
    
    def test_readme_examples(self):
        """Test the exact fetch examples from user requirements"""
        examples = [
            f"{self.BASE_URL}/works/W1492801337",
            f"{self.BASE_URL}/works/openalex:W1492801337", 
            f"{self.BASE_URL}/works/doi:10.1007/978-3-540-24614-5_17",
            f"{self.BASE_URL}/works/mag:1492801337",
        ]
        
        for url in examples:
            response = requests.get(url, timeout=10)
            assert response.status_code == 200, f"Failed: {url}"
            
            data = response.json()
            work_id = data.get('id', '')
            if work_id.startswith('https://openalex.org/'):
                work_id = work_id.split('/')[-1]
            
            assert work_id == 'W1492801337', f"Wrong work from {url}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
