#!/usr/bin/env python3
"""
API acceptance tests for ID shortcut functionality

Simplified test suite focusing on API-level acceptance testing.
For comprehensive ID handling tests, see test_id_handling.py.

Tests the exact user examples:
- fetch("http://localhost:9020/works/W1492801337")
- fetch("http://localhost:9020/works/openalex:W1492801337") 
- fetch("http://localhost:9020/works/doi:10.1007/978-3-540-24614-5_17")
- fetch("http://localhost:9020/works/mag:1492801337")
"""

import pytest
import requests
import json
import urllib.parse
from typing import Dict, Any


@pytest.fixture(scope="session", autouse=True)
def check_server():
    """Check if server is running before running tests"""
    try:
        response = requests.get("http://localhost:9020/", timeout=5)
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Server not running on localhost:9020: {e}")


class TestIDShortcutsAPI:
    """API acceptance tests for ID shortcut functionality"""
    
    BASE_URL = "http://localhost:9020"
    TEST_WORK_ID = "W1492801337"
    TEST_DOI = "10.1007/978-3-540-24614-5_17"
    TEST_MAG_ID = "1492801337"
    
    def _make_request(self, path: str, expected_status: int = 200) -> Dict[str, Any]:
        """Make a request and return JSON response"""
        response = requests.get(f"{self.BASE_URL}{path}", timeout=10)
        assert response.status_code == expected_status
        return response.json()
    
    def _validate_work_response(self, work: Dict[str, Any]):
        """Validate response is the expected test work"""
        assert 'id' in work and 'title' in work
        
        work_id = work['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        assert work_id == self.TEST_WORK_ID

    def test_direct_openalex_id(self):
        """Test: Direct OpenAlex ID"""
        work = self._make_request(f"/works/{self.TEST_WORK_ID}")
        self._validate_work_response(work)

    def test_openalex_prefix_format(self):
        """Test: OpenAlex prefix format"""
        work = self._make_request(f"/works/openalex:{self.TEST_WORK_ID}")
        self._validate_work_response(work)

    def test_doi_format(self):
        """Test: DOI format"""
        work = self._make_request(f"/works/doi:{self.TEST_DOI}")
        self._validate_work_response(work)

    def test_mag_format(self):
        """Test: MAG format"""
        work = self._make_request(f"/works/mag:{self.TEST_MAG_ID}")
        self._validate_work_response(work)

    def test_url_encoded_formats(self):
        """Test: URL-encoded formats (what browsers send)"""
        test_cases = [
            f"openalex:{self.TEST_WORK_ID}",
            f"doi:{self.TEST_DOI}",
            f"mag:{self.TEST_MAG_ID}",
        ]
        
        for raw_id in test_cases:
            encoded_id = urllib.parse.quote(raw_id)
            work = self._make_request(f"/works/{encoded_id}")
            self._validate_work_response(work)

    def test_exact_user_examples(self):
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
            assert work_id == 'W1492801337'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
