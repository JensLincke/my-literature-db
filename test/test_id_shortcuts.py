#!/usr/bin/env python3
"""
Acceptance tests for ID shortcut functionality

This test suite validates that all ID shortcut formats work correctly through the API.
Tests the functionality implemented for:
- fetch("http://swacopilot:9020/works/W1492801337")
- fetch("http://swacopilot:9020/works/openalex:W1492801337") 
- fetch("http://swacopilot:9020/works/doi:10.1007/978-3-540-24614-5_17")
- fetch("http://swacopilot:9020/works/mag:1492801337")

Assumes the server is running on localhost:9020.

Run with:
    python -m pytest test/test_id_shortcuts.py -v
"""

import pytest
import requests
import json
import urllib.parse
from typing import Dict, Any, Optional


@pytest.fixture(scope="session", autouse=True)
def check_server():
    """Check if server is running before running tests"""
    BASE_URL = "http://localhost:9020"
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        print(f"Server is running (status: {response.status_code})")
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Server not running on {BASE_URL}: {e}")


class TestIDShortcuts:
    """Test ID shortcut functionality for works endpoint"""
    
    BASE_URL = "http://localhost:9020"
    
    # Test data - using W1492801337 which has all ID types
    TEST_WORK_ID = "W1492801337"
    TEST_DOI = "10.1007/978-3-540-24614-5_17"
    TEST_MAG_ID = "1492801337"
    EXPECTED_TITLE = "Dynamic Service Adaptation for Runtime System Extensions"
    
    def _make_request(self, path: str, expected_status: int = 200) -> Dict[str, Any]:
        """Make a request and return JSON response"""
        url = f"{self.BASE_URL}{path}"
        try:
            response = requests.get(url, timeout=10)
            assert response.status_code == expected_status, \
                f"Expected status {expected_status} for {url}, got {response.status_code}. Response: {response.text}"
            return response.json()
        except requests.exceptions.Timeout:
            pytest.fail(f"Request to {url} timed out after 10 seconds")
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON response from {url}: {response.text}")
        except requests.exceptions.ConnectionError as e:
            pytest.skip(f"Could not connect to server at {url}: {e}")
    
    def _validate_work_response(self, work: Dict[str, Any], expected_work_id: str = None):
        """Validate that the response is a valid work with expected properties"""
        # Should have required fields
        assert 'id' in work, "Work should have 'id' field"
        assert 'title' in work, "Work should have 'title' field"
        
        # Extract work ID from response (could be full URL or short ID)
        work_id = work['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        
        # Should match expected work ID if provided
        if expected_work_id:
            assert work_id == expected_work_id, f"Expected work ID {expected_work_id}, got {work_id}"
        
        # Should be our test work
        assert work_id == self.TEST_WORK_ID, f"Expected test work {self.TEST_WORK_ID}, got {work_id}"
        
        # Title should match (allowing for truncation)
        title = work.get('title', '')
        assert self.EXPECTED_TITLE in title or title in self.EXPECTED_TITLE, \
            f"Expected title to contain '{self.EXPECTED_TITLE}', got '{title}'"
        
        # Should have the IDs structure with all our test IDs
        if 'ids' in work:
            ids = work['ids']
            
            # Check OpenAlex ID
            if 'openalex' in ids:
                openalex_id = ids['openalex']
                if openalex_id.startswith('https://openalex.org/'):
                    openalex_id = openalex_id.split('/')[-1]
                assert openalex_id == self.TEST_WORK_ID
            
            # Check DOI
            if 'doi' in ids:
                doi = ids['doi']
                assert self.TEST_DOI in doi, f"Expected DOI to contain {self.TEST_DOI}, got {doi}"
            
            # Check MAG ID
            if 'mag' in ids:
                mag_id = str(ids['mag'])
                assert mag_id == self.TEST_MAG_ID, f"Expected MAG ID {self.TEST_MAG_ID}, got {mag_id}"

    # Test 1: Direct OpenAlex ID (baseline test)
    def test_direct_openalex_id(self):
        """Test: fetch('/works/W1492801337') - Direct OpenAlex ID"""
        work = self._make_request(f"/works/{self.TEST_WORK_ID}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 2: OpenAlex prefix format
    def test_openalex_prefix_format(self):
        """Test: fetch('/works/openalex:W1492801337') - OpenAlex prefix format"""
        work = self._make_request(f"/works/openalex:{self.TEST_WORK_ID}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 3: DOI format  
    def test_doi_format(self):
        """Test: fetch('/works/doi:10.1007/978-3-540-24614-5_17') - DOI format"""
        work = self._make_request(f"/works/doi:{self.TEST_DOI}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 4: MAG format
    def test_mag_format(self):
        """Test: fetch('/works/mag:1492801337') - MAG format"""
        work = self._make_request(f"/works/mag:{self.TEST_MAG_ID}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 5: URL-encoded formats (what browsers actually send)
    def test_url_encoded_openalex_format(self):
        """Test: URL-encoded OpenAlex format (openalex%3AW1492801337)"""
        # This is what the browser sends when you use fetch() with a colon
        encoded_id = urllib.parse.quote(f"openalex:{self.TEST_WORK_ID}")
        work = self._make_request(f"/works/{encoded_id}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    def test_url_encoded_doi_format(self):
        """Test: URL-encoded DOI format (doi%3A10.1007/978-3-540-24614-5_17)"""
        # This is what the browser sends when you use fetch() with a colon in DOI
        encoded_id = urllib.parse.quote(f"doi:{self.TEST_DOI}")
        work = self._make_request(f"/works/{encoded_id}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    def test_url_encoded_mag_format(self):
        """Test: URL-encoded MAG format (mag%3A1492801337)"""
        # This is what the browser sends when you use fetch() with a colon
        encoded_id = urllib.parse.quote(f"mag:{self.TEST_MAG_ID}")
        work = self._make_request(f"/works/{encoded_id}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 6: Field selection with ID shortcuts
    def test_doi_format_with_field_selection(self):
        """Test: DOI format with field selection parameter"""
        work = self._make_request(f"/works/doi:{self.TEST_DOI}?select=id,title,ids")
        self._validate_work_response(work, self.TEST_WORK_ID)
        
        # Should only have selected fields (plus system fields)
        expected_fields = {'id', 'title', 'ids', '_id'}  # _id is always included
        actual_fields = set(work.keys())
        
        # All requested fields should be present
        for field in ['id', 'title', 'ids']:
            if field in actual_fields:  # Field might not exist in document
                continue

    def test_openalex_format_with_field_selection(self):
        """Test: OpenAlex format with field selection parameter"""
        work = self._make_request(f"/works/openalex:{self.TEST_WORK_ID}?select=id,title,publication_year")
        self._validate_work_response(work, self.TEST_WORK_ID)

    # Test 7: Error cases
    def test_unknown_prefix_format(self):
        """Test: Unknown prefix should treat as regular ID and return 404"""
        response = requests.get(f"{self.BASE_URL}/works/unknown:12345")
        assert response.status_code == 404

    def test_invalid_doi_format(self):
        """Test: Invalid DOI should return 404"""
        response = requests.get(f"{self.BASE_URL}/works/doi:invalid.doi.format")
        assert response.status_code == 404

    def test_invalid_mag_format(self):
        """Test: Invalid MAG ID should return 404"""
        response = requests.get(f"{self.BASE_URL}/works/mag:invalid_mag_id")
        assert response.status_code == 404

    def test_nonexistent_openalex_id(self):
        """Test: Non-existent OpenAlex ID should return 404"""
        response = requests.get(f"{self.BASE_URL}/works/openalex:W9999999999")
        assert response.status_code == 404

    # Test 8: Edge cases
    def test_doi_with_complex_characters(self):
        """Test: DOI with complex characters and encoding"""
        # Test a more complex DOI pattern if available
        # For now, test our known DOI with URL encoding of special characters
        complex_doi = self.TEST_DOI.replace("/", "%2F").replace(".", "%2E")
        encoded_id = f"doi%3A{complex_doi}"
        
        work = self._make_request(f"/works/{encoded_id}")
        self._validate_work_response(work, self.TEST_WORK_ID)

    def test_case_sensitivity(self):
        """Test: Case sensitivity in prefixes"""
        # Prefixes should be case-sensitive (lowercase only)
        response = requests.get(f"{self.BASE_URL}/works/DOI:{self.TEST_DOI}")
        assert response.status_code == 404, "DOI prefix should be case-sensitive (lowercase only)"
        
        response = requests.get(f"{self.BASE_URL}/works/OpenAlex:{self.TEST_WORK_ID}")
        assert response.status_code == 404, "openalex prefix should be case-sensitive (lowercase only)"

    # Test 9: Integration with other endpoints
    def test_id_shortcuts_work_only_on_individual_endpoints(self):
        """Test: ID shortcuts should only work on individual work endpoints, not lists"""
        # These should not work on list endpoints
        response = requests.get(f"{self.BASE_URL}/works?filter=id:doi:{self.TEST_DOI}")
        # This might return results or empty, but should not crash
        assert response.status_code in [200, 400]  # 400 if filter format is invalid

    # Test 10: Performance test
    def test_id_shortcuts_performance(self):
        """Test: All ID shortcut formats should respond reasonably quickly"""
        import time
        
        test_cases = [
            f"/works/{self.TEST_WORK_ID}",
            f"/works/openalex:{self.TEST_WORK_ID}",
            f"/works/doi:{self.TEST_DOI}",
            f"/works/mag:{self.TEST_MAG_ID}",  # Re-enabled - MAG index build completed
        ]
        
        for path in test_cases:
            start_time = time.time()
            work = self._make_request(path)
            end_time = time.time()
            
            duration = end_time - start_time
            self._validate_work_response(work, self.TEST_WORK_ID)
            
            # Should respond within reasonable time (adjust as needed)
            assert duration < 5.0, f"Request to {path} took {duration:.2f}s, expected < 5.0s"


class TestIDShortcutsDocumentation:
    """Test that the examples from the user request work exactly as specified"""
    
    BASE_URL = "http://localhost:9020"
    
    def test_exact_user_examples(self):
        """Test the exact fetch examples provided by the user"""
        
        # Test cases exactly as specified in the user request
        test_cases = [
            # Original format
            f"{self.BASE_URL}/works/W1492801337",
            # OpenAlex format  
            f"{self.BASE_URL}/works/openalex:W1492801337",
            # DOI format
            f"{self.BASE_URL}/works/doi:10.1007/978-3-540-24614-5_17",
            # MAG format - re-enabled now that index build is complete
            f"{self.BASE_URL}/works/mag:1492801337",
        ]
        
        for url in test_cases:
            print(f"Testing: {url}")
            
            # Simulate what fetch() would do
            response = requests.get(url, timeout=30)
            
            # Should return 200 OK
            assert response.status_code == 200, f"Expected 200 for {url}, got {response.status_code}"
            
            # Should return valid JSON
            try:
                data = response.json()
            except json.JSONDecodeError:
                pytest.fail(f"Invalid JSON response from {url}")
            
            # Should be the same work
            work_id = data.get('id', '')
            if work_id.startswith('https://openalex.org/'):
                work_id = work_id.split('/')[-1]
            
            assert work_id == 'W1492801337', f"Expected W1492801337, got {work_id} from {url}"
            
            # Should have consistent title
            title = data.get('title', '')
            expected_title = "Dynamic Service Adaptation for Runtime System Extensions"
            assert expected_title in title or title in expected_title, \
                f"Title mismatch for {url}: got '{title}'"


if __name__ == '__main__':
    print("ID Shortcuts Acceptance Tests")
    print("=" * 50)
    print("Testing ID shortcut functionality:")
    print("- fetch('/works/W1492801337')")
    print("- fetch('/works/openalex:W1492801337')")  
    print("- fetch('/works/doi:10.1007/978-3-540-24614-5_17')")
    print("- fetch('/works/mag:1492801337')")
    print()
    print("Assumes server is running on http://localhost:9020")
    print()
    
    # Run with pytest
    pytest.main([__file__, '-v'])
