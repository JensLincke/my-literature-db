#!/usr/bin/env python3
"""
Test for direct ID URL support

Tests for accessing entities directly via URLs like:
- http://localhost:9020/W1492801337 (work)
- http://localhost:9020/A5102590311 (author)
- http://localhost:9020/C123456789 (concept)
etc.
"""
import pytest
import requests
from typing import Dict, Any


@pytest.fixture(scope="session", autouse=True)
def check_server():
    """Check if server is running before running tests"""
    BASE_URL = "http://localhost:9020"
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Server not running on {BASE_URL}: {e}")


class TestDirectIDURLs:
    BASE_URL = "http://localhost:9020"
    
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
    
    def test_direct_work_id_url(self):
        """Test direct work ID URL: http://localhost:9020/W1492801337"""
        response = self._make_request("/W1492801337")
        
        # Should return a work object
        assert 'id' in response
        assert 'title' in response
        
        # Should be the same work we get from /works/W1492801337
        work_id = response['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        assert work_id == 'W1492801337'
    
    def test_direct_author_id_url(self):
        """Test direct author ID URL: http://localhost:9020/A5102590311"""
        response = self._make_request("/A5102590311")
        
        # Should return an author object
        assert 'id' in response
        assert 'display_name' in response
        
        # Should be the same author we get from /authors/A5102590311
        author_id = response['id']
        if author_id.startswith('https://openalex.org/'):
            author_id = author_id.split('/')[-1]
        assert author_id == 'A5102590311'
    
    def test_direct_concept_id_url(self):
        """Test direct concept ID URL for a concept ID"""
        # First let's get a valid concept ID from the concepts endpoint
        concepts_response = self._make_request("/concepts?limit=1")
        if concepts_response.get('results'):
            concept = concepts_response['results'][0]
            concept_id = concept['id']
            if concept_id.startswith('https://openalex.org/'):
                concept_id = concept_id.split('/')[-1]
            
            # Now test direct access
            response = self._make_request(f"/{concept_id}")
            
            # Should return a concept object
            assert 'id' in response
            assert 'display_name' in response
            
            # Should be the same concept
            response_id = response['id']
            if response_id.startswith('https://openalex.org/'):
                response_id = response_id.split('/')[-1]
            assert response_id == concept_id
        else:
            pytest.skip("No concepts available in database")
    
    def test_direct_institution_id_url(self):
        """Test direct institution ID URL"""
        # First let's get a valid institution ID 
        institutions_response = self._make_request("/institutions?limit=1")
        if institutions_response.get('results'):
            institution = institutions_response['results'][0]
            institution_id = institution['id']
            if institution_id.startswith('https://openalex.org/'):
                institution_id = institution_id.split('/')[-1]
            
            # Now test direct access
            response = self._make_request(f"/{institution_id}")
            
            # Should return an institution object
            assert 'id' in response
            assert 'display_name' in response
            
            # Should be the same institution
            response_id = response['id']
            if response_id.startswith('https://openalex.org/'):
                response_id = response_id.split('/')[-1]
            assert response_id == institution_id
        else:
            pytest.skip("No institutions available in database")
    
    def test_direct_doi_url(self):
        """Test direct DOI URL: http://localhost:9020/doi:10.1007/978-3-540-24614-5_17"""
        response = self._make_request("/doi:10.1007/978-3-540-24614-5_17")
        
        # Should return a work object (DOI maps to works)
        assert 'id' in response
        assert 'title' in response
        
        # Should be the expected work
        work_id = response['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        assert work_id == 'W1492801337'
    
    def test_direct_mag_url(self):
        """Test direct MAG URL: http://localhost:9020/mag:1492801337"""
        response = self._make_request("/mag:1492801337")
        
        # Should return a work object (MAG maps to works)
        assert 'id' in response
        assert 'title' in response
        
        # Should be the expected work
        work_id = response['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        assert work_id == 'W1492801337'
    
    def test_direct_url_with_select_parameter(self):
        """Test direct URL with select parameter"""
        response = self._make_request("/W1492801337?select=id,title")
        
        # Should only have the selected fields
        assert 'id' in response
        assert 'title' in response
        # Should not have other fields like 'abstract'
        assert 'abstract_inverted_index' not in response
    
    def test_direct_url_404_for_invalid_id(self):
        """Test that invalid direct IDs return 404"""
        self._make_request("/W9999999999", expected_status=404)
    
    def test_direct_url_precedence(self):
        """Test that existing routes take precedence over direct ID matching"""
        # /works should still work as the list endpoint
        response = self._make_request("/works?limit=1")
        assert 'results' in response
        assert 'meta' in response


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
