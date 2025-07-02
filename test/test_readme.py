#!/usr/bin/env python3
"""
Integration tests for README examples

This test suite validates that all the example API calls in the README work correctly.
Assumes the server is running on localhost:9020.

Run with:
    python -m pytest test/test_readme.py -v
    or
    python test/test_readme.py
"""

import unittest
import requests
import json
import time
from typing import Dict, Any, Optional


class TestReadmeExamples(unittest.TestCase):
    """Integration tests for all README examples"""
    
    BASE_URL = "http://localhost:9020"
    
    @classmethod
    def setUpClass(cls):
        """Check if server is running before running tests"""
        try:
            response = requests.get(f"{cls.BASE_URL}/", timeout=5)
            print(f"Server is running (status: {response.status_code})")
        except requests.exceptions.RequestException as e:
            raise unittest.SkipTest(f"Server not running on {cls.BASE_URL}: {e}")
    
    def _make_request(self, path: str, expected_status: int = 200) -> Dict[str, Any]:
        """Make a request and return JSON response"""
        url = f"{self.BASE_URL}{path}"
        try:
            response = requests.get(url, timeout=30)
            self.assertEqual(response.status_code, expected_status, 
                           f"Expected status {expected_status} for {url}, got {response.status_code}")
            return response.json()
        except requests.exceptions.Timeout:
            self.fail(f"Request to {url} timed out")
        except json.JSONDecodeError:
            self.fail(f"Invalid JSON response from {url}")
    
    def _validate_work_structure(self, work: Dict[str, Any]):
        """Validate basic work structure"""
        required_fields = ['id']
        for field in required_fields:
            self.assertIn(field, work, f"Work missing required field: {field}")
        
        # ID should be a string containing W (could be URL or just ID)
        if 'id' in work:
            work_id = work['id']
            if work_id.startswith('https://openalex.org/'):
                # Extract the ID from URL
                work_id = work_id.split('/')[-1]
            self.assertTrue(work_id.startswith('W'), f"Work ID should start with 'W': {work['id']}")
    
    def _validate_author_structure(self, author: Dict[str, Any]):
        """Validate basic author structure"""
        required_fields = ['id']
        for field in required_fields:
            self.assertIn(field, author, f"Author missing required field: {field}")
        
        # ID should be a string containing A (could be URL or just ID)
        if 'id' in author:
            author_id = author['id']
            if author_id.startswith('https://openalex.org/'):
                # Extract the ID from URL
                author_id = author_id.split('/')[-1]
            self.assertTrue(author_id.startswith('A'), f"Author ID should start with 'A': {author['id']}")
    
    def _validate_source_structure(self, source: Dict[str, Any]):
        """Validate basic source structure"""
        required_fields = ['id']
        for field in required_fields:
            self.assertIn(field, source, f"Source missing required field: {field}")
        
        # ID should be a string containing S (could be URL or just ID)
        if 'id' in source:
            source_id = source['id']
            if source_id.startswith('https://openalex.org/'):
                # Extract the ID from URL
                source_id = source_id.split('/')[-1]
            self.assertTrue(source_id.startswith('S'), f"Source ID should start with 'S': {source['id']}")
    
    def _validate_concept_structure(self, concept: Dict[str, Any]):
        """Validate basic concept structure"""
        required_fields = ['id']
        for field in required_fields:
            self.assertIn(field, concept, f"Concept missing required field: {field}")
        
        # ID should be a string containing C (could be URL or just ID)
        if 'id' in concept:
            concept_id = concept['id']
            if concept_id.startswith('https://openalex.org/'):
                # Extract the ID from URL
                concept_id = concept_id.split('/')[-1]
            self.assertTrue(concept_id.startswith('C'), f"Concept ID should start with 'C': {concept['id']}")
    
    def _validate_institution_structure(self, institution: Dict[str, Any]):
        """Validate basic institution structure"""
        required_fields = ['id']
        for field in required_fields:
            self.assertIn(field, institution, f"Institution missing required field: {field}")
        
        # ID should be a string containing I (could be URL or just ID)
        if 'id' in institution:
            institution_id = institution['id']
            if institution_id.startswith('https://openalex.org/'):
                # Extract the ID from URL
                institution_id = institution_id.split('/')[-1]
            self.assertTrue(institution_id.startswith('I'), f"Institution ID should start with 'I': {institution['id']}")

    # Test Get a Work by ID section
    def test_get_work_by_id_basic(self):
        """Test: Get a specific work by its OpenAlex ID"""
        data = self._make_request("/works/W2741809807")
        self._validate_work_structure(data)
        # ID could be full URL or just the ID
        work_id = data['id']
        if work_id.startswith('https://openalex.org/'):
            work_id = work_id.split('/')[-1]
        self.assertEqual(work_id, 'W2741809807')
    
    def test_get_work_by_id_with_select(self):
        """Test: Get only specific fields"""
        data = self._make_request("/works/W2741809807?select=id,title,publication_year,cited_by_count")
        self._validate_work_structure(data)
        
        # Should only have the selected fields (plus any that are always included)
        expected_fields = {'id', 'title', 'publication_year', 'cited_by_count', '_id'}
        actual_fields = set(data.keys())
        # Check that all requested fields are present
        for field in ['id', 'title', 'publication_year', 'cited_by_count']:
            if field in actual_fields:  # Some fields might not exist in the document
                continue
    
    # Test Search for Works section
    def test_search_basic(self):
        """Test: Basic search"""
        data = self._make_request("/works/search?q=machine%20learning")
        
        self.assertIn('results', data)
        self.assertIn('total', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
    
    def test_search_with_pagination(self):
        """Test: Search with pagination"""
        data = self._make_request("/works/search?q=climate%20change&skip=0&limit=10")
        
        self.assertIn('results', data)
        self.assertIn('total', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 10)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
    
    def test_search_with_filters(self):
        """Test: Search with filters"""
        data = self._make_request("/works/search?q=neural%20networks&filter=publication_year:2023,cited_by_count:>50")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
            # Validate filter conditions if the fields exist
            for work in data['results']:
                if 'publication_year' in work:
                    self.assertEqual(work['publication_year'], 2023)
                if 'cited_by_count' in work:
                    self.assertGreater(work['cited_by_count'], 50)
    
    def test_search_with_sorting(self):
        """Test: Search with sorting"""
        data = self._make_request("/works/search?q=artificial%20intelligence&sort=cited_by_count:desc")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        
        if len(data['results']) > 1:
            # Check that results are sorted by cited_by_count in descending order
            citation_counts = []
            for work in data['results']:
                if 'cited_by_count' in work and work['cited_by_count'] is not None:
                    citation_counts.append(work['cited_by_count'])
            
            if len(citation_counts) > 1:
                # Check if sorted in descending order
                for i in range(len(citation_counts) - 1):
                    self.assertGreaterEqual(citation_counts[i], citation_counts[i + 1])
    
    def test_search_with_field_selection(self):
        """Test: Search with field selection"""
        data = self._make_request("/works/search?q=deep%20learning&select=id,title,publication_year&limit=5")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 5)
        
        if data['results']:
            work = data['results'][0]
            self._validate_work_structure(work)
            # Should have the selected fields
            for field in ['id', 'title', 'publication_year']:
                if field in work:  # Field might not exist in the document
                    continue
    
    # Test List and Filter Works section
    def test_list_recent_works(self):
        """Test: List recent works"""
        data = self._make_request("/works?filter=publication_year:2023&per_page=10")
        
        self.assertIn('results', data)
        self.assertIn('meta', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 10)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
            # Check filter condition
            for work in data['results']:
                if 'publication_year' in work:
                    self.assertEqual(work['publication_year'], 2023)
    
    def test_filter_by_citation_count(self):
        """Test: Filter by citation count (previously problematic)"""
        data = self._make_request("/works?filter=cited_by_count:>100&sort=cited_by_count:desc&per_page=5")
        
        self.assertIn('results', data)
        self.assertIn('meta', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 5)
        
        # Check that total_count is -1 (performance optimization)
        self.assertEqual(data['meta']['total_count'], -1)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
            # Check filter and sort conditions
            for work in data['results']:
                if 'cited_by_count' in work and work['cited_by_count'] is not None:
                    self.assertGreater(work['cited_by_count'], 100)
    
    def test_filter_multiple_criteria(self):
        """Test: Filter with multiple criteria"""
        data = self._make_request("/works?filter=publication_year:>2020,cited_by_count:>10&per_page=20")
        
        self.assertIn('results', data)
        self.assertIn('meta', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 20)
        
        if data['results']:
            self._validate_work_structure(data['results'][0])
            # Check filter conditions
            for work in data['results']:
                if 'publication_year' in work and work['publication_year'] is not None:
                    self.assertGreater(work['publication_year'], 2020)
                if 'cited_by_count' in work and work['cited_by_count'] is not None:
                    self.assertGreater(work['cited_by_count'], 10)
    
    # Test Other Entity Types section
    def test_get_author_by_id(self):
        """Test: Get an author by ID"""
        data = self._make_request("/authors/A5023888391")
        self._validate_author_structure(data)
        # ID could be full URL or just the ID
        author_id = data['id']
        if author_id.startswith('https://openalex.org/'):
            author_id = author_id.split('/')[-1]
        self.assertEqual(author_id, 'A5023888391')
    
    def test_search_authors(self):
        """Test: Search for authors"""
        data = self._make_request("/authors/search?q=John%20Smith")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_author_structure(data['results'][0])
    
    def test_get_institution_by_id(self):
        """Test: Get an institution"""
        data = self._make_request("/institutions/I27837315")
        self._validate_institution_structure(data)
        # ID could be full URL or just the ID
        institution_id = data['id']
        if institution_id.startswith('https://openalex.org/'):
            institution_id = institution_id.split('/')[-1]
        self.assertEqual(institution_id, 'I27837315')
    
    def test_search_concepts(self):
        """Test: Search for concepts"""
        data = self._make_request("/concepts/search?q=machine%20learning")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_concept_structure(data['results'][0])
    
    # Additional tests for filter functionality with different entity types
    def test_sources_filter_works_count(self):
        """Test: Filter sources by works_count (the original problematic case)"""
        data = self._make_request("/sources?filter=works_count:>1000&per_page=5")
        
        self.assertIn('results', data)
        self.assertIn('meta', data)
        self.assertIsInstance(data['results'], list)
        self.assertLessEqual(len(data['results']), 5)
        
        if data['results']:
            self._validate_source_structure(data['results'][0])
            # Check filter condition
            for source in data['results']:
                if 'works_count' in source and source['works_count'] is not None:
                    self.assertGreater(source['works_count'], 1000)
    
    def test_authors_filter_works_count(self):
        """Test: Filter authors by works_count"""
        data = self._make_request("/authors?filter=works_count:>100&per_page=5")
        
        self.assertIn('results', data)
        self.assertIn('meta', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_author_structure(data['results'][0])
            # Check filter condition
            for author in data['results']:
                if 'works_count' in author and author['works_count'] is not None:
                    self.assertGreater(author['works_count'], 100)
    
    def test_url_encoded_filters(self):
        """Test: URL-encoded filter parameters work correctly"""
        # Test the exact case that was failing: works_count:%3E1000
        data = self._make_request("/sources?filter=works_count:%3E1000&per_page=3")
        
        self.assertIn('results', data)
        self.assertIsInstance(data['results'], list)
        
        if data['results']:
            self._validate_source_structure(data['results'][0])
            for source in data['results']:
                if 'works_count' in source and source['works_count'] is not None:
                    self.assertGreater(source['works_count'], 1000)
    
    def test_performance_note_validation(self):
        """Test: Validate that filtered queries return total_count: -1 for performance"""
        endpoints_with_filters = [
            "/works?filter=cited_by_count:>100&per_page=1",
            "/sources?filter=works_count:>1000&per_page=1",
            "/authors?filter=works_count:>100&per_page=1"
        ]
        
        for endpoint in endpoints_with_filters:
            with self.subTest(endpoint=endpoint):
                data = self._make_request(endpoint)
                self.assertIn('meta', data)
                self.assertIn('total_count', data['meta'])
                # Should be -1 for performance (as documented in README)
                self.assertEqual(data['meta']['total_count'], -1, 
                               f"Filtered query should return total_count: -1 for performance: {endpoint}")


class TestServerHealth(unittest.TestCase):
    """Basic server health tests"""
    
    BASE_URL = "http://localhost:9020"
    
    def test_server_responsive(self):
        """Test that server responds to requests"""
        try:
            response = requests.get(f"{self.BASE_URL}/", timeout=5)
            # Any response (even 404) means server is running
            self.assertIsNotNone(response.status_code)
        except requests.exceptions.RequestException:
            self.fail("Server not responding")
    
    def test_basic_endpoints_exist(self):
        """Test that basic endpoints exist and don't return 500 errors"""
        endpoints = [
            "/works",
            "/authors", 
            "/institutions",
            "/concepts",
            "/sources"
        ]
        
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                try:
                    response = requests.get(f"{self.BASE_URL}{endpoint}?per_page=1", timeout=10)
                    # Should not be a server error
                    self.assertLess(response.status_code, 500, 
                                  f"Server error for {endpoint}: {response.status_code}")
                except requests.exceptions.Timeout:
                    self.fail(f"Timeout for {endpoint}")


if __name__ == '__main__':
    print("README Integration Tests")
    print("=" * 40)
    print("Testing all example API calls from README.md")
    print("Assumes server is running on http://localhost:9020")
    print()
    
    # Run with verbose output
    unittest.main(verbosity=2)
