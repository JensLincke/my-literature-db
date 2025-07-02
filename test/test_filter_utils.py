#!/usr/bin/env python3
"""
Unit tests for filter_utils.py

Run with:
    python -m pytest test/test_filter_utils.py -v
    or
    python -m unittest test.test_filter_utils -v
"""

import unittest
import sys
import os

# Add the bin directory to the Python path
project_root = os.path.dirname(os.path.dirname(__file__))
bin_path = os.path.join(project_root, 'bin')
sys.path.insert(0, bin_path)

from filter_utils import (
    parse_filter_param, 
    parse_filter_expression, 
    parse_filter_value, 
    build_mongodb_query,
    FILTER_OPERATIONS
)
from urllib.parse import unquote


class TestFilterUtils(unittest.TestCase):
    """Test cases for filter parsing utilities"""

    def test_url_decoding(self):
        """Test URL decoding of filter parameters"""
        test_cases = [
            ('works_count:%3E1000', 'works_count:>1000'),
            ('publication_year:%3E2020', 'publication_year:>2020'),
            ('cited_by_count:%3C%3D100', 'cited_by_count:<=100'),
            ('display_name.search:neural%20networks', 'display_name.search:neural networks')
        ]
        
        for encoded, expected in test_cases:
            with self.subTest(encoded=encoded):
                decoded = unquote(encoded)
                self.assertEqual(decoded, expected)

    def test_parse_filter_value_numeric(self):
        """Test parsing numeric field values"""
        test_cases = [
            ('works_count', '1000', 1000),
            ('publication_year', '2023', 2023),
            ('cited_by_count', '100', 100),
            ('h_index', '50', 50)
        ]
        
        for field_name, value_str, expected in test_cases:
            with self.subTest(field=field_name, value=value_str):
                result = parse_filter_value(field_name, value_str)
                self.assertEqual(result, expected)
                self.assertIsInstance(result, int)

    def test_parse_filter_value_boolean(self):
        """Test parsing boolean field values"""
        test_cases = [
            ('is_oa', 'true', True),
            ('is_oa', 'false', False),
            ('has_doi', 'True', True),
            ('has_pdf', 'False', False),
            ('is_retracted', '1', True),
            ('is_retracted', '0', False)
        ]
        
        for field_name, value_str, expected in test_cases:
            with self.subTest(field=field_name, value=value_str):
                result = parse_filter_value(field_name, value_str)
                self.assertEqual(result, expected)
                self.assertIsInstance(result, bool)

    def test_parse_filter_value_string(self):
        """Test parsing string field values"""
        test_cases = [
            ('display_name', 'test name', 'test name'),
            ('title', 'research paper', 'research paper'),
            ('type', 'journal-article', 'journal-article')
        ]
        
        for field_name, value_str, expected in test_cases:
            with self.subTest(field=field_name, value=value_str):
                result = parse_filter_value(field_name, value_str)
                self.assertEqual(result, expected)
                self.assertIsInstance(result, str)

    def test_parse_filter_expression_comparison(self):
        """Test parsing comparison filter expressions"""
        test_cases = [
            ('works_count:>1000', 'works_count', 'gt', 1000),
            ('publication_year:>=2020', 'publication_year', 'gte', 2020),
            ('cited_by_count:<100', 'cited_by_count', 'lt', 100),
            ('cited_by_count:<=50', 'cited_by_count', 'lte', 50),
            ('works_count:!=0', 'works_count', 'ne', 0)
        ]
        
        for expr, expected_field, expected_op, expected_value in test_cases:
            with self.subTest(expression=expr):
                field, operation, value = parse_filter_expression(expr)
                self.assertEqual(field, expected_field)
                self.assertEqual(operation, expected_op)
                self.assertEqual(value, expected_value)

    def test_parse_filter_expression_equality(self):
        """Test parsing equality filter expressions"""
        test_cases = [
            ('publication_year:2023', 'publication_year', 'eq', 2023),
            ('type:journal-article', 'type', 'eq', 'journal-article'),
            ('is_oa:true', 'is_oa', 'eq', True)
        ]
        
        for expr, expected_field, expected_op, expected_value in test_cases:
            with self.subTest(expression=expr):
                field, operation, value = parse_filter_expression(expr)
                self.assertEqual(field, expected_field)
                self.assertEqual(operation, expected_op)
                self.assertEqual(value, expected_value)

    def test_parse_filter_expression_search(self):
        """Test parsing search filter expressions"""
        test_cases = [
            ('display_name.search:neural networks', 'display_name', 'search', 'neural networks'),
            ('title.search:machine learning', 'title', 'search', 'machine learning')
        ]
        
        for expr, expected_field, expected_op, expected_value in test_cases:
            with self.subTest(expression=expr):
                field, operation, value = parse_filter_expression(expr)
                self.assertEqual(field, expected_field)
                self.assertEqual(operation, expected_op)
                self.assertEqual(value, expected_value)

    def test_build_mongodb_query_comparison(self):
        """Test building MongoDB queries for comparison operations"""
        test_cases = [
            ('works_count', 'gt', 1000, {'works_count': {'$gt': 1000}}),
            ('publication_year', 'gte', 2020, {'publication_year': {'$gte': 2020}}),
            ('cited_by_count', 'lt', 100, {'cited_by_count': {'$lt': 100}}),
            ('cited_by_count', 'lte', 50, {'cited_by_count': {'$lte': 50}}),
            ('works_count', 'ne', 0, {'works_count': {'$ne': 0}})
        ]
        
        for field, operation, value, expected in test_cases:
            with self.subTest(field=field, operation=operation, value=value):
                result = build_mongodb_query(field, operation, value)
                self.assertEqual(result, expected)

    def test_build_mongodb_query_equality(self):
        """Test building MongoDB queries for equality operations"""
        test_cases = [
            ('publication_year', 'eq', 2023, {'publication_year': {'$eq': 2023}}),
            ('type', 'eq', 'journal-article', {'type': {'$eq': 'journal-article'}}),
            # Note: is_oa has special handling - it maps to open_access.is_oa without $eq operator
            ('is_oa', 'eq', True, {'open_access.is_oa': True})
        ]
        
        for field, operation, value, expected in test_cases:
            with self.subTest(field=field, operation=operation, value=value):
                result = build_mongodb_query(field, operation, value)
                self.assertEqual(result, expected)

    def test_build_mongodb_query_search(self):
        """Test building MongoDB queries for search operations"""
        test_cases = [
            ('display_name', 'search', 'neural networks', 
             {'display_name': {'$regex': 'neural networks', '$options': 'i'}}),
            ('title', 'search', 'machine learning', 
             {'title': {'$regex': 'machine learning', '$options': 'i'}})
        ]
        
        for field, operation, value, expected in test_cases:
            with self.subTest(field=field, operation=operation, value=value):
                result = build_mongodb_query(field, operation, value)
                self.assertEqual(result, expected)

    def test_parse_filter_param_single(self):
        """Test parsing single filter parameters"""
        test_cases = [
            ('works_count:>1000', {'works_count': {'$gt': 1000}}),
            ('publication_year:2023', {'publication_year': {'$eq': 2023}}),
            ('cited_by_count:<100', {'cited_by_count': {'$lt': 100}}),
            # Note: is_oa maps to open_access.is_oa in the actual MongoDB structure
            ('is_oa:true', {'open_access.is_oa': True})
        ]
        
        for filter_param, expected in test_cases:
            with self.subTest(filter_param=filter_param):
                result = parse_filter_param(filter_param)
                self.assertEqual(result, expected)

    def test_parse_filter_param_url_encoded(self):
        """Test parsing URL-encoded filter parameters"""
        test_cases = [
            ('works_count:%3E1000', {'works_count': {'$gt': 1000}}),
            ('publication_year:%3E%3D2020', {'publication_year': {'$gte': 2020}}),
            ('cited_by_count:%3C100', {'cited_by_count': {'$lt': 100}})
        ]
        
        for filter_param, expected in test_cases:
            with self.subTest(filter_param=filter_param):
                result = parse_filter_param(filter_param)
                self.assertEqual(result, expected)

    def test_parse_filter_param_multiple(self):
        """Test parsing multiple filter parameters"""
        test_cases = [
            ('works_count:>1000,publication_year:2023', 
             {'works_count': {'$gt': 1000}, 'publication_year': {'$eq': 2023}}),
            # Note: is_oa maps to open_access.is_oa in the actual MongoDB structure
            ('cited_by_count:>50,is_oa:true', 
             {'cited_by_count': {'$gt': 50}, 'open_access.is_oa': True})
        ]
        
        for filter_param, expected in test_cases:
            with self.subTest(filter_param=filter_param):
                result = parse_filter_param(filter_param)
                self.assertEqual(result, expected)

    def test_problematic_case_from_log(self):
        """Test the specific case that was failing in the server log"""
        # This is the exact case from the server log that was producing the wrong query
        filter_param = 'works_count:%3E1000'
        
        # This should NOT produce {'works_count:': {'$gt': '1000'}}
        # It should produce {'works_count': {'$gt': 1000}}
        result = parse_filter_param(filter_param)
        
        expected = {'works_count': {'$gt': 1000}}
        self.assertEqual(result, expected)
        
        # Verify the field name doesn't have a trailing colon
        field_names = list(result.keys())
        self.assertTrue(len(field_names) > 0)
        first_field = field_names[0]
        self.assertFalse(first_field.endswith(':'), 
                        f"Field name '{first_field}' should not end with colon")
        
        # Verify the value is an integer, not a string
        if '$gt' in result[first_field]:
            value = result[first_field]['$gt']
            self.assertIsInstance(value, int, 
                                f"Value should be int, got {type(value)}: {value}")
            self.assertEqual(value, 1000)

    def test_empty_and_invalid_filters(self):
        """Test handling of empty and invalid filter parameters"""
        test_cases = [
            ('', {}),
            (None, {}),
            ('invalid_expression', {}),
            ('field_without_operator', {}),
            (':', {})
        ]
        
        for filter_param, expected in test_cases:
            with self.subTest(filter_param=filter_param):
                result = parse_filter_param(filter_param)
                self.assertEqual(result, expected)


class TestFilterOperations(unittest.TestCase):
    """Test the FILTER_OPERATIONS constant and related logic"""
    
    def test_filter_operations_completeness(self):
        """Test that all expected filter operations are defined"""
        expected_operations = {
            ':': 'eq',
            '>': 'gt',
            '<': 'lt',
            '>=': 'gte',
            '<=': 'lte',
            '!=': 'ne',
            '.search:': 'search',
            '.equals:': 'exact'
        }
        
        self.assertEqual(FILTER_OPERATIONS, expected_operations)

    def test_operation_precedence(self):
        """Test that multi-character operations are checked before single-character ones"""
        # This tests that '>=' is matched before '>', etc.
        test_cases = [
            'field:>=100',  # Should match '>=' not '>'
            'field:<=50',   # Should match '<=' not '<'
            'field:!=0'     # Should match '!=' not just check for ':'
        ]
        
        for expr in test_cases:
            with self.subTest(expression=expr):
                field, operation, value = parse_filter_expression(expr)
                if '>=' in expr:
                    self.assertEqual(operation, 'gte')
                elif '<=' in expr:
                    self.assertEqual(operation, 'lte')
                elif '!=' in expr:
                    self.assertEqual(operation, 'ne')


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)
