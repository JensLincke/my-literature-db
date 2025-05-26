"""
Filter utilities for the OpenAlex Local API

This module provides filter parsing utilities for the API that mimic
the filtering functionality of the official OpenAlex API.

Filter syntax examples:
- filter=publication_year:2020
- filter=type:journal-article
- filter=publication_year:>2018
- filter=cited_by_count:>100
- filter=display_name.search:neural+networks
- filter=institutions.country_code:us
"""

from typing import Dict, Any, List, Optional, Tuple
import re
from urllib.parse import unquote

# Constants for filter operations
FILTER_OPERATIONS = {
    ":": "eq",  # Equals
    ">": "gt",  # Greater than
    "<": "lt",  # Less than
    ">=": "gte",  # Greater than or equal
    "<=": "lte",  # Less than or equal
    "!=": "ne",  # Not equal
    ".search:": "search",  # Text search (regex)
    ".equals:": "exact"  # Exact match
}

# Filter type mapping (to convert string values to appropriate types)
FILTER_TYPE_MAPPING = {
    "publication_year": int,
    "cited_by_count": int,
    "works_count": int,
    "level": int,
    "cited_by_count": int,
    "h_index": int,
    "i10_index": int,
    "counts": int,
    "year": int,
    "volume": int,
    "issue": int,
}

# Boolean fields that should be converted from strings to booleans
BOOLEAN_FIELDS = [
    "has_doi", 
    "has_pdf", 
    "has_references", 
    "is_oa",
    "is_retracted",
    "has_fulltext",
    "is_paratext"
]

def parse_filter_value(field_name: str, value: str) -> Any:
    """Convert filter value to appropriate type based on field name"""
    # Handle boolean fields
    if field_name in BOOLEAN_FIELDS or any(field_name.endswith(f".{bf}") for bf in BOOLEAN_FIELDS):
        # Convert string values to boolean
        return str(value).lower() in ["true", "1", "yes", "t"]
        
    # Handle null/none values
    if value.lower() in ["null", "none"]:
        return None
    
    # Handle numeric fields
    type_converter = FILTER_TYPE_MAPPING.get(field_name)
    if type_converter:
        try:
            return type_converter(value)
        except ValueError:
            # If conversion fails, return original value
            pass
    
    # Special case for IDs - might want to extract IDs from OpenAlex URLs
    if field_name.endswith(".id") or field_name == "id":
        # Extract ID from URLs if needed (e.g. https://openalex.org/W12345 -> W12345)
        if "/" in value:
            return value.split("/")[-1]
    
    # Default case - return the value as-is
    return value

def parse_filter_expression(filter_expr: str) -> Tuple[str, str, Any]:
    """
    Parse a single filter expression into field, operation, and value.
    
    Example: "publication_year:>2018" -> ("publication_year", "gt", 2018)
    """
    # Check for special operations first
    for op_str, op_name in FILTER_OPERATIONS.items():
        if op_str in filter_expr and op_str != ":":  # Special case for colon
            field_name, value = filter_expr.split(op_str, 1)
            value = unquote(value.replace("+", " "))
            
            # Handle pipe-delimited values (OR conditions)
            if "|" in value:
                # Parse each value in the pipe-delimited list
                values = [parse_filter_value(field_name, v.strip()) for v in value.split("|")]
                return field_name, op_name, values
                
            return field_name, op_name, parse_filter_value(field_name, value)
    
    # Default case is equality with colon
    if ":" in filter_expr:
        field_name, value = filter_expr.split(":", 1)
        value = unquote(value.replace("+", " "))
        
        # Handle pipe-delimited values (OR conditions)
        if "|" in value:
            # Parse each value in the pipe-delimited list
            values = [parse_filter_value(field_name, v.strip()) for v in value.split("|")]
            return field_name, "eq", values
            
        return field_name, "eq", parse_filter_value(field_name, value)
    
    # Invalid expression
    return "", "", None

def build_mongodb_query(field: str, operation: str, value: Any) -> Dict:
    """
    Convert a parsed filter expression to a MongoDB query.
    
    Example: ("publication_year", "gt", 2018) -> {"publication_year": {"$gt": 2018}}
    """
    # Handle array values (OR conditions from pipe-delimited values)
    if isinstance(value, list):
        # Create a list of individual conditions to be combined with $or
        or_conditions = []
        for individual_value in value:
            # Build a query for each value and add to the OR conditions
            single_value_query = build_mongodb_query(field, operation, individual_value)
            or_conditions.append(single_value_query)
        
        # Return the $or query
        return {"$or": or_conditions}
    
    # Special case for search operation
    if operation == "search":
        return {field.replace(".search", ""): {"$regex": value, "$options": "i"}}
    
    # Special case for exact operation
    if operation == "exact":
        return {field.replace(".equals", ""): value}
    
    # Special case for null/None value
    if value is None:
        if operation == "eq":
            return {field: None}
        elif operation == "ne":
            return {field: {"$ne": None}}
    
    # Standard comparison operations
    op_map = {
        "eq": "$eq",
        "gt": "$gt",
        "lt": "$lt",
        "gte": "$gte",
        "lte": "$lte",
        "ne": "$ne"
    }
    
    mongo_op = op_map.get(operation, "$eq")
    
    # Handle common OpenAlex patterns
    
    # Handle authorships fields (e.g., "authorships.author.id")
    if field.startswith("authorships."):
        parts = field.split(".")
        if len(parts) >= 3:
            # Use the $elemMatch operator for array field matching
            return {"authorships": {"$elemMatch": {".".join(parts[1:]): {mongo_op: value}}}}
    
    # Handle institutions fields
    if field.startswith("institutions."):
        parts = field.split(".")
        if len(parts) >= 2:
            # For fields like institutions.country_code:us
            return {f"institutions": {"$elemMatch": {parts[1]: {mongo_op: value}}}}
    
    # Handle concepts fields
    if field.startswith("concepts."):
        parts = field.split(".")
        if len(parts) >= 2:
            return {f"concepts": {"$elemMatch": {parts[1]: {mongo_op: value}}}}
    
    # Handle sources fields
    if field.startswith("source."):
        parts = field.split(".")
        if len(parts) >= 2:
            # Could be in primary_location.source or locations.source
            return {"$or": [
                {f"primary_location.source.{parts[1]}": {mongo_op: value}},
                {f"locations.source.{parts[1]}": {mongo_op: value}}
            ]}
    
    # Handle other custom fields
    if field == "cited_by_count":
        # Handle citation count specially
        return {"cited_by_count": {mongo_op: value}}
    
    # Handle boolean existence fields
    if field == "has_doi":
        return {"ids.doi": {"$exists": value}}
        
    if field == "has_pdf":
        return {"open_access.is_oa": value}
        
    if field == "has_references":
        return {"referenced_works": {"$exists": value, "$ne": []}}
    
    # Handle open access fields
    if field == "is_oa" or field == "open_access.is_oa":
        return {"open_access.is_oa": value}
    
    # Handle publication date ranges
    if field == "from_publication_date":
        return {"publication_date": {mongo_op: value}}
    
    if field == "to_publication_date":
        return {"publication_date": {mongo_op: value}}
        
    # Handle language
    if field == "language":
        return {"language": value.lower()}
    
    # Handle citations (for compatibility with OpenAlex)
    if field == "cites" or field == "cites.id":
        if isinstance(value, str) and value.startswith("W"):
            # Extract the work ID if it's in OpenAlex format
            work_id = value.split("/")[-1] if "/" in value else value
            return {"referenced_works": work_id}
    
    # Handle simple dot notation without special cases
    if "." in field and not field.endswith(".search") and not field.endswith(".equals"):
        # Use regular MongoDB dot notation
        return {field: {mongo_op: value}}
            
    # Default to simple field query
    return {field: {mongo_op: value}}

def parse_filter_param(filter_param: Optional[str]) -> Dict:
    """
    Parse the filter parameter into a MongoDB query object.
    
    Supports multiple filters combined with commas.
    Example: "publication_year:>2018,cited_by_count:>10"
    """
    if not filter_param:
        return {}
    
    # Split by comma for multiple filters
    filter_expressions = filter_param.split(",")
    
    # Process each filter expression
    query = {}
    and_conditions = []
    
    for expr in filter_expressions:
        expr = expr.strip()
        if not expr:
            continue
            
        field, operation, value = parse_filter_expression(expr)
        if not field or not operation or value is None:
            continue
            
        expr_query = build_mongodb_query(field, operation, value)
        
        # Check if we need to use $and or can directly merge
        need_and = False
        for key, val in expr_query.items():
            if key in query:
                # If the same field has multiple conditions, use $and
                need_and = True
                break
                
        if need_and:
            and_conditions.append(expr_query)
        else:
            # Directly merge into the main query
            query.update(expr_query)
    
    # If we have $and conditions, add them to the query
    if and_conditions:
        if query:
            # If we already have other query conditions, add them to the $and
            and_conditions.append(query)
            query = {"$and": and_conditions}
        else:
            # If query is empty, just use the $and conditions
            query = {"$and": and_conditions}
    
    return query
