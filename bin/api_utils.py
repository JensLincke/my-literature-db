"""
API Utilities for the OpenAlex Local API

This module provides shared utilities for the FastAPI application,
including parameter models, dependency functions, and documentation helpers.
"""

from typing import Optional, Dict, Any, List, Type
from fastapi import Query, Path, Depends
from pydantic import BaseModel, Field, create_model

# Constants
MAX_RESULTS_PER_PAGE = 100

# Common parameter models
class PaginationParams:
    """Common pagination parameters"""
    def __init__(
        self,
        page: int = Query(
            1,
            description="Page number for pagination",
            gt=0,
            example=1
        ),
        per_page: int = Query(
            25,
            description="Number of results per page",
            gt=0,
            le=MAX_RESULTS_PER_PAGE,
            example=25
        ),
    ):
        self.page = page
        self.per_page = per_page


class SearchParams:
    """Common search parameters"""
    def __init__(
        self,
        q: str = Query(
            ..., 
            description="Search query. Enter terms in any order (e.g. 'John Smith 2023 machine learning') or use quotes for exact phrases"
        ),
        skip: int = Query(0, ge=0),
        limit: int = Query(10, ge=1, le=100),
        explain_score: bool = Query(False, description="Include explanation of search score")
    ):
        self.q = q
        self.skip = skip
        self.limit = limit
        self.explain_score = explain_score

# Entity-specific parameter models
class WorksFilterParams:
    """Filter parameters for works"""
    def __init__(
        self,
        title: Optional[str] = Query(
            None,
            description="Filter works by title (case-insensitive partial match)",
            example="machine learning"
        ),
        year: Optional[int] = Query(
            None,
            description="Filter works by publication year",
            example=2023,
            ge=1000,
            le=2030
        ),
        type: Optional[str] = Query(
            None,
            description="Filter works by type (e.g., 'article', 'book', 'conference-paper')",
            example="article"
        )
    ):
        self.title = title
        self.year = year
        self.type = type


class AuthorsFilterParams:
    """Filter parameters for authors"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter authors by name (case-insensitive partial match)",
            example="John Smith"
        )
    ):
        self.name = name


class ConceptsFilterParams:
    """Filter parameters for concepts"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter concepts by name (case-insensitive partial match)",
            example="machine learning"
        ),
        level: Optional[int] = Query(
            None, 
            description="Filter concepts by level (0-5, where lower is more general)",
            ge=0, 
            le=5
        )
    ):
        self.name = name
        self.level = level


class InstitutionsFilterParams:
    """Filter parameters for institutions"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter institutions by name (case-insensitive partial match)",
            example="Harvard"
        ),
        country: Optional[str] = Query(
            None,
            description="Filter institutions by country code (2-letter ISO code)",
            example="US"
        )
    ):
        self.name = name
        self.country = country


class PublishersFilterParams:
    """Filter parameters for publishers"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter publishers by name (case-insensitive partial match)",
            example="Elsevier"
        )
    ):
        self.name = name


class SourcesFilterParams:
    """Filter parameters for sources"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter sources by name (case-insensitive partial match)",
            example="Nature"
        ),
        type: Optional[str] = Query(
            None,
            description="Filter sources by type (e.g., 'journal', 'conference', 'repository')",
            example="journal"
        )
    ):
        self.name = name
        self.type = type


# Documentation helper functions
def entity_list_description(entity_name: str) -> str:
    """Generate consistent description for entity list endpoints"""
    return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted."


def entity_get_description(entity_name: str) -> str:
    """Generate consistent description for entity detail endpoints"""
    return f"Get detailed information about a specific {entity_name}."


def entity_search_description(entity_name: str) -> str:
    """Generate consistent description for entity search endpoints"""
    return f"Search {entity_name} using MongoDB text search with relevance ranking."


# Response models for documentation
class PaginatedResponse(BaseModel):
    meta: Dict[str, Any] = Field(..., example={
        "count": 25,
        "total_count": 1358,
        "page": 1,
        "per_page": 25,
        "total_pages": 55
    })
    results: List[Dict[str, Any]]


class SearchResponse(BaseModel):
    total: int = Field(..., example=1358)
    skip: int = Field(..., example=0)
    limit: int = Field(..., example=10)
    results: List[Dict[str, Any]]
    message: Optional[str] = None
