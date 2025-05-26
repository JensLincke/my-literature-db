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


class TopicsFilterParams:
    """Filter parameters for topics"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter topics by name (case-insensitive partial match)",
            example="artificial intelligence"
        )
    ):
        self.name = name


class FieldsFilterParams:
    """Filter parameters for fields"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter fields by name (case-insensitive partial match)",
            example="Computer Science"
        )
    ):
        self.name = name


class SubfieldsFilterParams:
    """Filter parameters for subfields"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter subfields by name (case-insensitive partial match)",
            example="Machine Learning"
        ),
        field: Optional[str] = Query(
            None,
            description="Filter subfields by parent field ID",
            example="F15741424"
        )
    ):
        self.name = name
        self.field = field


class DomainsFilterParams:
    """Filter parameters for domains"""
    def __init__(
        self,
        name: Optional[str] = Query(
            None,
            description="Filter domains by name (case-insensitive partial match)",
            example="Natural Sciences"
        )
    ):
        self.name = name


# Documentation helper functions
def entity_list_description(entity_name: str) -> str:
    """Generate consistent description for entity list endpoints"""
    common_filter_examples = """
#### Common Filter Syntax
- Basic equality: `filter=publication_year:2020`
- Comparison: `filter=publication_year:>2018`, `filter=cited_by_count:>=100`
- Text search: `filter=display_name.search:neural+networks`
- Exact match: `filter=display_name.equals:Neural Networks`
- Multiple values (OR): `filter=publication_year:2020|2021|2022` 
- Multiple filters (AND): `filter=publication_year:>2018,cited_by_count:>10`

#### Sorting
- Sort by citation count: `sort=cited_by_count:desc`
- Sort by publication year: `sort=publication_year:desc` or `sort=publication_year:asc`
- Sort by name: `sort=display_name:asc`
- Multiple sort criteria: `sort=publication_year:desc,cited_by_count:desc`

#### Field Selection
- Select specific fields: `select=id,title,publication_year,cited_by_count`
- Select fields in nested objects: `select=id,authorships.author.display_name`
"""

    work_filter_examples = """
#### Works-specific Filters
- By type: `filter=type:journal-article`
- By open access status: `filter=is_oa:true`
- By DOI presence: `filter=has_doi:true`
- By citation count: `filter=cited_by_count:>100`
- By author: `filter=authorships.author.id:A12345`
- By institution: `filter=institutions.id:I12345`
- By concept: `filter=concepts.id:C12345`
- By publication year range: `filter=publication_year:>=2018,publication_year:<=2022`
"""

    author_filter_examples = """
#### Author-specific Filters
- By name: `filter=display_name.search:Smith`
- By institution: `filter=last_known_institution.id:I12345` 
- By works count: `filter=works_count:>50`
- By citation count: `filter=cited_by_count:>1000`
"""

    concept_filter_examples = """
#### Concept-specific Filters
- By name: `filter=display_name.search:learning`
- By level: `filter=level:1` (0=root to 5=specific)
- By works count: `filter=works_count:>1000`
"""

    institution_filter_examples = """
#### Institution-specific Filters
- By name: `filter=display_name.search:Harvard`
- By country: `filter=country_code:us`
- By type: `filter=type:education`
- By works count: `filter=works_count:>5000`
"""
    
    if entity_name == "works":
        return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted. {common_filter_examples} {work_filter_examples}"
    elif entity_name == "authors":
        return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted. {common_filter_examples} {author_filter_examples}"
    elif entity_name == "concepts":
        return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted. {common_filter_examples} {concept_filter_examples}"
    elif entity_name == "institutions":
        return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted. {common_filter_examples} {institution_filter_examples}" 
    
    return f"Returns a paginated list of academic {entity_name} that can be filtered and sorted. {common_filter_examples}"


def entity_get_description(entity_name: str) -> str:
    """Generate consistent description for entity detail endpoints"""
    return f"""Get detailed information about a specific {entity_name}.
    
#### Field Selection
You can specify which fields to return using the `select` parameter:

`/{entity_name.lower()}s/W12345?select=id,title,publication_year,cited_by_count`

This will return only the specified fields for the entity."""


def entity_search_description(entity_name: str) -> str:
    """Generate consistent description for entity search endpoints"""
    search_description = f"Search {entity_name} using MongoDB text search with relevance ranking."
    param_examples = """
#### Search Parameters

**Filters:** Narrow down search results with filters:
`/search?q=neural networks&filter=publication_year:>2018,cited_by_count:>100`

**Sorting:** Control the order of results (default is by relevance):
`/search?q=neural networks&sort=cited_by_count:desc`

**Field Selection:** Control which fields are returned:
`/search?q=neural networks&select=id,title,publication_year,authorships`

**Combined Example:**
`/search?q=neural networks&filter=publication_year:>2018&sort=cited_by_count:desc&select=id,title,cited_by_count`
"""
    
    if entity_name in ["works", "authors", "concepts"]:
        return f"{search_description} {param_examples}"
    
    return search_description


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
