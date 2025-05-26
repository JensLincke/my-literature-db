"""
Entity router factory for the OpenAlex Local API

This module provides a factory function to create FastAPI routers for different entity types
with standardized CRUD operations.
"""

from typing import Dict, Any, Callable, Optional, Type, List
from fastapi import APIRouter, Depends, Query, Path, HTTPException
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING

from handlers import BaseEntityHandler, WorksHandler
from filter_utils import parse_filter_param
from api_utils import (
    PaginationParams, SearchParams, entity_list_description, entity_get_description,
    entity_search_description, PaginatedResponse, SearchResponse
)


class EntityRouter:
    """
    Factory for creating standardized entity routers with CRUD operations
    """
    
    def __init__(
        self, 
        router: APIRouter, 
        db: AsyncIOMotorDatabase, 
        handlers: Dict[str, BaseEntityHandler],
        entity_type: str,
        entity_name_singular: str,
        entity_name_plural: str,
        filter_params_class: Optional[Type] = None,
        sort_field: str = "works_count",
        related_entities: List[str] = None,
        jsonable_encoder: Callable = None
    ):
        self.router = router
        self.db = db
        self.handlers = handlers
        self.entity_type = entity_type
        self.entity_name_singular = entity_name_singular
        self.entity_name_plural = entity_name_plural
        self.filter_params_class = filter_params_class
        self.sort_field = sort_field
        self.related_entities = related_entities or []
        self.jsonable_encoder = jsonable_encoder
        
        # Register the standard routes
        self._register_routes()

    def _register_routes(self):
        """Register the standard routes for this entity type"""
        
        # 1. List/filter endpoint
        @self.router.get(
            f"/{self.entity_name_plural}",
            summary=f"List and search {self.entity_name_plural}",
            description=entity_list_description(self.entity_name_plural),
            response_model=PaginatedResponse
        )
        async def list_entities(
            pagination: PaginationParams = Depends(),
            filter: Optional[str] = Query(None, description="OpenAlex-style filter parameter. Examples: 'publication_year:2020', 'cited_by_count:>100'"),
            sort: Optional[str] = Query(None, description="Sort parameter. Examples: 'cited_by_count:desc', 'publication_year:asc'"),
            select: Optional[str] = Query(None, description="Fields to return. Examples: 'id,title,publication_year'"),
            filters: Any = Depends(self.filter_params_class) if self.filter_params_class else None
        ):
            """List and filter entities with pagination"""
            # Process filter parameters into extra_filters dict
            extra_filters = {}
            if filters:
                for attr, value in vars(filters).items():
                    # Custom handling for specific fields
                    if value is not None:
                        if attr == 'name':
                            # Handle name as display_name with regex
                            extra_filters["display_name"] = {"$regex": value, "$options": "i"}
                        elif attr == 'title':
                            # Handle title with regex
                            extra_filters["title"] = {"$regex": value, "$options": "i"}
                        elif attr == 'country':
                            # Handle country code
                            extra_filters["country_code"] = value.upper()
                        elif attr == 'level':
                            # Handle numeric level
                            extra_filters["level"] = value
                        elif attr == 'type' and self.entity_name_plural == 'sources':
                            # Handle source type
                            extra_filters["type"] = value.lower()
                        elif attr == 'type':
                            # Handle other types
                            extra_filters["type"] = value
                        elif attr == 'year':
                            # Handle publication year
                            extra_filters["publication_year"] = value
                        else:
                            # Default case
                            extra_filters[attr] = value

            return await self.handlers[self.entity_type].list_entities(
                page=pagination.page,
                per_page=pagination.per_page,
                sort_field=self.sort_field,
                filter_param=filter,
                sort_param=sort,
                select_param=select,
                extra_filters=extra_filters
            )

        # 2. Get entity by ID endpoint
        @self.router.get(
            f"/{self.entity_name_plural}/{{entity_id}}",
            summary=f"Get {self.entity_name_singular} details",
            description=entity_get_description(self.entity_name_singular)
        )
        async def get_entity(
            entity_id: str = Path(..., description=f"The ID of the {self.entity_name_singular} to retrieve"),
            select: Optional[str] = Query(None, description="Fields to return. Examples: 'id,title,publication_year'")
        ):
            """Get a specific entity by ID with related entities"""
            entity = await self.handlers[self.entity_type].get_entity(entity_id, select)
            
            # Add related entities if specified
            if 'works' in self.related_entities:
                field_name = f"{self.entity_type[:-1] if self.entity_type.endswith('s') else self.entity_type}_id"
                
                # Different entities may require different query fields
                filter_field = field_name
                if self.entity_type == 'authors':
                    filter_field = "author_ids"
                elif self.entity_type == 'concepts':
                    filter_field = "concept_ids"
                elif self.entity_type == 'institutions':
                    filter_field = "institution_ids"
                
                # Get related works
                entity["works"] = await self.db.works.find(
                    {filter_field: entity_id},
                    {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
                ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
                
            # Add other related entities as needed
            if 'authors' in self.related_entities and entity.get("_author_ids"):
                entity["authors"] = await self.db.authors.find(
                    {"id": {"$in": entity["_author_ids"]}},
                    {"_id": 0, "id": 1, "display_name": 1}
                ).to_list(length=None)
            
            if 'concepts' in self.related_entities and entity.get("_concept_ids"):
                entity["concepts"] = await self.db.concepts.find(
                    {"id": {"$in": entity["_concept_ids"]}},
                    {"_id": 0, "id": 1, "display_name": 1, "level": 1}
                ).to_list(length=None)
            
            return self.jsonable_encoder(entity)

        # 3. Search endpoint (only add if "search" is in related_entities)
        if "search" in self.related_entities:
            @self.router.get(
                f"/{self.entity_name_plural}/search",
                summary=f"Search {self.entity_name_plural}",
                description=entity_search_description(self.entity_name_plural),
                response_model=SearchResponse
            )
            async def search_entities(
                search_params: SearchParams = Depends(),
                filter: Optional[str] = Query(None, description="OpenAlex-style filter parameter"),
                sort: Optional[str] = Query(None, description="Sort parameter (defaults to relevance score)"),
                select: Optional[str] = Query(None, description="Fields to return")
            ):
                """Search entities using MongoDB text search"""
                # Custom search handler for works
                if self.entity_type == 'works':
                    return await self.handlers[self.entity_type].search_works(
                        search_params.q, 
                        search_params.skip, 
                        search_params.limit, 
                        search_params.explain_score,
                        None,  # Don't parse the filter here, pass it as string
                        filter,
                        sort,
                        select
                    )
                else:
                    # Generic search for other entity types
                    # Process filter for non-work entities
                    filter_query = parse_filter_param(filter) if filter else None
                    return await self.handlers[self.entity_type].search_entities(
                        search_params.q,
                        search_params.skip,
                        search_params.limit,
                        search_params.explain_score,
                        filter_query,
                        None,  # Use default projection
                        sort,
                        select
                    )

        # 4. Group by endpoint (for analytics)
        @self.router.get(
            f"/{self.entity_name_plural}/group_by/{{field}}",
            summary=f"Group {self.entity_name_plural} by a field",
            description=f"Group {self.entity_name_plural} by a field and return counts. Useful for analytics."
        )
        async def group_entities(
            field: str = Path(..., description="The field to group by"),
            filter: Optional[str] = Query(None, description="OpenAlex-style filter parameter to filter the entities before grouping")
        ):
            """Group entities by a field and return counts"""
            # Process any traditional filters
            extra_filters = {}
            
            return await self.handlers[self.entity_type].group_entities(
                group_by=field,
                filter_param=filter,
                extra_filters=extra_filters
            )


def create_entity_routers(app, db, handlers, jsonable_encoder):
    """Create and register all entity routers"""
    from api_utils import (
        WorksFilterParams, AuthorsFilterParams, ConceptsFilterParams,
        InstitutionsFilterParams, PublishersFilterParams, SourcesFilterParams,
        TopicsFilterParams, FieldsFilterParams, SubfieldsFilterParams, DomainsFilterParams
    )
    
    # Create router for works
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="works",
        entity_name_singular="work",
        entity_name_plural="works",
        filter_params_class=WorksFilterParams,
        sort_field="cited_by_count",
        related_entities=["authors", "concepts", "search"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for authors
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="authors",
        entity_name_singular="author",
        entity_name_plural="authors",
        filter_params_class=AuthorsFilterParams,
        sort_field="cited_by_count",
        related_entities=["works"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for concepts
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="concepts",
        entity_name_singular="concept",
        entity_name_plural="concepts",
        filter_params_class=ConceptsFilterParams,
        sort_field="works_count",
        related_entities=["works"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for institutions
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="institutions",
        entity_name_singular="institution",
        entity_name_plural="institutions",
        filter_params_class=InstitutionsFilterParams,
        sort_field="works_count",
        related_entities=["works"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for publishers
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="publishers",
        entity_name_singular="publisher",
        entity_name_plural="publishers",
        filter_params_class=PublishersFilterParams,
        sort_field="works_count",
        related_entities=["works", "search"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for sources
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="sources",
        entity_name_singular="source",
        entity_name_plural="sources",
        filter_params_class=SourcesFilterParams,
        sort_field="works_count",
        related_entities=["works"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for topics
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="topics",
        entity_name_singular="topic",
        entity_name_plural="topics",
        filter_params_class=TopicsFilterParams,
        sort_field="works_count",
        related_entities=["works"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for fields
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="fields",
        entity_name_singular="field",
        entity_name_plural="fields",
        filter_params_class=FieldsFilterParams,
        sort_field="works_count",
        related_entities=["works", "subfields"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for subfields
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="subfields",
        entity_name_singular="subfield",
        entity_name_plural="subfields",
        filter_params_class=SubfieldsFilterParams,
        sort_field="works_count",
        related_entities=["works", "fields"],
        jsonable_encoder=jsonable_encoder
    )
    
    # Create router for domains
    EntityRouter(
        router=app,
        db=db,
        handlers=handlers,
        entity_type="domains",
        entity_name_singular="domain",
        entity_name_plural="domains",
        filter_params_class=DomainsFilterParams,
        sort_field="works_count",
        related_entities=["works", "fields"],
        jsonable_encoder=jsonable_encoder
    )
