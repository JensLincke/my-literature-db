"""Base handlers for OpenAlex API endpoints"""

from typing import Optional, Any, Dict, List, Tuple
from fastapi import HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import DESCENDING
import logging
from time import perf_counter

from elastic_index import ESIndex

from filter_utils import parse_filter_param, parse_sort_param, parse_select_param, parse_group_by_param

class BaseEntityHandler:
    """Base handler for all entity types (works, authors, concepts, etc.)"""
    
    def __init__(self, collection: AsyncIOMotorCollection, entity_name: str):
        self.collection = collection
        self.entity_name = entity_name
        self.esindex = ESIndex()
        self.logger = logging.getLogger(f"handlers.{entity_name}")
        
    def verbose(self) -> bool:
        """Returns whether debug logging is enabled"""
        return self.logger.isEnabledFor(logging.DEBUG)

    async def list_entities(
        self,
        name: Optional[str] = None,
        page: int = 1,
        per_page: int = 25,
        sort_field: str = "works_count",
        filter_param: Optional[str] = None,
        sort_param: Optional[str] = None,
        select_param: Optional[str] = None,
        title: Optional[str] = None,
        year: Optional[int] = None,
        type: Optional[str] = None,
        extra_filters: Dict = None
    ) -> Dict[str, Any]:
        """Generic method for listing entities with pagination"""
        query = {}
        
        # Handle entity-specific name field
        if name:
            name_field = "title" if self.entity_name == "work" else "display_name"
            query[name_field] = {"$regex": name, "$options": "i"}
            
        # Handle work-specific filters
        if title:
            query["title"] = {"$regex": title, "$options": "i"}
        if year:
            query["publication_year"] = year
        if type:
            query["type"] = type
        
        # Add OpenAlex-style filter if provided
        if filter_param:
            filter_query = parse_filter_param(filter_param)
            query.update(filter_query)
            
        # Add traditional filters if provided
        if extra_filters:
            query.update(extra_filters)
        
        # Parse sorting parameters
        sort_specs = parse_sort_param(sort_param, self.entity_name)
        
        # Create sort list for MongoDB
        sort_list = []
        for field, direction in sort_specs:
            if field == "score" and direction == "textScore":
                # Skip textScore sorting here, only applicable in text search
                continue
            elif field == "relevance_score":
                # Skip relevance_score here too, only applicable in text search
                continue
            else:
                sort_list.append((field, direction))
                
        # If no valid sort fields, use default
        if not sort_list:
            sort_list = [(sort_field, DESCENDING)]
            
        # Handle field selection
        projection = parse_select_param(select_param)
            
        skip = (page - 1) * per_page
        
        # Apply query with sort and projection
        cursor = self.collection.find(query, projection)
        
        # Apply sorting
        if sort_list:
            # Convert to MongoDB sort format
            cursor = cursor.sort(sort_list)
        
        total_count = await self.collection.count_documents(query)
        results = await cursor.skip(skip).limit(per_page).to_list(per_page)
        
        return {
            "meta": {
                "count": len(results),
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page
            },
            "results": results
        }

    async def get_entity(self, entity_id: str, select_param: Optional[str] = None) -> Dict[str, Any]:
        """Generic method for getting a single entity by ID"""
        # Handle field selection
        projection = parse_select_param(select_param)
        
        # Check both _id and id fields for the entity
        entity = await self.collection.find_one({"_id": entity_id}, projection)
        if not entity:
            entity = await self.collection.find_one({"id": entity_id}, projection)
            if not entity:
                raise HTTPException(
                    status_code=404, 
                    detail=f"{self.entity_name} not found"
                )
        return entity


    async def search_elasticsearch(query):
        result = await esindex.search(
            index=self.entity_type,
            query=search_params.q,
            skip=search_params.skip,
            limit=search_params.limit,
            filter_query=filter_query
        )
        return result


    async def search_entities(
        self,
        q: str,
        skip: int = 0,
        limit: int = 10,
        explain_score: bool = False,
        filter_query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort_param: Optional[str] = None,
        select_param: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generic method for text search across entities"""
        if self.verbose():
            start_time = perf_counter()
            self.logger.debug(f"Starting search with query: '{q}'")
            self.logger.debug(f"Parameters: skip={skip}, limit={limit}, explain_score={explain_score}")
            if filter_query:
                self.logger.debug(f"Filter query: {filter_query}")
            
        try:
            # Ensure the query is not empty
            # Basic text search query
            search_query = {"$text": {"$search": q}}
            
            if self.verbose():
                self.logger.debug(f"Initial text search query: {search_query}")
            
            # Add any filter conditions
            if filter_query:
                # Combine text search with filter using $and
                search_query = {"$and": [search_query, filter_query]}
                if self.verbose():
                    self.logger.debug(f"Combined search query with filters: {search_query}")
            
            # Ensure projection exists
            if not projection:
                projection = {}
            
            # Override with select parameter if provided
            if select_param:
                projection = parse_select_param(select_param)
            
            # Add scoring if needed
            use_scoring = explain_score or (sort_param and "relevance_score" in sort_param)
            if use_scoring and "score" not in projection:
                projection["score"] = {"$meta": "textScore"}

            self.logger.debug(f"start finding")

            # Create cursor first
            cursor = self.collection.find(search_query, projection)
            
            # Instead of getting exact count, use limit+1 to check if there are more results
            total_cursor = self.collection.find(search_query).limit(limit + skip + 1)
            total_docs = await total_cursor.to_list(None)
            total = len(total_docs)
            has_more = total > (limit + skip)
            
            self.logger.debug(f"found something")

            # Add sorting if specified
            if sort_param:
                sort_specs = parse_sort_param(sort_param, self.entity_name)
                for field, direction in sort_specs:
                    if field != "relevance_score":
                        cursor = cursor.sort(field, direction)
            elif use_scoring:
                # Default to score-based sorting if scoring is enabled
                cursor = cursor.sort([("score", {"$meta": "textScore"})])
            
            if self.verbose():
                self.logger.debug(f"Fetching documents with skip={skip}, limit={limit}")
            
            # Get results using the documents we already fetched
            documents = total_docs[skip:skip + limit] if total_docs else []
            
            if not documents:
                return {
                    "total": 0,
                    "skip": skip,
                    "limit": limit,
                    "results": [],
                    "message": f"No matching {self.entity_name}s found. Try different search terms."
                }
                            
            if self.verbose():
                self.logger.debug(f"Retrieved {len(documents)} documents")
            
            if explain_score:
                if self.verbose():
                    self.logger.debug("Adding score explanations to documents")
                for doc in documents:
                    doc["_score_explanation"] = {
                        "score": doc.get("score", 0),
                        "query": q
                    }
            
            result = {
                "total": total,
                "skip": skip,
                "limit": limit,
                "has_more": has_more,
                "results": documents
            }

            if self.verbose():
                total_time = perf_counter() - start_time
                self.logger.debug(f"Search completed in {total_time:.3f}s")
                self.logger.debug(f"Retrieved {len(documents)} documents, has_more={has_more}")

            return result

        except Exception as e:
            if self.verbose():
                self.logger.error(f"Search failed: {str(e)}")
            raise HTTPException(
                status_code=503,
                detail=f"Text search is not available - the search index is still being built. Error: {str(e)}"
            )

    async def group_entities(
        self,
        group_by: str,
        filter_param: Optional[str] = None,
        extra_filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Group entities by a specified field and return counts"""
        query = {}
        
        # Add OpenAlex-style filter if provided
        if filter_param:
            filter_query = parse_filter_param(filter_param)
            query.update(filter_query)
            
        # Add traditional filters if provided
        if extra_filters:
            query.update(extra_filters)
            
        # Get the aggregation pipeline
        pipeline = parse_group_by_param(group_by)
        
        # Add match stage at the beginning if there are filters
        if query:
            pipeline.insert(0, {"$match": query})
            
        # Run the aggregation
        results = await self.collection.aggregate(pipeline).to_list(length=None)
        
        # Count total unique values
        total_groups = len(results)
        
        return {
            "meta": {
                "count": total_groups,
                "group_by": group_by
            },
            "group_by": [
                {
                    "key": result.get("key"),
                    "count": result.get("count")
                }
                for result in results
            ]
        }

class WorksHandler(BaseEntityHandler):
    """Handler for academic works"""
    
    def __init__(self, collection: AsyncIOMotorCollection, entity_name: str = "work"):
        super().__init__(collection, entity_name)
        self.default_sort_field = "cited_by_count"

    async def search_entities(
        self,
        q: str,
        skip: int = 0,
        limit: int = 10,
        explain_score: bool = False,
        filter_query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort_param: Optional[str] = None,
        select_param: Optional[str] = None
    ) -> Dict[str, Any]:
        """Override search_entities for works with custom projection and scoring"""
        # Set default work-specific projection if none provided
        if not projection:
            projection = {
                "title": 1,
                "publication_year": 1,
                "authorships": 1,
                "type": 1,
                "_citation_key": 1
            }
        

        if projection:
            projection["search_blob"] = 1

        # Add text score only if needed for sorting or score explanation
        if explain_score or (sort_param and "relevance_score" in sort_param):
            if projection:
                projection["score"] = {"$meta": "textScore"}
            
        result = await super().search_entities(
            q=q,
            skip=skip,
            limit=limit,
            explain_score=explain_score,
            filter_query=filter_query,
            projection=projection,
            sort_param=sort_param,
            select_param=select_param
        )
                
        return result
