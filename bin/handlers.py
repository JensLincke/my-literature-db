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
        self.useElasticSearch = True  # Set to False to disable Elasticsearch usage
        
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
        
        print(f"Listing entities for {self.entity_name} with filters: {filter_param}, extra_filters: {extra_filters}")


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
        


        print(f"start query query {query} with projection {projection} and sort {sort_list}")


        # Apply query with sort and projection
        cursor = self.collection.find(query, projection, max_time_ms=10000)
        
        print(f"cursor created: {cursor}")

        # Apply sorting
        if sort_list:
            # Convert to MongoDB sort format
            cursor = cursor.sort(sort_list)
        
        # Use estimated count for performance - exact count is very expensive on large collections
        # For filtered queries, we'll use -1 to indicate "many results"
        if query:  # If there are filters applied
            total_count = -1  # Indicate "many results" without expensive counting
        else:
            # Only do exact counting for unfiltered queries (which should be fast)
            try:
                total_count = await self.collection.estimated_document_count(maxTimeMS=10000)
            except Exception as e:
                logging.warning(f"Count operation timed out: {e}")
                total_count = -1

        try:
            results = await cursor.skip(skip).limit(per_page).to_list(per_page)
        except Exception as e:
            logging.warning(f"Query operation timed out: {e}")
            # Return empty results with timeout indication
            return {
                "meta": {
                    "count": 0,
                    "total_count": -1,
                    "page": page,
                    "per_page": per_page,
                    "total_pages": -1,
                    "error": "Query timeout"
                },
                "results": []
            }

        print(f"Retrieved {len(results)} results for {self.entity_name} on page {page} with per_page {per_page}")


        return {
            "meta": {
                "count": len(results),
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page if total_count > 0 else -1
            },
            "results": results
        }

    async def get_entity(self, entity_id: str, select_param: Optional[str] = None) -> Dict[str, Any]:
        """Generic method for getting a single entity by ID"""
        # Handle field selection
        projection = parse_select_param(select_param)
        
        # URL decode the entity_id to handle encoded colons (%3A -> :)
        import urllib.parse
        entity_id = urllib.parse.unquote(entity_id)
        
        # Check if entity_id has a prefix (doi:, openalex:, mag:)
        if ":" in entity_id:
            prefix, actual_id = entity_id.split(":", 1)
            
            if prefix == "doi":
                # Search by DOI in the ids.doi field - try multiple URL formats
                # First try the standard https://doi.org/ format
                try:
                    entity = await self.collection.find_one({"ids.doi": f"https://doi.org/{actual_id}"}, projection, max_time_ms=10000)
                    if not entity:
                        # Try the older http://dx.doi.org/ format
                        entity = await self.collection.find_one({"ids.doi": f"http://dx.doi.org/{actual_id}"}, projection, max_time_ms=10000)
                    if not entity:
                        # Try regex search to match any DOI URL format ending with the actual_id
                        entity = await self.collection.find_one({"ids.doi": {"$regex": f"/{actual_id}$"}}, projection, max_time_ms=10000)
                except Exception as e:
                    logging.warning(f"DOI query timed out for {actual_id}: {e}")
                    entity = None
            elif prefix == "openalex":
                # Search by OpenAlex ID - try both full URL and short ID
                try:
                    entity = await self.collection.find_one({"ids.openalex": f"https://openalex.org/{actual_id}"}, projection, max_time_ms=10000)
                    if not entity:
                        # Also try the short ID directly
                        entity = await self.collection.find_one({"_id": actual_id}, projection, max_time_ms=10000)
                except Exception as e:
                    logging.warning(f"OpenAlex query timed out for {actual_id}: {e}")
                    entity = None
            elif prefix == "mag":
                # Search by MAG ID (stored as integer)
                try:
                    mag_id = int(actual_id)
                    entity = await self.collection.find_one({"ids.mag": mag_id}, projection, max_time_ms=10000)
                except (ValueError, Exception) as e:
                    # Handle both invalid MAG ID format and database connection/timeout issues
                    if isinstance(e, ValueError):
                        entity = None
                    else:
                        # For database connection issues, log and return None to trigger 404
                        logging.warning(f"Database query failed for MAG ID {mag_id}: {e}")
                        entity = None
            else:
                # Unknown prefix, treat as regular ID
                try:
                    entity = await self.collection.find_one({"_id": entity_id}, projection, max_time_ms=10000)
                except Exception as e:
                    logging.warning(f"Unknown prefix query timed out for {entity_id}: {e}")
                    entity = None
        else:
            # No prefix, check both _id and id fields for the entity
            try:
                entity = await self.collection.find_one({"_id": entity_id}, projection, max_time_ms=10000)
                if not entity:
                    entity = await self.collection.find_one({"id": entity_id}, projection, max_time_ms=10000)
            except Exception as e:
                logging.warning(f"ID query timed out for {entity_id}: {e}")
                entity = None
        
        if not entity:
            raise HTTPException(
                status_code=404, 
                detail=f"{self.entity_name} not found"
            )
        return entity


    async def search_elasticsearch(self, query, skip, limit):
        # Convert to lowercase plural form to match the router and ES index naming
        index_name = self.entity_name.lower() + "s" if not self.entity_name.lower().endswith('s') else self.entity_name.lower()
        result = await self.esindex.search(
            index=index_name,
            query=query,
            skip=skip,
            limit=limit
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
        logger = self.logger

        if self.verbose():
            start_time = perf_counter()
            self.logger.debug(f"Starting search with query: '{q}'")
            self.logger.debug(f"Parameters: skip={skip}, limit={limit}, explain_score={explain_score}")
            if filter_query:
                self.logger.debug(f"Filter query: {filter_query}")
            
        try:
            documents = []
            logger.debug(f"SEARCH " + self.entity_name)
            if self.useElasticSearch:
                logger.debug(f"Use Elasticsearch")
                found = await self.search_elasticsearch(
                    query=q,  # Pass the raw query string to let elastic_index handle the query construction
                    skip=skip,
                    limit=limit
                )
                total = found["total"]
                has_more = total > (skip + limit)
                
                # Get the IDs in ranked order from Elasticsearch
                ids = [doc["id"] for doc in found["results"]]
                
                # Handle field selection for Elasticsearch results
                if select_param:
                    projection = parse_select_param(select_param)
                
                # Get documents from MongoDB while preserving Elasticsearch order
                mongo_docs = {}
                try:
                    async for doc in self.collection.find({"id": {"$in": ids}}, projection, max_time_ms=10000):
                        mongo_docs[doc["id"]] = doc
                except Exception as e:
                    logging.warning(f"MongoDB query for search results timed out: {e}")
                    mongo_docs = {}
                
                # Preserve the order from Elasticsearch results
                documents = []
                for id in ids:
                    if id in mongo_docs:
                        doc = mongo_docs[id]
                        # Add the search score from Elasticsearch
                        es_doc = next((d for d in found["results"] if d["id"] == id), None)
                        if es_doc:
                            doc["_score"] = es_doc["score"]
                        documents.append(doc)
            else:
                logger.debug(f"Use Basic Search")
                # Ensure the query is not empty
                # Basic text search query
                search_query = {"$text": {"$search": q}}
                
                if self.verbose():
                    logger.debug(f"Initial text search query: {search_query}")
                
                # Add any filter conditions
                if filter_query:
                    # Combine text search with filter using $and
                    search_query = {"$and": [search_query, filter_query]}
                    if self.verbose():
                        logger.debug(f"Combined search query with filters: {search_query}")
                
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

                logger.debug(f"start finding")

                # Create cursor first with timeout
                try:
                    cursor = self.collection.find(search_query, projection, max_time_ms=10000)
                    
                    # Instead of getting exact count, use limit+1 to check if there are more results
                    total_cursor = self.collection.find(search_query, max_time_ms=10000).limit(limit + skip + 1)
                    total_docs = await total_cursor.to_list(None)
                    total = len(total_docs)
                    has_more = total > (limit + skip)
                except Exception as e:
                    logging.warning(f"Search query timed out: {e}")
                    return {
                        "total": 0,
                        "skip": skip,
                        "limit": limit,
                        "results": [],
                        "message": f"Search query timed out. Try simpler search terms."
                    }
                
                logger.debug(f"found something")

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
                    logger.debug(f"Fetching documents with skip={skip}, limit={limit}")
                
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
                logger.debug(f"Retrieved {len(documents)} documents")
            
            if explain_score:
                if self.verbose():
                    logger.debug("Adding score explanations to documents")
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
                logger.debug(f"Search completed in {total_time:.3f}s")
                logger.debug(f"Retrieved {len(documents)} documents, has_more={has_more}")

            return result

        except Exception as e:
            if self.verbose():
                logger.error(f"Search failed: {str(e)}")
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
            
        # Run the aggregation with timeout
        try:
            results = await self.collection.aggregate(pipeline, maxTimeMS=10000).to_list(length=None)
        except Exception as e:
            logging.warning(f"Aggregation query timed out: {e}")
            return {
                "meta": {
                    "count": 0,
                    "group_by": group_by,
                    "error": "Query timeout"
                },
                "group_by": []
            }
        
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
