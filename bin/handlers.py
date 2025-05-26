"""Base handlers for OpenAlex API endpoints"""

from typing import Optional, Any, Dict, List
from fastapi import HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import DESCENDING

from filter_utils import parse_filter_param

class BaseEntityHandler:
    """Base handler for all entity types (works, authors, concepts, etc.)"""
    
    def __init__(self, collection: AsyncIOMotorCollection, entity_name: str):
        self.collection = collection
        self.entity_name = entity_name

    async def list_entities(
        self,
        name: Optional[str] = None,
        page: int = 1,
        per_page: int = 25,
        sort_field: str = "works_count",
        filter_param: Optional[str] = None,
        extra_filters: Dict = None
    ) -> Dict[str, Any]:
        """Generic method for listing entities with pagination"""
        query = {}
        if name:
            query["display_name"] = {"$regex": name, "$options": "i"}
        
        # Add OpenAlex-style filter if provided
        if filter_param:
            filter_query = parse_filter_param(filter_param)
            query.update(filter_query)
            
        # Add traditional filters if provided
        if extra_filters:
            query.update(extra_filters)
        
        skip = (page - 1) * per_page
        cursor = self.collection.find(query).sort(sort_field, DESCENDING)
        
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

    async def get_entity(self, entity_id: str) -> Dict[str, Any]:
        """Generic method for getting a single entity by ID"""
        entity = await self.collection.find_one({"_id": entity_id})
        if not entity:
            entity = await self.collection.find_one({"id": entity_id})
            if not entity:
                raise HTTPException(
                    status_code=404, 
                    detail=f"{self.entity_name} not found"
                )
        return entity

    async def search_entities(
        self,
        q: str,
        skip: int = 0,
        limit: int = 10,
        explain_score: bool = False,
        filter_query: Dict = None,
        projection: Dict = None
    ) -> Dict[str, Any]:
        """Generic method for text search across entities"""
        try:
            # Basic text search query
            search_query = {"$text": {"$search": q}}
            
            # Add any filter conditions
            if filter_query:
                # Combine text search with filter using $and
                search_query = {"$and": [search_query, filter_query]}
            
            if not projection:
                projection = {
                    "score": {"$meta": "textScore"},
                    "display_name": 1,
                    "works_count": 1,
                }

            cursor = self.collection.find(
                search_query,
                projection
            ).sort([("score", {"$meta": "textScore"})])
            
            total = await self.collection.count_documents(search_query)
            
            if total == 0:
                return {
                    "total": 0,
                    "skip": skip,
                    "limit": limit,
                    "results": [],
                    "message": f"No matching {self.entity_name}s found. Try different search terms."
                }

            documents = await cursor.skip(skip).limit(limit).to_list(None)
            
            if explain_score:
                for doc in documents:
                    doc["_score_explanation"] = {
                        "score": doc.get("score", 0),
                        "query": q
                    }
            
            return {
                "total": total,
                "skip": skip,
                "limit": limit,
                "results": documents
            }

        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Text search is not available - the search index is still being built. Error: {str(e)}"
            )

class WorksHandler(BaseEntityHandler):
    """Handler for academic works"""
    
    async def list_works(
        self,
        title: Optional[str] = None,
        year: Optional[int] = None,
        type: Optional[str] = None,
        per_page: int = 25,
        filter: Optional[str] = None
    ):
        extra_filters = {}
        if title:
            extra_filters["title"] = {"$regex": title, "$options": "i"}
        if year:
            extra_filters["publication_year"] = year
        if type:
            extra_filters["type"] = type
            
        return await self.list_entities(
            per_page=per_page,
            sort_field="cited_by_count",
            filter_param=filter,
            extra_filters=extra_filters
        )

    async def search_works(
        self, 
        q: str, 
        skip: int = 0, 
        limit: int = 10, 
        explain_score: bool = False,
        filter_query: Dict = None,
        filter_param: Optional[str] = None
    ):
        # If we received a filter parameter string, parse it
        if filter_param and not filter_query:
            filter_query = parse_filter_param(filter_param)
            
        projection = {
            "score": {"$meta": "textScore"},
            "title": 1,
            "publication_year": 1,
            "authorships": 1,
            "type": 1,
            "_citation_key": 1
        }
        
        if explain_score:
            projection["search_blob"] = 1
            
        result = await self.search_entities(
            q, 
            skip, 
            limit, 
            explain_score, 
            filter_query, 
            projection
        )
        
        if explain_score and result.get("results"):
            for doc in result["results"]:
                search_blob = doc.pop("search_blob", "").lower()
                terms = [t.strip('"').lower() for t in q.split()]
                matches = [term for term in terms if term in search_blob]
                doc["_score_explanation"].update({
                    "matching_terms": matches,
                    "search_blob": search_blob
                })
                
        return result
