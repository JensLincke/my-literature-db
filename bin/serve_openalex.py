#!/usr/bin/env python3
"""
OpenAlex API Server (MongoDB Version)

This script creates a FastAPI server to query the local OpenAlex MongoDB database.
It provides endpoints to search and retrieve works, authors, and concepts.

Usage:
    uvicorn serve_openalex:app [--host HOST] [--port PORT] [--reload]

Requirements:
    pip install fastapi uvicorn motor
"""

import os
from typing import List, Optional
from datetime import datetime
import json
import base64

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING
from bson import ObjectId

# MongoDB connection settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MAX_RESULTS_PER_PAGE = 100

class MongoJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for MongoDB types"""
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

def jsonable_encoder(obj):
    """Convert MongoDB documents to JSON-serializable objects"""
    if isinstance(obj, dict):
        return {key: jsonable_encoder(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [jsonable_encoder(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    return obj

# Create FastAPI app
app = FastAPI(
    title="OpenAlex Local API",
    description="API for querying local OpenAlex data",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Override FastAPI's default JSON encoder
app.json_encoder = MongoJSONEncoder

# MongoDB client
client = None
db = None

@app.on_event("startup")
async def startup_db_client():
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db = client.openalex

@app.on_event("shutdown")
async def shutdown_db_client():
    if client:
        client.close()

@app.get("/")
async def get_root():
    """Get API information and database status"""
    # Get last import info
    metadata = await db.metadata.find_one({"key": "last_import"})
    
    # Get estimated counts (much faster than exact counts)
    entity_counts = {
        "works": await db.works.estimated_document_count(),
        "authors": await db.authors.estimated_document_count(),
        "concepts": await db.concepts.estimated_document_count()
    }
    
    api_info = {
        "name": "OpenAlex Local API",
        "version": "1.0.0",
        "last_import": metadata["value"] if metadata else None,
        "entity_counts": entity_counts,
        "endpoints": [
            {"path": "/works", "description": "List and search works"},
            {"path": "/works/{id}", "description": "Get details of a specific work"},
            {"path": "/authors", "description": "List and search authors"},
            {"path": "/authors/{id}", "description": "Get details of a specific author"},
            {"path": "/concepts", "description": "List and search concepts"},
            {"path": "/concepts/{id}", "description": "Get details of a specific concept"},
            {"path": "/search", "description": "Search across all entities"}
        ]
    }
    return api_info

@app.get("/works", 
    summary="List and search works",
    description="Returns a paginated list of academic works that can be filtered, sorted, and grouped.")
async def list_works(
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
    ),
    cursor: Optional[str] = Query(
        None,
        description="Cursor for pagination (pass the next_cursor value from the previous response)"
    ),
    sort_by: str = Query(
        "_id",
        description="Field to sort results by",
        enum=["_id", "publication_year", "cited_by_count", "title"]
    ),
    sort_order: str = Query(
        "asc",
        description="Sort order",
        enum=["asc", "desc"]
    ),
    include_count: bool = Query(
        False,
        description="Whether to include total count in response (may be slow for large datasets)"
    ),
    per_page: int = Query(
        25,
        description="Number of results per page",
        gt=0,
        le=MAX_RESULTS_PER_PAGE,
        example=25
    ),
    group_by: Optional[str] = Query(
        None,
        description="Group results by field",
        enum=["publication_year", "type", "language", "is_retracted", "has_fulltext"]
    )
):
    """List and search works with optional grouping"""
    # Build query
    query = {}
    if title:
        query["title"] = {"$regex": title, "$options": "i"}
    if year:
        query["publication_year"] = year
    if type:
        query["type"] = type

    # Validate group_by parameter if provided
    allowed_group_fields = ["publication_year", "type", "language", "is_retracted", "has_fulltext"]
    if group_by and group_by not in allowed_group_fields:
        raise HTTPException(status_code=400, detail=f"Invalid group_by field. Allowed fields: {allowed_group_fields}")
    
    # Validate and process sort parameters
    allowed_sort_fields = ["_id", "publication_year", "cited_by_count", "title"]
    if sort_by not in allowed_sort_fields:
        raise HTTPException(status_code=400, detail=f"Invalid sort field. Allowed fields: {allowed_sort_fields}")
    
    sort_direction = DESCENDING if sort_order.lower() == "desc" else 1

    # Add cursor condition if provided
    if cursor:
        try:
            decoded = base64.b64decode(cursor.encode()).decode()
            field, value = decoded.split(":", 1)
            if field == "_id":
                pass  # Keep as string
            elif field == "publication_year":
                value = int(value)
            elif field == "cited_by_count":
                value = int(value)
            
            op = "$lt" if sort_direction == DESCENDING else "$gt"
            query[field] = {op: value}
        except:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    # Build pipeline
    pipeline = []
    pipeline.append({"$match": query})

    if group_by:
        # If grouping, use a simpler pipeline for distinct values first
        distinct_pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${group_by}"}},
            {"$sort": {"_id": DESCENDING}},
            {"$limit": per_page}
        ]
        
        # Get distinct values first
        # Get counts with a single pipeline
        count_pipeline = [
            {"$match": query},
            {
                "$group": {
                    "_id": f"${group_by}",
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": DESCENDING}},
            {"$limit": per_page}
        ]
        # Force index usage via hint option
        count_results = await db.works.aggregate(
            count_pipeline,
            hint={"publication_year": 1} if group_by == "publication_year" else None
        ).to_list(None)
        
        response = {
            "meta": {
                "count": len(count_results),
                "per_page": per_page,
                "group_by": group_by
            },
            "group_by": [{
                "key": str(group["_id"]) if group["_id"] is not None else None,
                "count": group["count"]
            } for group in count_results]
        }
        
        return jsonable_encoder(response)
    else:
        # If not grouping, use original logic
        if include_count:
            pipeline.append({
                "$facet": {
                    "totalCount": [{"$count": "count"}],
                    "results": [
                        {"$sort": {sort_by: sort_direction}},
                        {"$limit": per_page + 1}
                    ]
                }
            })
            result = await db.works.aggregate(pipeline).to_list(1)
            result = result[0]
            
            total_count = result["totalCount"][0]["count"] if result["totalCount"] else 0
            results = result["results"]
        else:
            pipeline.extend([
                {"$sort": {sort_by: sort_direction}},
                {"$limit": per_page + 1}
            ])
            results = await db.works.aggregate(pipeline).to_list(None)

        # Check if there are more results
        has_more = len(results) > per_page
        if has_more:
            results = results[:-1]  # Remove the extra item

        # Create next cursor if there are more results
        next_cursor = None
        if has_more and results:
            last_doc = results[-1]
            cursor_value = str(last_doc.get(sort_by))
            next_cursor = base64.b64encode(f"{sort_by}:{cursor_value}".encode()).decode()

        # Convert MongoDB documents to JSON-serializable objects
        results = jsonable_encoder(results)
        
        response = {
            "meta": {
                "count": len(results),
                "per_page": per_page,
                "has_more": has_more,
                "next_cursor": next_cursor,
                "sort_by": sort_by,
                "sort_order": sort_order
            },
            "results": results
        }

        if include_count:
            response["meta"]["total_count"] = total_count

        return response

@app.get("/works/{work_id}")
async def get_work(work_id: str):
    """Get details of a specific work"""
    # Try to find work by _id (OpenAlex short_id) first, then by full id
    work = await db.works.find_one({"_id": work_id})
    if not work:
        # If not found by _id, try full id
        work = await db.works.find_one({"id": work_id})
        if not work:
            raise HTTPException(status_code=404, detail="Work not found")
    
    # Get author details if available
    if work.get("_author_ids"):
        authors = await db.authors.find(
            {"id": {"$in": work["_author_ids"]}},
            {"_id": 0, "id": 1, "display_name": 1}
        ).to_list(length=None)
        work["authors"] = authors
    
    # Get concept details if available
    if work.get("_concept_ids"):
        concepts = await db.concepts.find(
            {"id": {"$in": work["_concept_ids"]}},
            {"_id": 0, "id": 1, "display_name": 1, "level": 1}
        ).to_list(length=None)
        work["concepts"] = concepts
    
    # Convert MongoDB document to JSON-serializable object
    return jsonable_encoder(work)

@app.get("/authors",
    summary="List and search authors",
    description="Returns a paginated list of academic authors sorted by citation count.")
async def list_authors(
    name: Optional[str] = Query(
        None,
        description="Filter authors by name (case-insensitive partial match)",
        example="John Smith"
    ),
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
    )
):
    """List and search authors"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.authors.find(query).sort("cited_by_count", DESCENDING)
    
    # Get total count
    total_count = await db.authors.count_documents(query)
    
    # Get paginated results
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

@app.get("/authors/{author_id}")
async def get_author(author_id: str):
    """Get details of a specific author"""
    # Try to find author by _id first, then by full id
    author = await db.authors.find_one({"_id": author_id})
    if not author:
        # If not found by _id, try full id
        author = await db.authors.find_one({"id": author_id})
        if not author:
            raise HTTPException(status_code=404, detail="Author not found")
    
    # Get author's top works
    works = await db.works.find(
        {"author_ids": author_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    author["works"] = works
    return jsonable_encoder(author)

@app.get("/concepts",
    summary="List and search concepts",
    description="Returns a paginated list of academic concepts/topics sorted by number of works.")
async def list_concepts(
    name: Optional[str] = Query(
        None,
        description="Filter concepts by name (case-insensitive partial match)",
        example="artificial intelligence"
    ),
    level: Optional[int] = Query(
        None,
        description="Filter concepts by level (0 is most general, higher numbers are more specific)",
        ge=0,
        le=5,
        example=1
    ),
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
    )
):
    """List and search concepts"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    if level is not None:
        query["level"] = level
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.concepts.find(query).sort("works_count", DESCENDING)
    
    # Get total count
    total_count = await db.concepts.count_documents(query)
    
    # Get paginated results
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

@app.get("/concepts/{concept_id}")
async def get_concept(concept_id: str):
    """Get details of a specific concept"""
    # Try to find concept by _id (OpenAlex short_id) first, then by full id
    concept = await db.concepts.find_one({"_id": concept_id})
    if not concept:
        # If not found by _id, try full id
        concept = await db.concepts.find_one({"id": concept_id})
        if not concept:
            raise HTTPException(status_code=404, detail="Concept not found")
    
    # Get concept's top works
    works = await db.works.find(
        {"concept_ids": concept_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    concept["works"] = works
    return jsonable_encoder(concept)

@app.get("/search",
    summary="Search across all entities",
    description="""
    Performs a global search across works, authors, and concepts.
    Results are sorted by citation count (for works and authors) or work count (for concepts).
    Returns a mixed list of entities, each with an entity_type field indicating the type ('work', 'author', or 'concept').
    """)
async def search(
    q: str = Query(
        ...,
        description="Search query string (searches titles, abstracts, names)",
        example="machine learning"
    ),
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
    )
):
    """Search across all entities"""
    skip = (page - 1) * per_page
    
    # Search in works
    works = await db.works.find({
        "$or": [
            {"title": {"$regex": q, "$options": "i"}},
            {"abstract": {"$regex": q, "$options": "i"}}
        ]
    }, {
        "id": 1,
        "title": 1,
        "publication_year": 1,
        "type": 1,
        "cited_by_count": 1
    }).sort("cited_by_count", DESCENDING).limit(per_page).to_list(length=None)
    
    for work in works:
        work["entity_type"] = "work"
    
    # Search in authors
    authors = await db.authors.find({
        "display_name": {"$regex": q, "$options": "i"}
    }, {
        "id": 1,
        "display_name": 1,
        "cited_by_count": 1
    }).sort("cited_by_count", DESCENDING).limit(per_page).to_list(length=None)
    
    for author in authors:
        author["entity_type"] = "author"
    
    # Search in concepts
    concepts = await db.concepts.find({
        "display_name": {"$regex": q, "$options": "i"}
    }, {
        "id": 1,
        "display_name": 1,
        "level": 1,
        "works_count": 1
    }).sort("works_count", DESCENDING).limit(per_page).to_list(length=None)
    
    for concept in concepts:
        concept["entity_type"] = "concept"
    
    # Combine and sort results
    all_results = works + authors + concepts
    all_results.sort(
        key=lambda x: x.get("cited_by_count", 0) if "cited_by_count" in x else x.get("works_count", 0),
        reverse=True
    )
    
    # Convert MongoDB documents to JSON-serializable objects
    all_results = jsonable_encoder(all_results)
    
    return {
        "meta": {
            "query": q,
            "count": len(all_results),
            "page": page,
            "per_page": per_page
        },
        "results": all_results[skip:skip + per_page]
    }

@app.get("/works/search")
async def search_works(
    q: str = Query(
        ..., 
        description="Search query. Enter terms in any order (e.g. 'John Smith 2023 machine learning') or use quotes for exact phrases"
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    explain_score: bool = Query(False, description="Include explanation of search score")
):
    """
    Search works using MongoDB text search. Features:
    - Natural language search through combined field of authors, year, and title
    - Order of terms doesn't matter
    - Use quotes for exact phrases (e.g. "John Smith" or "machine learning")
    - Returns results sorted by relevance
    """
    search_query = {"$text": {"$search": q}}
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
    
    cursor = db.works.find(
        search_query,
        projection
    ).sort([("score", {"$meta": "textScore"})])
    
    # Get total count
    total = await db.works.count_documents(search_query)
    
    # Apply pagination
    documents = await cursor.skip(skip).limit(limit).to_list(None)
    
    if explain_score:
        # Enhance results with match explanation
        for doc in documents:
            score = doc.get("score", 0)
            search_blob = doc.get("search_blob", "").lower()
            terms = [t.strip('"').lower() for t in q.split()]
            
            # Find which terms matched in the search blob
            matches = [term for term in terms if term in search_blob]
            
            doc["_score_explanation"] = {
                "score": score,
                "matching_terms": matches,
                "search_blob": search_blob  # Include for transparency
            }
            # Remove search_blob from final output
            doc.pop("search_blob", None)
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "results": documents
    }


