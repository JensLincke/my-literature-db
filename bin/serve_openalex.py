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
    allow_origin_regex=None,
    expose_headers=["*"],
    max_age=600,
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
        "concepts": await db.concepts.estimated_document_count(),
        "institutions": await db.institutions.estimated_document_count(),
        "venues": await db.venues.estimated_document_count(),
        "publishers": await db.publishers.estimated_document_count(),
        "sources": await db.sources.estimated_document_count()
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
            {"path": "/institutions", "description": "List and search institutions"},
            {"path": "/institutions/{id}", "description": "Get details of a specific institution"},
            {"path": "/venues", "description": "List and search publication venues"},
            {"path": "/venues/{id}", "description": "Get details of a specific venue"},
            {"path": "/publishers", "description": "List and search publishers"},
            {"path": "/publishers/{id}", "description": "Get details of a specific publisher"},
            {"path": "/sources", "description": "List and search sources"},
            {"path": "/sources/{id}", "description": "Get details of a specific source"},
            {"path": "/institutions", "description": "List and search institutions"},
            {"path": "/institutions/{id}", "description": "Get details of a specific institution"},
            {"path": "/venues", "description": "List and search venues"},
            {"path": "/venues/{id}", "description": "Get details of a specific venue"},
            {"path": "/publishers", "description": "List and search publishers"},
            {"path": "/publishers/{id}", "description": "Get details of a specific publisher"},
            {"path": "/sources", "description": "List and search sources"},
            {"path": "/sources/{id}", "description": "Get details of a specific source"}
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
    # Perform text search using MongoDB text index
    try:
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
        
    except Exception as e:
        # If text search fails (e.g., no index), raise an error with more detail
        raise HTTPException(
            status_code=503,
            detail=f"Text search is not available - the search index is still being built. Please wait for the index creation to complete. Error details: {str(e)}")

    if total == 0:
        return {
            "total": 0,
            "skip": skip,
            "limit": limit,
            "results": [],
            "message": "No matching documents found. Try different search terms."
        }
    
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

@app.get("/institutions",
    summary="List and search institutions",
    description="Returns a paginated list of academic institutions sorted by work count.")
async def list_institutions(
    name: Optional[str] = Query(
        None,
        description="Filter institutions by name (case-insensitive partial match)",
        example="University of California"
    ),
    country: Optional[str] = Query(
        None,
        description="Filter institutions by country code",
        example="US"
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
    """List and search institutions"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    if country:
        query["country_code"] = country.upper()
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.institutions.find(query).sort("works_count", DESCENDING)
    
    # Get total count
    total_count = await db.institutions.count_documents(query)
    
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

@app.get("/institutions/{institution_id}")
async def get_institution(institution_id: str):
    """Get details of a specific institution"""
    # Try to find institution by _id first, then by full id
    institution = await db.institutions.find_one({"_id": institution_id})
    if not institution:
        # If not found by _id, try full id
        institution = await db.institutions.find_one({"id": institution_id})
        if not institution:
            raise HTTPException(status_code=404, detail="Institution not found")
    
    # Get institution's top works
    works = await db.works.find(
        {"institution_ids": institution_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    institution["works"] = works
    return jsonable_encoder(institution)

@app.get("/venues",
    summary="List and search venues",
    description="Returns a paginated list of publication venues (journals, conferences) sorted by work count.")
async def list_venues(
    name: Optional[str] = Query(
        None,
        description="Filter venues by name (case-insensitive partial match)",
        example="Nature"
    ),
    type: Optional[str] = Query(
        None,
        description="Filter venues by type (journal, conference, repository)",
        example="journal"
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
    """List and search venues"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    if type:
        query["type"] = type.lower()
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.venues.find(query).sort("works_count", DESCENDING)
    
    # Get total count
    total_count = await db.venues.count_documents(query)
    
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

@app.get("/venues/{venue_id}")
async def get_venue(venue_id: str):
    """Get details of a specific venue"""
    # Try to find venue by _id first, then by full id
    venue = await db.venues.find_one({"_id": venue_id})
    if not venue:
        # If not found by _id, try full id
        venue = await db.venues.find_one({"id": venue_id})
        if not venue:
            raise HTTPException(status_code=404, detail="Venue not found")
    
    # Get venue's top works
    works = await db.works.find(
        {"venue_id": venue_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    venue["works"] = works
    return jsonable_encoder(venue)

@app.get("/publishers",
    summary="List and search publishers",
    description="Returns a paginated list of publishers sorted by work count.")
async def list_publishers(
    name: Optional[str] = Query(
        None,
        description="Filter publishers by name (case-insensitive partial match)",
        example="Elsevier"
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
    """List and search publishers"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.publishers.find(query).sort("works_count", DESCENDING)
    
    # Get total count
    total_count = await db.publishers.count_documents(query)
    
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

@app.get("/publishers/{publisher_id}")
async def get_publisher(publisher_id: str):
    """Get details of a specific publisher"""
    # Try to find publisher by _id first, then by full id
    publisher = await db.publishers.find_one({"_id": publisher_id})
    if not publisher:
        # If not found by _id, try full id
        publisher = await db.publishers.find_one({"id": publisher_id})
        if not publisher:
            raise HTTPException(status_code=404, detail="Publisher not found")
    
    # Get publisher's top works
    works = await db.works.find(
        {"publisher_id": publisher_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    publisher["works"] = works
    return jsonable_encoder(publisher)

@app.get("/sources",
    summary="List and search sources",
    description="Returns a paginated list of sources sorted by work count.")
async def list_sources(
    name: Optional[str] = Query(
        None,
        description="Filter sources by name (case-insensitive partial match)",
        example="arXiv"
    ),
    type: Optional[str] = Query(
        None,
        description="Filter sources by type",
        example="repository"
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
    """List and search sources"""
    # Build query
    query = {}
    if name:
        query["display_name"] = {"$regex": name, "$options": "i"}
    if type:
        query["type"] = type.lower()
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.sources.find(query).sort("works_count", DESCENDING)
    
    # Get total count
    total_count = await db.sources.count_documents(query)
    
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

@app.get("/sources/{source_id}")
async def get_source(source_id: str):
    """Get details of a specific source"""
    # Try to find source by _id first, then by full id
    source = await db.sources.find_one({"_id": source_id})
    if not source:
        # If not found by _id, try full id
        source = await db.sources.find_one({"id": source_id})
        if not source:
            raise HTTPException(status_code=404, detail="Source not found")
    
    # Get source's top works
    works = await db.works.find(
        {"source_id": source_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    source["works"] = works
    return jsonable_encoder(source)

@app.get("/publishers/search")
async def search_publishers(
    q: str = Query(
        ..., 
        description="Search query. Enter terms to search in publisher names. Use quotes for exact phrases."
    ),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    explain_score: bool = Query(False, description="Include explanation of search score")
):
    """
    Search publishers using MongoDB text search. Features:
    - Natural language search through publisher names
    - Use quotes for exact phrases (e.g. "Oxford University Press")
    - Returns results sorted by relevance
    """
    try:
        search_query = {"$text": {"$search": q}}
        projection = {
            "score": {"$meta": "textScore"},
            "display_name": 1,
            "works_count": 1,
            "country_code": 1
        }
        
        cursor = db.publishers.find(
            search_query,
            projection
        ).sort([("score", {"$meta": "textScore"})])
        
        # Get total count
        total = await db.publishers.count_documents(search_query)
        
        if total == 0:
            return {
                "total": 0,
                "skip": skip,
                "limit": limit,
                "results": [],
                "message": "No matching publishers found. Try different search terms."
            }
    except Exception as e:
        # If text search fails due to missing index
        raise HTTPException(
            status_code=503,
            detail="Text search is not available. Please run the update_openalex_index.py script to create the required indexes."
        )

    if total == 0:
        return {
            "total": 0,
            "skip": skip,
            "limit": limit,
            "results": [],
            "message": "No matching publishers found. Try different search terms."
        }
    
    # Apply pagination
    documents = await cursor.skip(skip).limit(limit).to_list(None)
    
    if explain_score:
        # Enhance results with match explanation
        for doc in documents:
            score = doc.get("score", 0)
            doc["_score_explanation"] = {
                "score": score,
                "query": q
            }
    
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "results": documents
    }




