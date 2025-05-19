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
    
    api_info = {
        "name": "OpenAlex Local API",
        "version": "1.0.0",
        "last_import": metadata["value"] if metadata else None,
        "entity_counts": metadata.get("entity_counts", {}) if metadata else {},
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

@app.get("/works")
async def list_works(
    title: Optional[str] = None,
    year: Optional[int] = None,
    type: Optional[str] = None,
    page: int = Query(1, gt=0),
    per_page: int = Query(25, gt=0, le=MAX_RESULTS_PER_PAGE)
):
    """List and search works"""
    # Build query
    query = {}
    if title:
        query["title"] = {"$regex": title, "$options": "i"}
    if year:
        query["publication_year"] = year
    if type:
        query["type"] = type
    
    # Execute query with pagination
    skip = (page - 1) * per_page
    cursor = db.works.find(query).sort("cited_by_count", DESCENDING)
    
    # Get total count
    total_count = await db.works.count_documents(query)
    
    # Get paginated results
    results = await cursor.skip(skip).limit(per_page).to_list(per_page)
    
    # Convert MongoDB documents to JSON-serializable objects
    results = jsonable_encoder(results)
    
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

@app.get("/works/{work_id}")
async def get_work(work_id: str):
    """Get details of a specific work"""
    # Try to find work by short_id first, then by full id
    work = await db.works.find_one({"short_id": work_id})
    if not work:
        # If not found by short_id, try full id
        work = await db.works.find_one({"id": work_id})
        if not work:
            raise HTTPException(status_code=404, detail="Work not found")
    
    # Get author details if available
    if work.get("author_ids"):
        authors = await db.authors.find(
            {"id": {"$in": work["author_ids"]}},
            {"id": 1, "display_name": 1}
        ).to_list(length=None)
        work["authors"] = authors
    
    # Get concept details if available
    if work.get("concept_ids"):
        concepts = await db.concepts.find(
            {"id": {"$in": work["concept_ids"]}},
            {"id": 1, "display_name": 1, "level": 1}
        ).to_list(length=None)
        work["concepts"] = concepts
    
    # Convert MongoDB document to JSON-serializable object
    return jsonable_encoder(work)

@app.get("/authors")
async def list_authors(
    name: Optional[str] = None,
    page: int = Query(1, gt=0),
    per_page: int = Query(25, gt=0, le=MAX_RESULTS_PER_PAGE)
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
    # Try to find author by short_id first, then by full id
    author = await db.authors.find_one({"short_id": author_id})
    if not author:
        # If not found by short_id, try full id
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

@app.get("/concepts")
async def list_concepts(
    name: Optional[str] = None,
    level: Optional[int] = None,
    page: int = Query(1, gt=0),
    per_page: int = Query(25, gt=0, le=MAX_RESULTS_PER_PAGE)
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
    # Try to find concept by short_id first, then by full id
    concept = await db.concepts.find_one({"short_id": concept_id})
    if not concept:
        # If not found by short_id, try full id
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

@app.get("/search")
async def search(
    q: str,
    page: int = Query(1, gt=0),
    per_page: int = Query(25, gt=0, le=MAX_RESULTS_PER_PAGE)
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
