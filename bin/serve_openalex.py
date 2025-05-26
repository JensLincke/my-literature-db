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

from handlers import BaseEntityHandler, WorksHandler

# MongoDB connection settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MAX_RESULTS_PER_PAGE = 100

# Initialize handlers
handlers = {}  # Will store our entity handlers

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
    global client, db, handlers
    client = AsyncIOMotorClient(MONGO_URI)
    db = client.openalex
    
    # Initialize handlers for each entity type
    handlers["works"] = WorksHandler(db.works, "Work")
    handlers["authors"] = BaseEntityHandler(db.authors, "Author")
    handlers["concepts"] = BaseEntityHandler(db.concepts, "Concept")
    handlers["institutions"] = BaseEntityHandler(db.institutions, "Institution")
    handlers["publishers"] = BaseEntityHandler(db.publishers, "Publisher")
    handlers["sources"] = BaseEntityHandler(db.sources, "Source")

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
            {"path": "/publishers", "description": "List and search publishers"},
            {"path": "/publishers/{id}", "description": "Get details of a specific publisher"},
            {"path": "/sources", "description": "List and search publication sources (journals, conferences, etc.)"},
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
    """List and search works"""
    return await handlers["works"].list_works(
        title=title,
        year=year,
        type=type,
        per_page=per_page
    )

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
    """Search works using MongoDB text search"""
    return await handlers["works"].search_works(q, skip, limit, explain_score)

@app.get("/works/{work_id}")
async def get_work(work_id: str):
    """Get details of a specific work"""
    work = await handlers["works"].get_entity(work_id)
    
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
    return await handlers["authors"].list_entities(
        name=name,
        page=page,
        per_page=per_page,
        sort_field="cited_by_count"
    )

@app.get("/authors/{author_id}")
async def get_author(author_id: str):
    """Get details of a specific author"""
    author = await handlers["authors"].get_entity(author_id)
    
    # Get author's top works
    works = await db.works.find(
        {"author_ids": author_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    
    author["works"] = works
    return jsonable_encoder(author)

# Concepts
@app.get("/concepts")
async def list_concepts(
    name: Optional[str] = Query(None),
    level: Optional[int] = Query(None, ge=0, le=5),
    page: int = Query(1, gt=0),
    per_page: int = Query(25, le=MAX_RESULTS_PER_PAGE),
):
    """List and search concepts"""
    extra_filters = {}
    if level is not None:
        extra_filters["level"] = level
        
    return await handlers["concepts"].list_entities(
        name=name,
        page=page,
        per_page=per_page,
        extra_filters=extra_filters
    )

@app.get("/concepts/{concept_id}")
async def get_concept(concept_id: str):
    """Get details of a specific concept"""
    concept = await handlers["concepts"].get_entity(concept_id)
    concept["works"] = await db.works.find(
        {"concept_ids": concept_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    return jsonable_encoder(concept)

# Institutions
@app.get("/institutions")
async def list_institutions(
    name: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    page: int = Query(1, gt=0),
    per_page: int = Query(25, le=MAX_RESULTS_PER_PAGE),
):
    """List and search institutions"""
    extra_filters = {}
    if country:
        extra_filters["country_code"] = country.upper()
        
    return await handlers["institutions"].list_entities(
        name=name,
        page=page,
        per_page=per_page,
        extra_filters=extra_filters
    )

@app.get("/institutions/{institution_id}")
async def get_institution(institution_id: str):
    """Get details of a specific institution"""
    institution = await handlers["institutions"].get_entity(institution_id)
    institution["works"] = await db.works.find(
        {"institution_ids": institution_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    return jsonable_encoder(institution)

# Publishers
@app.get("/publishers")
async def list_publishers(
    name: Optional[str] = Query(None),
    page: int = Query(1, gt=0),
    per_page: int = Query(25, le=MAX_RESULTS_PER_PAGE),
):
    """List and search publishers"""
    return await handlers["publishers"].list_entities(
        name=name,
        page=page,
        per_page=per_page
    )

@app.get("/publishers/{publisher_id}")
async def get_publisher(publisher_id: str):
    """Get details of a specific publisher"""
    publisher = await handlers["publishers"].get_entity(publisher_id)
    publisher["works"] = await db.works.find(
        {"publisher_id": publisher_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    return jsonable_encoder(publisher)

@app.get("/publishers/search")
async def search_publishers(
    q: str = Query(...),
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    explain_score: bool = Query(False)
):
    """Search publishers using MongoDB text search"""
    return await handlers["publishers"].search_entities(q, skip, limit, explain_score)

# Sources
@app.get("/sources")
async def list_sources(
    name: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    page: int = Query(1, gt=0),
    per_page: int = Query(25, le=MAX_RESULTS_PER_PAGE),
):
    """List and search sources"""
    extra_filters = {}
    if type:
        extra_filters["type"] = type.lower()
        
    return await handlers["sources"].list_entities(
        name=name,
        page=page,
        per_page=per_page,
        extra_filters=extra_filters
    )

@app.get("/sources/{source_id}")
async def get_source(source_id: str):
    """Get details of a specific source"""
    source = await handlers["sources"].get_entity(source_id)
    source["works"] = await db.works.find(
        {"source_id": source_id},
        {"id": 1, "title": 1, "publication_year": 1, "cited_by_count": 1, "type": 1}
    ).sort("cited_by_count", DESCENDING).limit(100).to_list(length=None)
    return jsonable_encoder(source)




