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
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
import base64
import logging
import logging.handlers

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import DESCENDING
from bson import ObjectId

from handlers import BaseEntityHandler, WorksHandler
from api_utils import MAX_RESULTS_PER_PAGE
from entity_router import create_entity_routers

# MongoDB connection settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Create file handler
file_handler = logging.handlers.RotatingFileHandler(
    'server.log',
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
file_handler.setLevel(logging.DEBUG)

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

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
    handlers["topics"] = BaseEntityHandler(db.topics, "Topic")
    handlers["fields"] = BaseEntityHandler(db.fields, "Field")
    handlers["subfields"] = BaseEntityHandler(db.subfields, "Subfield")
    handlers["domains"] = BaseEntityHandler(db.domains, "Domain")
    
    # Register all entity routers
    create_entity_routers(app, db, handlers, jsonable_encoder)

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
        "sources": await db.sources.estimated_document_count(),
        "topics": await db.topics.estimated_document_count(),
        "fields": await db.fields.estimated_document_count(),
        "subfields": await db.subfields.estimated_document_count(),
        "domains": await db.domains.estimated_document_count()
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
            {"path": "/sources/{id}", "description": "Get details of a specific source"},
            {"path": "/topics", "description": "List and search research topics"},
            {"path": "/topics/{id}", "description": "Get details of a specific topic"},
            {"path": "/fields", "description": "List and search research fields"},
            {"path": "/fields/{id}", "description": "Get details of a specific field"},
            {"path": "/subfields", "description": "List and search research subfields"},
            {"path": "/subfields/{id}", "description": "Get details of a specific subfield"},
            {"path": "/domains", "description": "List and search research domains"},
            {"path": "/domains/{id}", "description": "Get details of a specific domain"}
        ]
    }
    return api_info

