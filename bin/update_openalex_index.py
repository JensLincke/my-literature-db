#!/usr/bin/env python3
"""
OpenAlex Index Manager

This script manages indexes for the OpenAlex MongoDB database and adds citation keys to works.
It can create/update indexes for all collections and generate citation keys for works.

The script can be run in two modes:
1. Index-only mode (--only-indexes): Only creates/updates indexes for all collections
2. Normal mode: Updates works with citation keys and creates necessary indexes

Citation keys are generated based on first author's last name, year, and significant title words.

Usage:
    python update_openalex_index.py [--only-indexes] [--mongo-uri MONGO_URI]
    python update_openalex_index.py [--limit LIMIT] [--batch-size SIZE]

Options:
    --only-indexes        Only create/update indexes without updating citation keys
    --mongo-uri URI      MongoDB connection URI (default: mongodb://localhost:27017)
    --limit LIMIT        Limit the number of works to process for citation keys
    --batch-size SIZE    Number of documents to process in each batch
    --skip-indexes       Skip index creation (use if indexes already exist)
"""
import os
import re
import sys
import logging
import argparse
from typing import List, Optional
from datetime import datetime

from pymongo import MongoClient, ASCENDING, UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("openalex-indexer")

# MongoDB connection settings
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# Stop words for citation key generation
STOP_WORDS = {
    'english': {'a', 'am', 'an', 'as', 'at', 'be', 'by', 'in', 'is', 'it', 'of', 'on', 'to', 
               'the', 'and', 'from', 'out', 'for', 'but'},
    'german': {'so', 'als', 'der', 'die', 'das', 'und', 'oder', 'aber', 'für'}
}

def clean_title(title: str) -> str:
    """Clean title by removing special characters and normalizing spaces"""
    if not title:
        return ""
    # Replace specific patterns
    title = re.sub(r'-based\s', 'based ', title)
    title = re.sub(r'-the-', 'the', title)
    # Remove special characters but keep spaces and hyphens
    title = re.sub(r'[^\w\s-]', '', title)
    return title.strip()

def get_significant_initials(title: str, max_words: int = 3) -> str:
    """Get significant initials from title words"""
    if not title:
        return ""
    
    # Clean and split the title
    words = clean_title(title).replace('-based ', 'based ').replace('-the-', 'the')
    words = re.split(r'[ -\/_]|(?=[0-9]+)', words)
    
    # Filter and process words
    significant_words = []
    for word in words:
        word = word.lower()
        if (len(word) > 0 and 
            word not in STOP_WORDS['english'] and 
            word not in STOP_WORDS['german'] and 
            not word[0].isdigit() and 
            not re.match(r'^[\(\)\[\]\/\\]', word)):
            significant_words.append(word)
    
    # Take first 3 significant words and get their initials
    return ''.join(word[0].upper() for word in significant_words[:max_words])

def fix_umlauts(text: str) -> str:
    """Convert German umlauts to their alternative spelling"""
    if not text:
        return ""
    
    umlaut_map = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'ß': 'ss',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue'
    }
    for umlaut, replacement in umlaut_map.items():
        text = text.replace(umlaut, replacement)
    return text

def generate_citation_key(work: dict) -> Optional[str]:
    """Generate citation key from work metadata"""
    try:
        # Get the first author's name
        authorships = work.get('authorships', [])
        if not authorships or not work.get('publication_year'):
            return None
        
        first_author = authorships[0].get('author', {}).get('display_name', '')
        if not first_author:
            return None
        
        # Process author name
        if ',' in first_author:
            last_name = first_author.split(',')[0]
        else:
            last_name = first_author.split()[-1]
        
        # Clean and normalize last name
        last_name = fix_umlauts(last_name)
        clean_last_name = re.sub(r'[ \-\']', '', last_name)
        normalized_last_name = clean_last_name[0].upper() + clean_last_name[1:].lower()
        
        # Get year and title initials
        year = str(work.get('publication_year'))
        title_initials = get_significant_initials(work.get('title', ''))
        
        if not title_initials:
            return None
            
        return f"{normalized_last_name}{year}{title_initials}"
        
    except Exception as e:
        logger.warning(f"Error generating citation key: {str(e)}")
        return None

async def update_works_index(db, limit: Optional[int] = None, batch_size: int = 1000) -> None:
    """Update works collection with citation keys and indexes"""
    try:
        # Check and create necessary indexes if they don't exist
        indexes = await db.works.list_indexes().to_list(None)
        existing_indexes = {idx['name'] for idx in indexes}
        logger.info(f"Found existing indexes: {existing_indexes}")
        
        # Create text index on search_blob if it doesn't exist
        if 'search_blob_text' not in existing_indexes:
            logger.info("Creating text index on search_blob (this may take a while)...")
            logger.info("You can continue using the database while the index builds in the background")
            start_time = datetime.now()
            await db.works.create_index(
                [("search_blob", "text")],
                default_language="english",  # Set default language
                language_override="no_language",  # Use a field name that doesn't exist to prevent language override
                background=True
            )
            duration = datetime.now() - start_time
            logger.info(f"Text index creation completed in {duration}")
            # Check if index was actually created
            indexes = await db.works.list_indexes().to_list(None)
            if any(idx.get('name') == 'search_blob_text' for idx in indexes):
                logger.info("Text index verified and ready to use")
            
        # MongoDB automatically names indexes as fieldname_direction (e.g. field_1 for ascending)
        required_indexes = [
            ("_citation_key", ASCENDING),
            ("title", ASCENDING),
            ("publication_year", ASCENDING),
            ("_author_ids", ASCENDING),
            ("_concept_ids", ASCENDING)
        ]
        
        for field, direction in required_indexes:
            index_name = f"{field}_1"
            if index_name not in existing_indexes:
                logger.info(f"Creating {field} index in background...")
                await db.works.create_index([(field, direction)], background=True)
                
        # Process works in batches
        updates = []
        processed = 0
        
        # Build find query for works that need updating
        find_query = {"$or": [
            {"_citation_key": {"$exists": False}},
            {"_citation_key": None},
            {"search_blob": {"$exists": False}}  # Also update works missing search_blob
        ]}
        
        # Get estimated count for progress reporting
        try:
            total_estimate = await db.works.estimated_document_count()
            logger.info(f"Estimated total documents: {total_estimate}")
        except Exception as e:
            logger.warning(f"Could not get document count estimate: {e}")
            total_estimate = None
        
        cursor = db.works.find(find_query)
        if limit:
            cursor = cursor.limit(limit)
            total_estimate = limit
            
        async for work in cursor:
            # Generate citation key
            citation_key = generate_citation_key(work)
            
            # Create search blob combining all relevant fields
            author_names = " ".join(
                auth.get("author", {}).get("display_name", "")
                for auth in work.get("authorships", [])
            )
            year = str(work.get("publication_year", ""))
            title = work.get("title", "")
            
            # Combine fields with extra spaces to prevent unwanted word combinations
            search_blob = f"{author_names} {year} {title}"
            
            # Create update operation
            update = {
                "$set": {
                    "search_blob": search_blob
                }
            }
            if citation_key:
                update["$set"]["_citation_key"] = citation_key
                
            updates.append(UpdateOne(
                {"_id": work["_id"]},
                update
            ))
            
            if len(updates) >= batch_size:
                result = await db.works.bulk_write(updates)
                processed += len(updates)
                if total_estimate:
                    logger.info(f"Processed {processed}/{total_estimate} works ({(processed/total_estimate)*100:.1f}%)")
                else:
                    logger.info(f"Processed {processed} works")
                
                # Check if we've hit the limit
                if limit and processed >= limit:
                    break
                    
                updates = []
        
        # Process remaining updates
        if updates:
            result = await db.works.bulk_write(updates)
            processed += len(updates)
            
        logger.info(f"Completed processing {processed} works")
        
    except PyMongoError as e:
        logger.error(f"MongoDB error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def create_all_indexes(db):
    """Create all necessary indexes for all collections"""
    ENTITY_TYPES = [
        "works", "authors", "concepts",
        "institutions", "sources", "topics",
        "fields", "subfields", "domains", 
        "funders", "publishers"
    ]
    
    logger.info("Starting to create indexes for all collections...")
    
    for entity_type in ENTITY_TYPES:
        collection = db[entity_type]
        logger.info(f"Creating indexes for {entity_type}...")
        
        # Common indexes for all collections
        collection.create_index([("id", ASCENDING)], unique=True, background=True)
        collection.create_index([("display_name", ASCENDING)], background=True)
        collection.create_index([("works_count", ASCENDING)], background=True)
        collection.create_index([("cited_by_count", ASCENDING)], background=True)
        collection.create_index([("updated_date", ASCENDING)], background=True)
        collection.create_index([("created_date", ASCENDING)], background=True)
        collection.create_index([("ids.openalex", ASCENDING)], background=True)
        
        # Collection-specific indexes
        if entity_type == "works":
            collection.create_index([("publication_year", ASCENDING)], background=True)
            collection.create_index([("type", ASCENDING)], background=True)
            collection.create_index([("open_access.is_oa", ASCENDING)], background=True)
            collection.create_index([("authorships.author.id", ASCENDING)], background=True)
            collection.create_index([("_author_ids", ASCENDING)], background=True)
            collection.create_index([("concepts.id", ASCENDING)], background=True)
            collection.create_index([("_concept_ids", ASCENDING)], background=True)
            collection.create_index([("ids.doi", ASCENDING)], background=True)
            collection.create_index([("_citation_key", ASCENDING)], background=True)
            # Create text index for search functionality
            collection.create_index([("search_blob", "text")], 
                                 background=True,
                                 default_language="english",
                                 language_override="no_language")
            
        elif entity_type == "authors":
            collection.create_index([("last_known_institution.id", ASCENDING)], background=True)
            collection.create_index([("x_concepts.id", ASCENDING)], background=True)
            collection.create_index([("ids.orcid", ASCENDING)], background=True)
            collection.create_index([("ids.scopus", ASCENDING)], background=True)
            
        elif entity_type == "concepts":
            collection.create_index([("level", ASCENDING)], background=True)
            collection.create_index([("ancestors.id", ASCENDING)], background=True)
            collection.create_index([("related_concepts.id", ASCENDING)], background=True)
            
        elif entity_type == "institutions":
            collection.create_index([("type", ASCENDING)], background=True)
            collection.create_index([("country_code", ASCENDING)], background=True)
            collection.create_index([("ids.ror", ASCENDING)], background=True)
            collection.create_index([("ids.grid", ASCENDING)], background=True)
            collection.create_index([("geo.country_code", ASCENDING)], background=True)
            
        elif entity_type == "domains":
            collection.create_index([("ids.wikidata", ASCENDING)], background=True)
            collection.create_index([("ids.wikipedia", ASCENDING)], background=True)
            collection.create_index([("fields.id", ASCENDING)], background=True)
            collection.create_index([("siblings.id", ASCENDING)], background=True)
            
        elif entity_type == "funders":
            collection.create_index([("country_code", ASCENDING)], background=True)
            collection.create_index([("grants_count", ASCENDING)], background=True)
            collection.create_index([("roles.role", ASCENDING)], background=True)
            collection.create_index([("roles.id", ASCENDING)], background=True)
            collection.create_index([("ids.ror", ASCENDING)], background=True)
            collection.create_index([("ids.crossref", ASCENDING)], background=True)
            collection.create_index([("x_concepts.id", ASCENDING)], background=True)
    
    logger.info("All index creation jobs have been initiated")

async def main():
    parser = argparse.ArgumentParser(description="Update OpenAlex works index and citation keys")
    parser.add_argument("--mongo-uri", type=str, default=MONGO_URI,
                       help="MongoDB connection URI")
    parser.add_argument("--limit", type=int, help="Limit the number of works to process")
    parser.add_argument("--skip-indexes", action="store_true",
                       help="Skip index creation (use if indexes already exist)")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of documents to process in each batch (default: 1000, max recommended: 10000)")
    parser.add_argument("--only-indexes", action="store_true",
                       help="Only create indexes without updating citation keys")
    args = parser.parse_args()
    
    try:
        # Connect to MongoDB
        client = AsyncIOMotorClient(args.mongo_uri)
        db = client.openalex
        logger.info("Connected to MongoDB")
        
        start_time = datetime.now()
        
        # Handle index creation
        if args.only_indexes:
            logger.info("Creating indexes for all collections...")
            create_all_indexes(db)
            duration = datetime.now() - start_time
            logger.info(f"Index creation completed in {duration}")
            return
            
        # Update works (including their indexes unless --skip-indexes)
        if args.skip_indexes:
            logger.info("Skipping index creation as requested")
        logger.info(f"Using batch size: {args.batch_size}")
        await update_works_index(db, args.limit, batch_size=args.batch_size)
        
        # Store update metadata
        await db.metadata.insert_one({
            "key": "last_index_update",
            "value": datetime.now().isoformat(),
            "type": "works_citation_keys"
        })
        
        duration = datetime.now() - start_time
        logger.info(f"Citation key update completed in {duration}")
        
    except Exception as e:
        logger.error(f"Error updating index: {str(e)}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())