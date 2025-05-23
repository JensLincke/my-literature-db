#!/usr/bin/env python3
"""
OpenAlex Index Updater

This script updates the MongoDB index for works and adds citation keys.
It generates citation keys based on the first author's last name, year, and significant title words.
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

async def main():
    parser = argparse.ArgumentParser(description="Update OpenAlex works index and citation keys")
    parser.add_argument("--mongo-uri", type=str, default=MONGO_URI,
                       help="MongoDB connection URI")
    parser.add_argument("--limit", type=int, help="Limit the number of works to process")
    parser.add_argument("--skip-indexes", action="store_true",
                       help="Skip index creation (use if indexes already exist)")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of documents to process in each batch (default: 1000, max recommended: 10000)")
    args = parser.parse_args()
    
    try:
        # Connect to MongoDB
        client = AsyncIOMotorClient(args.mongo_uri)
        db = client.openalex
        logger.info("Connected to MongoDB")
        
        # Update works
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
        
        logger.info("Index update completed successfully")
        
    except Exception as e:
        logger.error(f"Error updating index: {str(e)}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())