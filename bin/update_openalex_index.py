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
        if not first_author or not first_author.strip():
            return None

        # Process author name
        if ',' in first_author:
            last_name = first_author.split(',')[0]
        else:
            last_name_parts = first_author.split()
            if not last_name_parts:
                return None
            last_name = last_name_parts[-1]

        # Clean and normalize last name
        last_name = fix_umlauts(last_name)
        clean_last_name = re.sub(r'[ \-\']', '', last_name)
        if not clean_last_name:
            return None
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

def update_works_index(db, batch_size: int = 1000, update_existing: bool = False):
    """Update citation keys for works in the database."""
    try:
        query = {} if update_existing else {'citation_key': {'$exists': False}}
        cursor = db.works.find(query)
        total_count = db.works.count_documents(query)
        processed = 0
        batch = []

        logger.info(f"Processing {total_count} documents...")
        for work in cursor:
            if not work.get('citation_key'):
                citation_key = generate_citation_key(work)
                if citation_key:
                    batch.append(UpdateOne(
                        {'_id': work['_id']},
                        {'$set': {'citation_key': citation_key}}
                    ))

            processed += 1
            if len(batch) >= batch_size:
                try:
                    result = db.works.bulk_write(batch)
                    logger.info(f"Processed {processed}/{total_count} documents. " +
                              f"Modified {result.modified_count} records.")
                except PyMongoError as e:
                    logger.error(f"Error in bulk write: {e}")
                batch = []

        # Process remaining documents
        if batch:
            try:
                result = db.works.bulk_write(batch)
                logger.info(f"Final batch: Modified {result.modified_count} records.")
            except PyMongoError as e:
                logger.error(f"Error in final bulk write: {e}")

        logger.info("Citation key update completed")

    except PyMongoError as e:
        logger.error(f"Error updating works index: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def create_all_indexes(db):
    """Create all required indexes for each collection."""
    try:
        # Works collection indexes
        logger.info("Creating indexes for works collection...")

        # Define required indexes
        required_indexes = [
            [('id', ASCENDING)],
            [('ids.openalex', ASCENDING)],
            [('ids.doi', ASCENDING)],
            [('ids.mag', ASCENDING)],
            [('citation_key', ASCENDING)]
        ]

        # Create all indexes in background without checking existing ones
        for index_spec in required_indexes:
            index_name = '_'.join(f"{field}_{order}" for field, order in index_spec)
            logger.info(f"Creating index {index_name} in background...")
            try:
                db.works.create_index(index_spec, background=True, maxTimeMS=1)
            except PyMongoError as e:
                # Ignore duplicate index errors or other non-critical errors
                logger.warning(f"Note: {e}")

        logger.info("Works indexes creation initiated in background")

    except PyMongoError as e:
        logger.error(f"Error creating indexes: {e}")
        raise

def parse_arguments():
    """Parse command-line arguments"""
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
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    start_time = datetime.now()

    try:
        # Connect to MongoDB
        client = MongoClient(args.mongo_uri)
        db = client.openalex

        if args.only_indexes:
            logger.info("Creating indexes for all collections...")
            create_all_indexes(db)
            duration = datetime.now() - start_time
            logger.info(f"Index creation initiated in {duration}. Indexes will continue building in the background.")
            client.close()
            sys.exit(0)

        # Update works index
        logger.info("Updating works index...")
        update_works_index(db, args.batch_size, args.skip_indexes)

        duration = datetime.now() - start_time
        logger.info(f"Process completed in {duration}")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    main()