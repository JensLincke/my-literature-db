#!/usr/bin/env python3
"""
OpenAlex Dataset Importer (MongoDB Version)

This script imports selected data from a local OpenAlex snapshot into MongoDB.
It expects the OpenAlex snapshot to be available at the path defined in OPENALEX_DATA_PATH.

Usage:
    python import_openalex_mongo.py [--limit LIMIT] [--mongo-uri MONGO_URI]

Options:
    --limit LIMIT           Limit the number of entries per entity to import (for testing)
    --mongo-uri MONGO_URI   MongoDB connection URI (default: mongodb://localhost:27017)
"""

import argparse
import gzip
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("openalex-importer")

# Path to OpenAlex snapshot data
OPENALEX_DATA_PATH = "/mnt/db/openalex/openalex-snapshot/data"

def find_latest_snapshot(entity_type):
    """Find the most recent snapshot folder for a given entity type"""
    base_path = Path(OPENALEX_DATA_PATH) / entity_type
    date_dirs = [d for d in base_path.glob("updated_date=*") if d.is_dir()]
    
    if not date_dirs:
        logger.error(f"No snapshot directories found for {entity_type}")
        return None
    
    # Sort by date (format: updated_date=YYYY-MM-DD)
    latest_dir = sorted(date_dirs, key=lambda d: d.name.split("=")[1], reverse=True)[0]
    return latest_dir

def extract_short_id(openalex_id):
    """Extract short ID from OpenAlex URL or ID string"""
    if not openalex_id:
        return None
    # Handle both URL and non-URL formats
    return openalex_id.split('/')[-1]

def process_entity(data, entity_type):
    """Process an entity before importing to MongoDB"""
    # Add short_id field
    data['short_id'] = extract_short_id(data.get('id'))
    
    # Process references to other entities
    if entity_type == "works":
        # Process author IDs
        data["author_ids"] = [extract_short_id(a.get("author", {}).get("id")) 
                             for a in data.get("authorships", [])]
        # Process concept IDs
        data["concept_ids"] = [extract_short_id(c.get("id")) 
                             for c in data.get("concepts", [])]
    
    return data

# Entity types to import
ENTITY_TYPES = ["works", "authors", "concepts"]

def process_entity_files(db, entity_type, limit=None):
    """Process all files for a given entity type"""
    collection = db[entity_type]
    
    # Create indexes for common queries
    collection.create_index([("short_id", ASCENDING)], unique=True)
    collection.create_index([("id", ASCENDING)], unique=True)
    
    if entity_type == "works":
        collection.create_index([("title", ASCENDING)])
        collection.create_index([("publication_year", ASCENDING)])
        collection.create_index([("cited_by_count", ASCENDING)])
        collection.create_index([("type", ASCENDING)])
        collection.create_index([("abstract", ASCENDING)])
        collection.create_index([("author_ids", ASCENDING)])
        collection.create_index([("concept_ids", ASCENDING)])
    elif entity_type == "authors":
        collection.create_index([("display_name", ASCENDING)])
        collection.create_index([("cited_by_count", ASCENDING)])
        collection.create_index([("works_count", ASCENDING)])
    elif entity_type == "concepts":
        collection.create_index([("display_name", ASCENDING)])
        collection.create_index([("level", ASCENDING)])
        collection.create_index([("works_count", ASCENDING)])
    
    latest_dir = find_latest_snapshot(entity_type)
    if not latest_dir:
        return
    
    logger.info(f"Importing {entity_type} from {latest_dir}")
    
    # Get all part files
    part_files = sorted(latest_dir.glob("part_*.gz"))
    if not part_files:
        logger.error(f"No part files found in {latest_dir}")
        return
    
    total_imported = 0
    for part_file in part_files:
        if limit and total_imported >= limit:
            break
            
        logger.info(f"Processing {part_file.name}")
        
        batch = []
        try:
            with gzip.open(part_file, 'rt', encoding='utf-8') as f:
                for line in f:
                    if limit and total_imported >= limit:
                        break
                    
                    try:
                        data = json.loads(line)
                        
                        # Skip entries with missing ID
                        if not data.get("id"):
                            continue
                        
                        data = process_entity(data, entity_type)
                        batch.append(data)
                        
                        # Process in batches for better performance
                        batch_size = min(1000, (limit - total_imported if limit else 1000))
                        if len(batch) >= batch_size:
                            try:
                                collection.insert_many(batch, ordered=False)
                            except PyMongoError as e:
                                logger.warning(f"Error inserting batch: {str(e)}")
                            
                            total_imported += len(batch)
                            logger.info(f"Imported {total_imported} {entity_type} records")
                            batch = []
                            
                            if limit and total_imported >= limit:
                                break
                    
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in {part_file.name}")
                        continue
                    except Exception as e:
                        logger.error(f"Error processing record: {str(e)}")
                        continue
                
                # Process any remaining records in the last batch
                if batch:
                    try:
                        collection.insert_many(batch, ordered=False)
                    except PyMongoError as e:
                        logger.warning(f"Error inserting final batch: {str(e)}")
                    total_imported += len(batch)
                    logger.info(f"Imported {total_imported} {entity_type} records")
        
        except Exception as e:
            logger.error(f"Error processing file {part_file}: {str(e)}")
            continue
    
    logger.info(f"Completed importing {total_imported} {entity_type} records")

def get_collection_counts(db):
    """Get the count of documents in each collection"""
    counts = {}
    for collection_name in ENTITY_TYPES:
        counts[collection_name] = db[collection_name].count_documents({})
    return counts

def confirm_wipe(mongo_uri):
    """Ask for confirmation before wiping the database"""
    try:
        client = MongoClient(mongo_uri)
        db = client.openalex
        counts = get_collection_counts(db)
        
        print("\nCurrent database contents:")
        print("-------------------------")
        total = 0
        for entity, count in counts.items():
            print(f"{entity.title()}: {count:,} documents")
            total += count
        print(f"\nTotal: {total:,} documents")
        print("-------------------------")
        
        response = input("\nAre you sure you want to wipe the database? This action cannot be undone. [y/N] ").lower()
        return response in ['y', 'yes']
    except Exception as e:
        logger.error(f"Error checking database contents: {e}")
        response = input("\nCouldn't get current database stats. Still want to wipe the database? [y/N] ").lower()
        return response in ['y', 'yes']

def main():
    parser = argparse.ArgumentParser(description="Import OpenAlex data into MongoDB")
    parser.add_argument("--limit", type=int, 
                       help="Limit the number of entries per entity type (for testing)")
    parser.add_argument("--mongo-uri", type=str, 
                       default="mongodb://localhost:27017",
                       help="MongoDB connection URI")
    parser.add_argument("--wipe", action="store_true",
                       help="Wipe the existing database before importing")
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info(f"Starting OpenAlex import at {start_time}")
    
    # Check if data directory exists
    if not os.path.isdir(OPENALEX_DATA_PATH):
        logger.error(f"OpenAlex data directory not found at {OPENALEX_DATA_PATH}")
        sys.exit(1)
    
    try:
        # Connect to MongoDB
        client = MongoClient(args.mongo_uri)
        db = client.openalex
        logger.info("Connected to MongoDB")
        
        # Wipe database if requested and confirmed
        if args.wipe:
            if confirm_wipe(args.mongo_uri):
                logger.info("Wiping existing database...")
                client.drop_database('openalex')
                db = client.openalex
                logger.info("Database wiped")
            else:
                logger.info("Database wipe cancelled")
                if input("Do you want to continue with the import? [y/N] ").lower() not in ['y', 'yes']:
                    logger.info("Import cancelled")
                    sys.exit(0)
        
        # Process each entity type
        for entity_type in ENTITY_TYPES:
            process_entity_files(db, entity_type, args.limit)
        
        # Store import metadata
        db.metadata.insert_one({
            "key": "last_import",
            "value": datetime.now().isoformat(),
            "entity_counts": {
                entity_type: db[entity_type].count_documents({})
                for entity_type in ENTITY_TYPES
            }
        })
        
    except PyMongoError as e:
        logger.error(f"MongoDB error: {str(e)}")
        sys.exit(1)
    finally:
        client.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Import completed in {duration}")

if __name__ == "__main__":
    main()
