#!/usr/bin/env python3
"""
OpenAlex Dataset Importer (MongoDB Version)

This script imports selected data from a local OpenAlex snapshot into MongoDB.
It expects the OpenAlex snapshot to be available at the path defined in OPENALEX_DATA_PATH.

Usage:
    python import_openalex.py [--limit LIMIT] [--mongo-uri MONGO_URI]

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

def find_snapshot_dirs(entity_type):
    """Find all snapshot folders for a given entity type, sorted by date"""
    base_path = Path(OPENALEX_DATA_PATH) / entity_type
    date_dirs = [d for d in base_path.glob("updated_date=*") if d.is_dir()]
    
    if not date_dirs:
        logger.error(f"No snapshot directories found for {entity_type}")
        return []
    
    # Sort by date (format: updated_date=YYYY-MM-DD)
    return sorted(date_dirs, key=lambda d: d.name.split("=")[1])

def extract_short_id(openalex_id):
    """Extract short ID from OpenAlex URL or ID string"""
    if not openalex_id:
        return None
    # Handle both URL and non-URL formats
    return openalex_id.split('/')[-1]

def process_entity(data, entity_type, update_date, part_file):
    """Process an entity before importing to MongoDB"""
    # Use OpenAlex short_id as MongoDB _id
    data['_id'] = extract_short_id(data.get('id'))
    
    # Add update information
    data['_update_date'] = update_date
    data['_update_part'] = str(part_file)
    
    # Process references to other entities
    if entity_type == "works":
        # Process author IDs
        data["_author_ids"] = [extract_short_id(a.get("author", {}).get("id")) 
                             for a in data.get("authorships", [])]
        # Process concept IDs
        data["_concept_ids"] = [extract_short_id(c.get("id")) 
                             for c in data.get("concepts", [])]
    
    return data

# Entity types to import
ENTITY_TYPES = ["works", "authors", "concepts"]

def process_entity_files(db, entity_type, limit=None):
    """Process all files for a given entity type"""
    collection = db[entity_type]
    
    # Create indexes for common queries
    collection.create_index([("id", ASCENDING)], unique=True)
    
    if entity_type == "works":
        collection.create_index([("title", ASCENDING)])
        collection.create_index([("publication_year", ASCENDING)])
        collection.create_index([("cited_by_count", ASCENDING)])
        collection.create_index([("type", ASCENDING)])
        collection.create_index([("abstract", ASCENDING)])
        collection.create_index([("_author_ids", ASCENDING)])
        collection.create_index([("_concept_ids", ASCENDING)])
    elif entity_type == "authors":
        collection.create_index([("display_name", ASCENDING)])
        collection.create_index([("cited_by_count", ASCENDING)])
        collection.create_index([("works_count", ASCENDING)])
    elif entity_type == "concepts":
        collection.create_index([("display_name", ASCENDING)])
        collection.create_index([("level", ASCENDING)])
        collection.create_index([("works_count", ASCENDING)])
    
    snapshot_dirs = find_snapshot_dirs(entity_type)
    if not snapshot_dirs:
        return
    
    total_imported = 0
    
    # Process each snapshot directory in chronological order
    for snapshot_dir in snapshot_dirs:
        update_date = snapshot_dir.name.split("=")[1]
        logger.info(f"Importing {entity_type} from {snapshot_dir} (date: {update_date})")
        
        # Get all part files
        part_files = sorted(snapshot_dir.glob("part_*.gz"))
        if not part_files:
            logger.error(f"No part files found in {snapshot_dir}")
            continue
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
                        
                        data = process_entity(data, entity_type, update_date, part_file)
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

def get_collection_stats(db):
    """Get statistics about each collection"""
    stats = {}
    for collection_name in ENTITY_TYPES:
        collection = db[collection_name]
        try:
            # Get document count
            count = collection.count_documents({})
            # Get the latest update date
            latest_doc = collection.find_one(
                {"_update_date": {"$exists": True}},
                sort=[("_update_date", -1)]
            )
            latest_date = latest_doc.get("_update_date") if latest_doc else None
            
            stats[collection_name] = {
                "count": count,
                "latest_update": latest_date
            }
        except (KeyError, AttributeError) as e:
            # Handle specific errors related to missing fields or invalid data
            logger.warning(f"Data error getting stats for {collection_name}: {str(e)}")
            stats[collection_name] = {
                "count": count if 'count' in locals() else 0,
                "latest_update": None,
                "error": f"Data error: {str(e)}"
            }
        except Exception as e:
            # Handle other unexpected errors
            logger.error(f"Unexpected error getting stats for {collection_name}: {str(e)}")
            stats[collection_name] = {
                "count": count if 'count' in locals() else 0,
                "latest_update": None,
                "error": f"Unexpected error: {str(e)}"
            }
    return stats

def print_database_status(db):
    """Print current database status"""
    stats = get_collection_stats(db)
    
    print("\nDatabase contents:")
    print("-------------------------")
    total = 0
    for entity, entity_stats in stats.items():
        count = entity_stats["count"]
        latest_update = entity_stats["latest_update"] or "No data"
        print(f"{entity.title()}: {count:,} documents (Latest update: {latest_update})")
        total += count
    print(f"\nTotal: {total:,} documents")
    print("-------------------------")
    return stats

def confirm_wipe(db):
    """Ask for confirmation before wiping the database"""
    try:
        print_database_status(db)
        
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
    parser.add_argument("--status", action="store_true",
                       help="Show current database status")
    args = parser.parse_args()
    
    try:
        # Connect to MongoDB
        client = MongoClient(args.mongo_uri)
        db = client.openalex
        logger.info("Connected to MongoDB")
        
        # If --status flag is set, just show status and exit
        if args.status:
            print_database_status(db)
            client.close()
            return
        
        start_time = datetime.now()
        logger.info(f"Starting OpenAlex import at {start_time}")
        
        # Check if data directory exists
        if not os.path.isdir(OPENALEX_DATA_PATH):
            logger.error(f"OpenAlex data directory not found at {OPENALEX_DATA_PATH}")
            sys.exit(1)
            
        # Wipe database if requested and confirmed
        if args.wipe:
            if confirm_wipe(db):
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
        
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"Import completed in {duration}")
        
    except PyMongoError as e:
        logger.error(f"MongoDB error: {str(e)}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    main()
