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

from pymongo import MongoClient, ASCENDING, UpdateOne
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
    def parse_date(dir_path):
        try:
            date_str = dir_path.name.split("=")[1]
            # Convert to datetime for proper chronological sorting
            return datetime.strptime(date_str, "%Y-%m-%d")
        except (IndexError, ValueError):
            # If date parsing fails, return a future date to sort it last
            logger.warning(f"Invalid date format in directory: {dir_path}")
            return datetime.max

    return sorted(date_dirs, key=parse_date)

def extract_short_id(openalex_id):
    """Extract short ID from OpenAlex URL or ID string"""
    if not openalex_id:
        return None
    # Handle both URL and non-URL formats
    return openalex_id.split('/')[-1]

def process_entity(data, entity_type, update_date, part_file):
    """Process an entity before importing to MongoDB"""
    if not isinstance(data, dict):
        logger.warning(f"Skipping invalid data type: {type(data)} (expected dict)")
        return None
        
    # Check for required ID
    if 'id' not in data:
        logger.warning("Skipping record missing required 'id' field")
        return None
    
    # Use OpenAlex short_id as MongoDB _id
    short_id = extract_short_id(data.get('id'))
    if not short_id:
        logger.warning(f"Could not extract valid ID from: {data.get('id')}")
        return None
        
    data['_id'] = short_id
    
    # Add update information
    data['_update_date'] = update_date
    data['_update_part'] = str(part_file)
    
    # Process references to other entities
    if entity_type == "works":
        # Process author IDs safely
        authorships = data.get("authorships", [])
        if isinstance(authorships, list):
            data["_author_ids"] = [
                author_id for a in authorships 
                if isinstance(a, dict)
                for author_id in [extract_short_id(a.get("author", {}).get("id"))]
                if author_id
            ]
            
        # Process concept IDs safely
        concepts = data.get("concepts", [])
        if isinstance(concepts, list):
            data["_concept_ids"] = [
                concept_id for c in concepts
                if isinstance(c, dict)
                for concept_id in [extract_short_id(c.get("id"))]
                if concept_id
            ]
            
        # Process institution IDs safely
        institutions = []
        if isinstance(authorships, list):
            for authorship in authorships:
                if isinstance(authorship, dict):
                    affiliations = authorship.get("institutions", [])
                    if isinstance(affiliations, list):
                        for affiliation in affiliations:
                            if isinstance(affiliation, dict):
                                inst_id = extract_short_id(
                                    affiliation.get("institution", {}).get("id")
                                )
                                if inst_id:
                                    institutions.append(inst_id)
        data["_institution_ids"] = institutions
        
        # Process source ID safely
        primary_location = data.get("primary_location", {})
        if isinstance(primary_location, dict):
            source = primary_location.get("source", {})
            if isinstance(source, dict) and source.get("id"):
                data["_source_id"] = extract_short_id(source["id"])
        # Process fields safely
        fields = data.get("fields", [])
        if isinstance(fields, list):
            data["_field_ids"] = [
                field_id for f in fields
                if isinstance(f, dict)
                for field_id in [extract_short_id(f.get("id"))]
                if field_id
            ]
            
        # Process subfields safely
        subfields = data.get("subfields", [])
        if isinstance(subfields, list):
            data["_subfield_ids"] = [
                subfield_id for f in subfields
                if isinstance(f, dict)
                for subfield_id in [extract_short_id(f.get("id"))]
                if subfield_id
            ]
            
        # Process topics safely
        topics = data.get("topics", [])
        if isinstance(topics, list):
            data["_topic_ids"] = [
                topic_id for t in topics
                if isinstance(t, dict)
                for topic_id in [extract_short_id(t.get("id"))]
                if topic_id
            ]
            
        # Process funders safely
        funder_ids = []
        grants = data.get("grants", [])
        if isinstance(grants, list):
            for grant in grants:
                if isinstance(grant, dict):
                    funder = grant.get("funder", {})
                    if isinstance(funder, dict) and funder.get("id"):
                        funder_id = extract_short_id(funder["id"])
                        if funder_id:
                            funder_ids.append(funder_id)
        data["_funder_ids"] = funder_ids
            
        # Process domains safely
        domains = data.get("domains", [])
        if isinstance(domains, list):
            data["_domain_ids"] = [
                domain_id for d in domains
                if isinstance(d, dict)
                for domain_id in [extract_short_id(d.get("id"))]
                if domain_id
            ]
            
        # Process publisher safely
        primary_location = data.get("primary_location", {})
        if isinstance(primary_location, dict):
            source = primary_location.get("source", {})
            if isinstance(source, dict):
                publisher = source.get("publisher", {})
                if isinstance(publisher, dict) and publisher.get("id"):
                    data["_publisher_id"] = extract_short_id(publisher["id"])
            
    return data

def get_last_import_date(db, entity_type):
    """Get the most recent update date for an entity type from the database"""
    try:
        # Try to find the most recent document based on _update_date
        latest_doc = db[entity_type].find_one(
            {"_update_date": {"$exists": True}},
            sort=[("_update_date", -1)]
        )
        if latest_doc and "_update_date" in latest_doc:
            return latest_doc["_update_date"]
    except Exception as e:
        logger.warning(f"Error getting last import date for {entity_type}: {e}")
    return None

# Entity types to import
ENTITY_TYPES = [
    "works", "authors", "concepts",
    "institutions", "sources", "topics",
    "fields", "subfields",
    "domains", "funders", "publishers"
]

# Note: Index creation has been moved to update_openalex_index.py

def process_entity_files(db, entity_type, limit=None):
    """Process all files for a given entity type"""
    collection = db[entity_type]
    started_import = False
    
    # Get the last import date for this entity type
    last_import_date = get_last_import_date(db, entity_type)
    if last_import_date:
        logger.info(f"Found existing data for {entity_type}, last import date: {last_import_date}")
    
    snapshot_dirs = find_snapshot_dirs(entity_type)
    if not snapshot_dirs:
        return
    
    # Filter snapshot directories to only process newer ones
    if last_import_date:
        snapshot_dirs = [
            d for d in snapshot_dirs
            if d.name.split("=")[1] > last_import_date
        ]
        if not snapshot_dirs:
            logger.info(f"No new data found for {entity_type} after {last_import_date}")
            return
        logger.info(f"Found {len(snapshot_dirs)} new snapshot(s) for {entity_type}")
    
    total_imported = 0
    
    # Process each snapshot directory in chronological order
    for snapshot_dir in snapshot_dirs:
        update_date = snapshot_dir.name.split("=")[1]
        logger.info(f"Importing {entity_type} from {snapshot_dir} (date: {update_date})")
        
        # Get all part files
        # Sort part files by their part number to ensure chronological order
        part_files = sorted(snapshot_dir.glob("part_*.gz"), 
                          key=lambda x: int(x.stem.split('_')[1]))
        if not part_files:
            logger.error(f"No part files found in {snapshot_dir}")
            continue
        
        # Process all part files in this snapshot
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
                                    # Convert batch to upsert operations
                                    operations = [
                                        UpdateOne(
                                            {"_id": doc["_id"]},
                                            {"$set": doc},
                                            upsert=True
                                        ) for doc in batch
                                    ]
                                    result = collection.bulk_write(operations, ordered=False)
                                    total_imported += result.upserted_count + result.modified_count
                                    logger.info(f"Imported {total_imported} {entity_type} records (upserted: {result.upserted_count}, modified: {result.modified_count})")
                                except PyMongoError as e:
                                    logger.warning(f"Error processing batch: {str(e)}")
                                
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
                            # Convert batch to upsert operations
                            operations = [
                                UpdateOne(
                                    {"_id": doc["_id"]},
                                    {"$set": doc},
                                    upsert=True
                                ) for doc in batch
                            ]
                            result = collection.bulk_write(operations, ordered=False)
                            total_imported += result.upserted_count + result.modified_count
                            logger.info(f"Imported {total_imported} {entity_type} records (upserted: {result.upserted_count}, modified: {result.modified_count})")
                        except PyMongoError as e:
                            logger.warning(f"Error processing final batch: {str(e)}")
            
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
    parser.add_argument("--force-full", action="store_true",
                       help="Force full import even if data exists (default: only import new data)")
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
        
        # If force-full is specified, wipe the database first
        if args.force_full and not args.wipe:
            if confirm_wipe(db):
                logger.info("Force full import requested. Wiping existing database...")
                client.drop_database('openalex')
                db = client.openalex
                logger.info("Database wiped")
            else:
                logger.info("Database wipe cancelled")
                if input("Do you want to continue with incremental import instead? [y/N] ").lower() not in ['y', 'yes']:
                    logger.info("Import cancelled")
                    sys.exit(0)
                args.force_full = False

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
        logger.info("Run 'update_openalex_index.py --only-indexes' to create indexes")
        
    except PyMongoError as e:
        logger.error(f"MongoDB error: {str(e)}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    main()
