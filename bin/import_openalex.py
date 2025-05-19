#!/usr/bin/env python3
"""
OpenAlex Dataset Importer

This script imports selected data from a local OpenAlex snapshot into a SQLite database.
It expects the OpenAlex snapshot to be available at the path defined in OPENALEX_DATA_PATH.

Usage:
    python import_openalex.py [--limit LIMIT]

Options:
    --limit LIMIT    Limit the number of entries per entity to import (for testing)
"""

import argparse
import gzip
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("openalex-importer")

# Path to OpenAlex snapshot data
OPENALEX_DATA_PATH = "/mnt/db/openalex/openalex-snapshot/data"
# SQLite database file 
DB_PATH = os.path.expanduser("~/openalex.db")

# Entity types to import (selectively importing to manage database size)
ENTITY_TYPES = {
    "works": {
        "table": "works",
        "columns": [
            "id TEXT PRIMARY KEY",
            "title TEXT",
            "publication_year INTEGER",
            "publication_date TEXT",
            "type TEXT",
            "doi TEXT",
            "abstract TEXT",
            "cited_by_count INTEGER",
            "is_open_access INTEGER",
            "author_ids TEXT",  # JSON array as text
            "concept_ids TEXT", # JSON array as text
            "created_date TEXT",
            "updated_date TEXT",
            "json_data TEXT"    # Original JSON data
        ],
        "extract": lambda data: {
            "id": data.get("id"),
            "title": data.get("title"),
            "publication_year": data.get("publication_year"),
            "publication_date": data.get("publication_date"),
            "type": data.get("type"),
            "doi": data.get("doi"),
            "abstract": data.get("abstract"),
            "cited_by_count": data.get("cited_by_count"),
            "is_open_access": 1 if data.get("is_open_access") else 0,
            "author_ids": json.dumps([a.get("author", {}).get("id") for a in data.get("authorships", [])]),
            "concept_ids": json.dumps([c.get("id") for c in data.get("concepts", [])]),
            "created_date": data.get("created_date"),
            "updated_date": data.get("updated_date"),
            "json_data": json.dumps(data)  # Store the complete original JSON
        }
    },
    "authors": {
        "table": "authors",
        "columns": [
            "id TEXT PRIMARY KEY",
            "display_name TEXT",
            "orcid TEXT",
            "works_count INTEGER",
            "cited_by_count INTEGER",
            "institution_id TEXT",
            "created_date TEXT",
            "updated_date TEXT",
            "json_data TEXT"    # Original JSON data
        ],
        "extract": lambda data: {
            "id": data.get("id"),
            "display_name": data.get("display_name"),
            "orcid": data.get("orcid"),
            "works_count": data.get("works_count"),
            "cited_by_count": data.get("cited_by_count"),
            "institution_id": data.get("last_known_institution", {}).get("id"),
            "created_date": data.get("created_date"),
            "updated_date": data.get("updated_date"),
            "json_data": json.dumps(data)  # Store the complete original JSON
        }
    },
    "concepts": {
        "table": "concepts",
        "columns": [
            "id TEXT PRIMARY KEY",
            "display_name TEXT",
            "level INTEGER",
            "works_count INTEGER",
            "cited_by_count INTEGER",
            "created_date TEXT",
            "updated_date TEXT",
            "json_data TEXT"    # Original JSON data
        ],
        "extract": lambda data: {
            "id": data.get("id"),
            "display_name": data.get("display_name"),
            "level": data.get("level"),
            "works_count": data.get("works_count"),
            "cited_by_count": data.get("cited_by_count"),
            "created_date": data.get("created_date"),
            "updated_date": data.get("updated_date"),
            "json_data": json.dumps(data)  # Store the complete original JSON
        }
    }
}

def create_tables(conn):
    """Create database tables if they don't exist"""
    cursor = conn.cursor()
    
    # Create tables for each entity type
    for entity_type, config in ENTITY_TYPES.items():
        table_name = config["table"]
        columns = ", ".join(config["columns"])
        
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        
        # Create index on updated_date for efficient querying
        cursor.execute(f"CREATE INDEX idx_{table_name}_updated_date ON {table_name}(updated_date)")
        
    conn.commit()
    cursor.close()

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

def process_entity_files(conn, entity_type, limit=None):
    """Process all files for a given entity type"""
    config = ENTITY_TYPES[entity_type]
    
    latest_dir = find_latest_snapshot(entity_type)
    if not latest_dir:
        return
    
    logger.info(f"Importing {entity_type} from {latest_dir}")
    
    # Get all part files
    part_files = sorted(latest_dir.glob("part_*.gz"))
    if not part_files:
        logger.error(f"No part files found in {latest_dir}")
        return
    
    cursor = conn.cursor()
    
    # Prepare the INSERT statement
    table_name = config["table"]
    extract_func = config["extract"]
    
    total_imported = 0
    for part_file in part_files:
        logger.info(f"Processing {part_file.name}")
        
        # Open and read the gzipped JSON file
        with gzip.open(part_file, 'rt', encoding='utf-8') as f:
            batch = []
            for line in f:
                if limit and total_imported >= limit:
                    break
                
                try:
                    data = json.loads(line)
                    extracted_data = extract_func(data)
                    
                    # Skip entries with missing required fields
                    if not extracted_data.get("id"):
                        continue
                    
                    # Extract fields and values for the INSERT query
                    fields = list(extracted_data.keys())
                    placeholders = ", ".join(["?" for _ in fields])
                    values = [extracted_data.get(field) for field in fields]
                    
                    # Add to batch for bulk insert
                    batch.append((
                        f"INSERT OR REPLACE INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})",
                        values
                    ))
                    
                    # Process in batches for better performance
                    if len(batch) >= 1000:
                        for query, params in batch:
                            cursor.execute(query, params)
                        conn.commit()
                        total_imported += len(batch)
                        logger.info(f"Imported {total_imported} {entity_type} records")
                        batch = []
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in {part_file.name}")
                    continue
                except Exception as e:
                    logger.error(f"Error processing record: {str(e)}")
                    continue
            
            # Process any remaining records in the last batch
            if batch:
                for query, params in batch:
                    cursor.execute(query, params)
                conn.commit()
                total_imported += len(batch)
                logger.info(f"Imported {total_imported} {entity_type} records")
    
    cursor.close()
    logger.info(f"Completed importing {total_imported} {entity_type} records")

def main():
    parser = argparse.ArgumentParser(description="Import OpenAlex data into SQLite")
    parser.add_argument("--limit", type=int, help="Limit the number of entries per entity type (for testing)")
    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info(f"Starting OpenAlex import at {start_time}")
    
    # Check if data directory exists
    if not os.path.isdir(OPENALEX_DATA_PATH):
        logger.error(f"OpenAlex data directory not found at {OPENALEX_DATA_PATH}")
        sys.exit(1)
    
    # Initialize database connection
    conn = sqlite3.connect(DB_PATH)
    
    try:
        # Create tables
        logger.info("Creating database tables")
        create_tables(conn)
        
        # Process each entity type
        for entity_type in ENTITY_TYPES:
            process_entity_files(conn, entity_type, args.limit)
            
        # Create a metadata table to track import info
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)")
        cursor.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)", 
                     ("last_import", datetime.now().isoformat()))
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error during import: {str(e)}")
        sys.exit(1)
    finally:
        conn.close()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Import completed in {duration}")
    logger.info(f"Database saved to {DB_PATH}")
    
if __name__ == "__main__":
    main()