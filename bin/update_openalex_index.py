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
    python update_openalex_index.py --list-indexes [--collection COLLECTION]

Options:
    --only-indexes        Only create/update indexes without updating citation keys
    --mongo-uri URI      MongoDB connection URI (default: mongodb://localhost:27017)
    --limit LIMIT        Limit the number of works to process for citation keys
    --batch-size SIZE    Number of documents to process in each batch
    --skip-indexes       Skip index creation (use if indexes already exist)
    --list-indexes       List all existing indexes and exit
    --collection NAME    Specify a collection name to check indexes for (optional)
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

async def update_works_index(db, limit: Optional[int] = None, batch_size: int = 1000, force: bool = False) -> None:
    """Update works collection with citation keys and indexes"""
    try:
        # Check and create necessary indexes if they don't exist
        indexes = db.works.list_indexes().to_list(None)
        existing_indexes = {idx['name'] for idx in indexes}
        logger.info(f"Found existing indexes: {existing_indexes}")

        # Create text index on search_blob if it doesn't exist
        if 'search_blob_text' not in existing_indexes:
            logger.info("Creating text index on search_blob (this may take a while)...")
            logger.info("You can continue using the database while the index builds in the background")
            start_time = datetime.now()
            db.works.create_index(
                [("search_blob", "text")],
                default_language="english",  # Set default language
                language_override="no_language",  # Use a field name that doesn't exist to prevent language override
                background=True
            )
            duration = datetime.now() - start_time
            logger.info(f"Text index creation completed in {duration}")

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
                db.works.create_index([(field, direction)], background=True)

        # Process works in batches
        updates = []
        processed = 0
        skipped = 0

        # Build find query for works that need updating
        find_query = {
            "$or": [
                {"_citation_key": {"$exists": False}},
                {"_citation_key": None},
                {"search_blob": {"$exists": False}},
                {"search_blob": None}
            ]
        }

        # Add projection to limit retrieved fields
        projection = {
            "_id": 1,
            "authorships": 1,
            "publication_year": 1,
            "title": 1,
            "_citation_key": 1,
            "search_blob": 1
        }

        # Get estimated count for progress reporting
        try:
            total_estimate = db.works.count_documents(find_query)
            logger.info(f"Estimated documents needing updates: {total_estimate}")
        except Exception as e:
            logger.warning(f"Could not get document count estimate: {e}")
            total_estimate = None

        cursor = db.works.find(find_query, projection)
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
            update = {"$set": {}}
            if force or not work.get("_citation_key"):
                if citation_key:
                    update["$set"]["_citation_key"] = citation_key
            if force or not work.get("search_blob"):
                update["$set"]["search_blob"] = search_blob

            if update["$set"]:
                updates.append(UpdateOne(
                    {"_id": work["_id"]},
                    update
                ))
            else:
                skipped += 1

            processed += 1
            if processed % 10000 == 0:  # Log progress every x documents
                percentage = ((processed + skipped) / total_estimate) * 100 if total_estimate else 0
                logger.info(f"Processed {processed} works, skipped {skipped} works so far. Progress: {percentage:.2f}%")

            if len(updates) >= batch_size:
                result = db.works.bulk_write(updates)
                logger.info(f"Batch update completed. Processed {len(updates)} updates.")
                updates = []

                if total_estimate:
                    logger.info(f"Progress: {processed + skipped}/{total_estimate} ({((processed + skipped)/total_estimate)*100:.1f}%)")
                else:
                    logger.info(f"Processed {processed} works, skipped {skipped} works.")

                # Check if we've hit the limit
                if limit and processed >= limit:
                    break

        # Process remaining updates
        if updates:
            result = db.works.bulk_write(updates)
            processed += len(updates)

        logger.info(f"Completed processing {processed} works, skipped {skipped} works.")

    except PyMongoError as e:
        logger.error(f"MongoDB error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

def create_index(collection, index_fields, unique=False):
    """Create an index on the specified fields if it doesn't exist"""
    try:
        # Get the auto-generated index name that MongoDB would use
        index_name = "_".join(f"{field}_{direction}" for field, direction in index_fields)
        
        # Check if index already exists
        existing_indexes = collection.index_information()
        existing_key_patterns = {
            name: [tuple(key) for key in info['key']]
            for name, info in existing_indexes.items()
        }
        
        # Check if an index with the same key pattern exists
        index_key_pattern = [tuple(field) for field in index_fields]
        for existing_pattern in existing_key_patterns.values():
            if existing_pattern == index_key_pattern:
                logger.info(f"Index already exists for fields: {index_fields}")
                return

        # Create index if it doesn't exist
        start_time = datetime.now()
        collection.create_index(index_fields, unique=unique, background=True)
        logger.info(f"Index created on fields: {index_fields} "
                   f"in {datetime.now() - start_time} seconds")
    except PyMongoError as e:
        logger.warning(f"Error creating index on {index_fields}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error creating index on {index_fields}: {str(e)}")
        raise

def create_text_index(collection, field_name, **kwargs):
    """Create a text index with special handling for language settings"""
    try:
        # Check if text index already exists
        existing_indexes = collection.index_information()
        for name, info in existing_indexes.items():
            if any('text' in str(key) for key in info['key']):
                logger.info(f"Text index already exists on {collection.name}")
                return

        # Default settings for text index
        default_settings = {
            'default_language': 'english',
            'language_override': 'no_language',
            'background': True
        }
        # Update with any provided kwargs
        settings = {**default_settings, **kwargs}
        
        start_time = datetime.now()
        collection.create_index(
            [(field_name, "text")],
            **settings
        )
        logger.info(f"Text index created on {field_name} "
                   f"in {datetime.now() - start_time} seconds")
    except PyMongoError as e:
        logger.warning(f"Error creating text index on {field_name}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error creating text index: {str(e)}")
        raise

def create_indexes(db):
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
        
        # Common indexes for all collections (note: removed unique constraint)
        create_index(collection, [("id", ASCENDING)])
        create_index(collection, [("display_name", ASCENDING)])  # Regular index for sorting and exact matches
        create_index(collection, [("works_count", ASCENDING)])
        create_index(collection, [("cited_by_count", ASCENDING)]) 

        # Create text index for search functionality
        if entity_type == "works":
            create_text_index(collection, "search_blob")
        else:
            create_text_index(collection, "display_name")

        # Collection-specific indexes
        if entity_type == "works":
            create_index(collection, [("ids.openalex", ASCENDING)])
            create_index(collection, [("publication_year", ASCENDING)])
            create_index(collection, [("authorships.author.id", ASCENDING)])
            create_index(collection, [("_author_ids", ASCENDING)])
            create_index(collection, [("concepts.id", ASCENDING)])
            create_index(collection, [("ids.doi", ASCENDING)])
            create_index(collection, [("_citation_key", ASCENDING)])
            
        elif entity_type == "authors":
            create_index(collection, [("last_known_institution.id", ASCENDING)])
            create_index(collection, [("x_concepts.id", ASCENDING)])
            create_index(collection, [("ids.orcid", ASCENDING)])
            
        elif entity_type == "concepts":
            create_index(collection, [("ancestors.id", ASCENDING)])
        
    logger.info("All index creation jobs have been initiated")




def check_index_progress(db, collection_name=None):
    """Check the progress of ongoing index creation operations and show completed indexes.
    
    Args:
        db: MongoDB database connection
        collection_name: Optional name of collection to check. If None, checks all collections.
    """
    try:
        # Check ongoing index builds
        print("\nChecking ongoing and queued index builds...")
        admin_db = db.client.admin
        current_ops = admin_db.command("currentOp", {"$all": True})
        found_index_ops = False
        seen_indexes = set()
        queued_indexes = {}  # Track queued indexes by collection
        
        # Process operations
        index_builds = []
        for op in current_ops['inprog']:
            # Look for both active and queued index operations
            if op.get('command', {}).get('createIndexes'):  # Remove 'works' check to allow all collections
                collection = op['command']['createIndexes']  # Get actual collection name
                indexes = op.get('command', {}).get('indexes', [])
                for idx in indexes:
                    key_str = ', '.join(f"{k}: {v}" for k, v in idx.get('key', {}).items())
                    index_name = idx.get('name', 'unknown')
                    if not op.get('msg'):  # If no msg, it's likely queued
                        if collection not in queued_indexes:
                            queued_indexes[collection] = set()
                        queued_indexes[collection].add(f"[{index_name} ({key_str})]")
                        continue

            # Look specifically for active index operations across all collections
            if (op.get('msg') and 'Index Build' in op['msg'] and
                op.get('ns', '').startswith(db.name + '.')):
                ns = op.get('ns', 'unknown')
                progress = op.get('progress', {})
                total = progress.get('total', 0)
                current = progress.get('done', 0)
                
                # Create a unique key for this index build
                index_key = f"{ns}:{total}"
                if index_key not in seen_indexes:
                    seen_indexes.add(index_key)
                    found_index_ops = True
                    if total > 0:
                        percent = (current / total) * 100
                    # Format numbers with commas for readability
                    current_fmt = f"{current:,}"
                    total_fmt = f"{total:,}"
                    
                    # Get index information
                    index_spec = op.get('command', {}).get('indexes', [{}])[0]
                    index_name = index_spec.get('name', 'unknown')
                    index_key = ', '.join(f"{k}: {v}" for k, v in index_spec.get('key', {}).items())
                    index_info = f"[{index_name} ({index_key})]" if index_key else ""
                    
                    # Determine operation type from the index build message
                    msg = op.get('msg', '').lower()
                    if "scanning" in msg:
                        operation_type = "documents (scanning collection)"
                    elif "inserting keys" in msg:
                        operation_type = "index entries (inserting)"
                    elif "sorting" in msg:
                        operation_type = "index entries (sorting)"
                    elif "draining writes" in msg:
                        operation_type = "writes (processing updates that occurred during index build)"
                    else:
                        # Default case - show the actual message for debugging
                        operation_type = f"operations ({msg})"
                    
                    index_builds.append((percent, 
                            f"Index build on {ns} {index_info}: {percent:.1f}% complete - Processed {current_fmt}/{total_fmt} {operation_type}"))
                else:
                    index_builds.append((0, 
                            f"Index build on {ns} {index_info} in progress (no progress data available)"))
        
        # Display queued indexes if any
        if queued_indexes:
            print("\n=== Queued Indexes ===")
            print("----------------------")
            for coll, indexes in sorted(queued_indexes.items()):
                print(f"\n  Collection: {coll}")
                print("  " + "-" * (len(coll) + 12))  # Underline collection name
                for idx in sorted(indexes):
                    print(f"    {idx}")
        
        if index_builds:
            print("\n=== Active Index Builds ===")
            print("-------------------------")
            # Group by collection
            builds_by_collection = {}
            for build in index_builds:
                coll = build[1].split()[3]  # Extract collection name from message
                if coll not in builds_by_collection:
                    builds_by_collection[coll] = []
                builds_by_collection[coll].append(build)
            
            # Display grouped results
            for coll, builds in sorted(builds_by_collection.items()):
                print(f"\n  Collection: {coll}")
                print("  " + "-" * (len(coll) + 12))  # Underline collection name
                for _, message in sorted(builds, reverse=True):
                    # Split message into parts for better formatting
                    parts = message.split(" - ")
                    if len(parts) == 2:
                        index_info, progress_info = parts
                        print(f"    {index_info}")
                        print(f"      → {progress_info}")
                    else:
                        print(f"    {message}")
        
        if not found_index_ops and not queued_indexes:
            print("\nNo active or queued index creation operations found")
            
    except PyMongoError as e:
        print(f"Error checking index progress: {e}", file=sys.stderr)
        raise

def list_indexes(db, collection_name=None):
    """List all existing indexes for the specified collections.
    
    Args:
        db: MongoDB database connection
        collection_name: Optional name of collection to check. If None, checks all collections.
    """
    try:
        # Get list of collections to check
        collections = ([collection_name] if collection_name 
                      else ["works", "authors", "concepts", "institutions", 
                           "sources", "topics", "fields", "subfields", 
                           "domains", "funders", "publishers"])
        
        # Check each collection's indexes
        first = True
        for coll_name in collections:
            if not first:
                print()  # Add blank line between collections
            first = False
            
            print(f"Collection '{coll_name}':")
            collection = db[coll_name]
            
            # Get existing indexes
            try:
                index_info = collection.index_information()
                if index_info:
                    for name, info in sorted(index_info.items()):
                        key_str = ', '.join(f"{k}: {v}" for k, v in info['key'])
                        is_unique = info.get('unique', False)
                        is_sparse = info.get('sparse', False)
                        properties = []
                        if is_unique:
                            properties.append("unique")
                        if is_sparse:
                            properties.append("sparse")
                        properties_str = f" ({', '.join(properties)})" if properties else ""
                        print(f"  - {name}{properties_str}")
                        print(f"    {key_str}")
                else:
                    print("  No indexes found")
            except PyMongoError as e:
                print(f"  Error: Could not retrieve indexes - {str(e)}")
                
    except PyMongoError as e:
        print(f"Error listing indexes: {str(e)}")
        raise

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Update OpenAlex works index and citation keys")
    parser.add_argument("--mongo-uri", type=str, default=MONGO_URI,
                       help="MongoDB connection URI")
    parser.add_argument("--limit", type=int, help="Limit the number of works to process")
    parser.add_argument("--skip-indexes", action="store_true",
                       help="Skip index creation")
    parser.add_argument("--batch-size", type=int, default=1000,
                       help="Number of documents to process in each batch (default: 1000, max recommended: 10000)")
    parser.add_argument("--skip-updating", action="store_true",
                       help="Only create indexes without updating citation keys")
    parser.add_argument("--index-progress", action="store_true",
                       help="Check the progress of ongoing index creation")
    parser.add_argument("--list-indexes", action="store_true",
                       help="List all existing indexes and exit")
    parser.add_argument("--collection", type=str,
                       help="Check indexes for specific collection")
    return parser.parse_args()


async def main():    
    args = parse_arguments()
    start_time = datetime.now()
    
    try:
        # Connect to MongoDB
        client = MongoClient(args.mongo_uri)
        db = client.openalex
        logger.info("Connected to MongoDB")
        
        start_time = datetime.now()
    
        if args.list_indexes:
            list_indexes(db, args.collection)
            client.close()
            sys.exit(0)

        if args.index_progress:
            check_index_progress(db, args.collection)
            client.close()
            sys.exit(0)

        # Handle index creation
        if not args.skip_indexes:
            logger.info("Creating indexes for all collections...")
            create_indexes(db)
            duration = datetime.now() - start_time
            logger.info(f"Index creation completed in {duration}")


        # Update works (including their indexes unless --skip-indexes)
        if not args.skip_updating:
            logger.info("update works with citation keys and indexes")
            logger.info(f"Using batch size: {args.batch_size}")
            update_works_index(db, args.limit, batch_size=args.batch_size)
        

        logger.info("update metadata for last index update")
        # Store update metadata
        db.metadata.insert_one({
            "key": "last_index_update",
            "value": datetime.now().isoformat(),
            "type": "works_citation_keys"
        })
        
        duration = datetime.now() - start_time
        logger.info(f"Update indexes completed in {duration}")       
    except Exception as e:
        logger.error(f"Error updating index: {str(e)}")
        sys.exit(1)
    finally:
        client.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())