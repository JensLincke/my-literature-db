"""
Script to index MongoDB data into Elasticsearch

Usage:
    python index_to_elasticsearch.py [--limit LIMIT] [--wipe COLLECTIONS]

Options:
    --limit LIMIT           Limit the number of entries per collection to index (for testing)
    --wipe COLLECTIONS      Wipe specific collections (comma-separated) or 'all' for all collections
"""

import asyncio
import argparse
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from elastic_index import ESIndex

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def index_collection(db, es_index, collection_name: str, batch_size: int = 1000, limit: int | None = None):
    """Index a MongoDB collection into Elasticsearch
    
    Args:
        db: MongoDB database instance
        es_index: Elasticsearch index instance
        collection_name: Name of the collection to index
        batch_size: Number of documents to index in each batch
        limit: Maximum number of documents to index (optional)
    """
    # Adjust batch size to not exceed the limit
    if limit and batch_size > limit:
        batch_size = limit
        
    collection = db[collection_name]
    total_docs = await collection.estimated_document_count()
    print(f"Indexing {'all' if limit is None else limit} documents from {collection_name} (estimated total: {total_docs})")
    
    # Start a session to handle the cursor timeout properly
    async with await db.client.start_session() as session:
        # Configure cursor with optimized settings for large collections
        cursor = collection.find(
            {}, 
            {"id": 1, "display_name": 1, "search_blob": 1},
            batch_size=batch_size,
            no_cursor_timeout=True,  # Prevent cursor from timing out
            allow_disk_use=True,     # Allow using disk for large result sets
            session=session          # Use the session to handle timeout properly
        )
        if limit:
            cursor = cursor.limit(limit)
            
        indexed = 0
        batch = []
        
        async for doc in cursor:
            # Create simplified document with only needed fields
            simple_doc = {
                "id": doc["id"],
                "display_name": doc["display_name"]
            }
            if collection_name == "works":
                simple_doc["display_name"] = doc.get("search_blob")

            # Add document to batch
            batch.append((doc["id"], simple_doc))
            
            if len(batch) >= batch_size:
                try:
                    success, failed = await es_index.bulk_index_documents(collection_name, batch)
                    indexed += len(batch)
                    print(f"Indexed {indexed}/{total_docs} documents in {collection_name}")
                except Exception as e:
                    print(f"Error bulk indexing in {collection_name}: {e}")
                    await asyncio.sleep(1)  # Wait a bit on error before continuing
                
                batch = []
                
                # Check if we've reached the limit
                if limit and indexed >= limit:
                    break
        
        # Index remaining documents
        if batch:
            try:
                success, failed = await es_index.bulk_index_documents(collection_name, batch)
                indexed += len(batch)
                print(f"Indexed {indexed}/{total_docs} documents in {collection_name}")
            except Exception as e:
                print(f"Error indexing final batch in {collection_name}: {e}")
                await asyncio.sleep(1)
            
            indexed += len(batch)
            print(f"Indexed {indexed}/{total_docs} documents in {collection_name}")

async def wipe_collections(es_index, collections_to_wipe):
    """Wipe specified collections from Elasticsearch
    
    Args:
        es_index: Elasticsearch index instance
        collections_to_wipe: List of collection names to wipe
    """
    all_collections = ["publishers", "concepts", "institutions", "sources", "works", "authors"]
    
    # If 'all' is specified, wipe all collections
    if "all" in collections_to_wipe:
        collections_to_wipe = all_collections
    
    for collection in collections_to_wipe:
        if collection not in all_collections:
            logger.warning(f"Unknown collection: {collection}")
            continue
        
        print(f"Wiping collection: {collection}")
        try:
            await es_index.delete_index(collection)
        except Exception as e:
            logger.error(f"Error wiping {collection}: {e}")

async def confirm_wipe(collections_to_wipe):
    """Ask for confirmation before wiping collections"""
    if "all" in collections_to_wipe:
        print("\nYou are about to wipe ALL collections from Elasticsearch!")
    else:
        print(f"\nYou are about to wipe the following collections from Elasticsearch:")
        for collection in collections_to_wipe:
            print(f"- {collection}")
            
    response = input("\nAre you sure you want to wipe these collections? This action cannot be undone. [y/N] ").lower()
    return response in ['y', 'yes']

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Index MongoDB data into Elasticsearch")
    parser.add_argument("--limit", type=int, 
                       help="Limit the number of entries per collection to index (for testing)")
    parser.add_argument("--wipe", type=str,
                       help="Wipe specific collections (comma-separated) or 'all' for all collections")
    args = parser.parse_args()
    
    # Initialize Elasticsearch handler
    es_index = ESIndex()
    mongo_client = None
    try:
        # Handle wipe request if specified
        if args.wipe:
            collections_to_wipe = [c.strip() for c in args.wipe.split(",")]
            if await confirm_wipe(collections_to_wipe):
                await wipe_collections(es_index, collections_to_wipe)
                if not collections_to_wipe or "all" in collections_to_wipe:
                    # If we wiped everything, we need to re-initialize
                    await es_index.initialize()
            else:
                print("Wipe cancelled")
                if not input("\nDo you want to continue with indexing? [y/N] ").lower() in ['y', 'yes']:
                    print("Indexing cancelled")
                    return
        else:
            # Initialize Elasticsearch indices
            await es_index.initialize()

        if args.wipe and not args.limit:
            # If only wiping was requested, exit here
            return

        # Initialize MongoDB client for indexing
        mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
        db = mongo_client.openalex
        
        cursors = []  # Keep track of cursors for cleanup
        
        # Collections to index
        collections = ["publishers", "concepts", "institutions", "sources", "works", "authors"]
        
        # Index each collection
        for collection in collections:
            print(f"Processing collection: {collection}")
            try:
                await index_collection(db, es_index, collection, limit=args.limit)
            except Exception as e:
                logger.error(f"Error indexing {collection}: {e}")
                continue
            finally:
                # Clean up any remaining cursors
                for cursor in cursors:
                    try:
                        cursor.close()
                    except:
                        pass
    finally:
        # Clean up
        if es_index:
            await es_index.close()
        if mongo_client:
            mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
