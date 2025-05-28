"""
Script to index MongoDB data into Elasticsearch

Usage:
    python index_to_elasticsearch.py [--limit LIMIT]

Options:
    --limit LIMIT  Limit the number of entries per collection to index (for testing)
"""

import asyncio
import argparse
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from elastic_index import ESIndex

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

async def index_collection(db, es_index, collection_name: str, batch_size: int = 1000, limit: int = None):
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
            {"id": 1, "display_name": 1},
            batch_size=batch_size,
            no_cursor_timeout=True,  # Prevent cursor from timing out
            allow_disk_use=True,     # Allow using disk for large result sets
            session=session          # Use the session to handle timeout properly
        )
        if limit:
            cursor = cursor.limit(limit)
            
        indexed = 0
        batch = []
        chunk_size = 2  # Process 2 documents at a time within a batch
        
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
                # Process batch in smaller chunks to avoid too many concurrent requests
                for i in range(0, len(batch), chunk_size):
                    chunk = batch[i:i + chunk_size]
                    try:
                        tasks = [
                            es_index.index_document(collection_name, doc_id, document)
                            for doc_id, document in chunk
                        ]
                        await asyncio.gather(*tasks)
                        
                        # Small delay between chunks to prevent overwhelming Elasticsearch
                        await asyncio.sleep(0.01)
                    except Exception as e:
                        print(f"Error indexing chunk in {collection_name}: {e}")
                        # Wait a bit longer on error before retrying
                        await asyncio.sleep(1)
                        continue
                
                indexed += len(batch)
                print(f"Indexed {indexed}/{total_docs} documents in {collection_name}")
                batch = []
                
                # Check if we've reached the limit
                if limit and indexed >= limit:
                    break
        
        # Index remaining documents
        if batch:
            for i in range(0, len(batch), chunk_size):
                chunk = batch[i:i + chunk_size]
                try:
                    tasks = [
                        es_index.index_document(collection_name, doc_id, document)
                        for doc_id, document in chunk
                    ]
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error indexing final chunk in {collection_name}: {e}")
                    await asyncio.sleep(1)
                    continue
            
            indexed += len(batch)
            print(f"Indexed {indexed}/{total_docs} documents in {collection_name}")

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Index MongoDB data into Elasticsearch")
    parser.add_argument("--limit", type=int, 
                       help="Limit the number of entries per collection to index (for testing)")
    args = parser.parse_args()
    
    # Initialize MongoDB client
    mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = mongo_client.openalex
    
    # Initialize Elasticsearch handler
    es_index = ESIndex()
    
    cursors = []  # Keep track of cursors for cleanup
    try:
        # Initialize Elasticsearch indices
        await es_index.initialize()
        
        # Collections to index
        collections = ["publishers", "concepts", "institutions", "sources", "works", "authors" ]
        
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
        await es_index.close()
        mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
