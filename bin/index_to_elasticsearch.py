"""
Script to index MongoDB data into Elasticsearch
"""

import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from es_handler import ESHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def index_collection(db, es_handler, collection_name: str, batch_size: int = 1000):
    """Index a MongoDB collection into Elasticsearch"""
    collection = db[collection_name]
    total_docs = await collection.count_documents({})
    logger.info(f"Indexing {total_docs} documents from {collection_name}")
    
    cursor = collection.find({}, {"id": 1, "display_name": 1})  # Only retrieve id and display_name fields
    indexed = 0
    batch = []
    
    async for doc in cursor:
        # Create simplified document with only needed fields
        simple_doc = {
            "id": doc["id"],
            "display_name": doc["display_name"]
        }
        
        # Add document to batch
        batch.append((doc["id"], simple_doc))
        
        if len(batch) >= batch_size:
            # Index batch
            tasks = [
                es_handler.index_document(collection_name, doc_id, document)
                for doc_id, document in batch
            ]
            await asyncio.gather(*tasks)
            
            indexed += len(batch)
            logger.info(f"Indexed {indexed}/{total_docs} documents in {collection_name}")
            batch = []
    
    # Index remaining documents
    if batch:
        tasks = [
            es_handler.index_document(collection_name, doc_id, document)
            for doc_id, document in batch
        ]
        await asyncio.gather(*tasks)
        indexed += len(batch)
        logger.info(f"Indexed {indexed}/{total_docs} documents in {collection_name}")

async def main():
    # Initialize MongoDB client
    mongo_client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = mongo_client.openalex
    
    # Initialize Elasticsearch handler
    es_handler = ESHandler()
    
    try:
        # Initialize Elasticsearch indices
        await es_handler.initialize()
        
        # Collections to index
        # ignore "works", "authors", "concepts", "institutions",  "sources" while experimenting
        collections = ["publishers"]
        
        # Index each collection
        for collection in collections:
            try:
                await index_collection(db, es_handler, collection)
            except Exception as e:
                logger.error(f"Error indexing {collection}: {e}")
                continue
        
    finally:
        # Clean up
        await es_handler.close()
        mongo_client.close()

if __name__ == "__main__":
    asyncio.run(main())
