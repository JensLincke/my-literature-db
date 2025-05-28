"""
Elasticsearch Index for Full Text Search
This module provides an interface to interact with Elasticsearch for indexing and searching openalex data.
It just provides basic functionality to index documents and perform full text search with strict matching.
"""

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import logging

logger = logging.getLogger(__name__)

class ESIndex:
    def __init__(self, host="localhost", port=9200):
        # Configure client for Elasticsearch without security
        self.client = AsyncElasticsearch(
            hosts=[f"http://{host}:{port}"],
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=10
        )
        self.index_prefix = "openalex"

    async def initialize(self):
        """Initialize Elasticsearch indices and mappings"""
        indices = ["publishers", "concepts", "institutions",  "sources", "works", "authors"]
        
        for index in indices:
            index_name = f"{self.index_prefix}_{index}"
            
            # Only create index if it doesn't exist
            if not await self.client.indices.exists(index=index_name):
                # Create index with proper mappings
                await self.client.indices.create(
                    index=index_name,
                    body={
                        "mappings": {
                            "properties": {
                                "id": {"type": "keyword"},
                                "display_name": {
                                    "type": "text",
                                    "analyzer": "standard",
                                    "fields": {
                                        "keyword": {"type": "keyword"}
                                    }
                                }
                            }
                        }
                    }
                )
                logger.info(f"Created index: {index_name}")

    async def index_document(self, index: str, doc_id: str, document: dict):
        """Index a document in Elasticsearch"""
        index_name = f"{self.index_prefix}_{index}".lower()
        try:
            await self.client.index(
                index=index_name,
                id=doc_id,
                document=document
            )
        except Exception as e:
            logger.error(f"Error indexing document {doc_id} in {index_name}: {e}")
            raise

    async def bulk_index_documents(self, index: str, documents: list):
        """Bulk index documents in Elasticsearch"""
        index_name = f"{self.index_prefix}_{index}".lower()
        
        # Convert documents to bulk format with create operation
        actions = [
            {
                '_index': index_name,
                '_id': doc[0],     # doc_id
                '_source': doc[1],  # document
                '_op_type': 'create'  # Will fail if document exists instead of overwriting
            }
            for doc in documents
        ]
        
        try:
            # Force refresh to make documents immediately visible
            success, failed = await async_bulk(
                self.client,
                actions,
                chunk_size=1000,
                max_chunk_bytes=100 * 1024 * 1024,  # 100MB
                request_timeout=30,
                refresh=True,
                raise_on_error=False  # Don't raise on document exists errors
            )
            
            # Handle failed operations
            failed_list = failed if isinstance(failed, list) else []
            already_exists = 0
            for err in failed_list:
                if isinstance(err, dict) and 'document already exists' in str(err.get('error', '')):
                    already_exists += 1
            
            failed_count = len(failed_list) - already_exists
            
            logger.info(f"Bulk indexing to {index_name}: {len(documents)} documents processed, "
                       f"{success} new, {already_exists} already existed, {failed_count} failed")
            
            return success, failed_list
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            raise

    async def search(self, index: str, query: str, skip: int = 0, limit: int = 10, filter_query: dict = None):
        """Search documents in Elasticsearch"""
        index_name = f"{self.index_prefix}_{index}"
        
        # Build the search query
        # Build query based on input type
        query_body = query if isinstance(query, dict) else {
            "simple_query_string": {
                "query": query,
                "fields": ["display_name"],
                "default_operator": "and",  # Force AND operation between terms
                "analyze_wildcard": False,  # Disable wildcard analysis
                "auto_generate_synonyms_phrase_query": False,  # Disable automatic phrase queries
                "flags": "PHRASE|PRECEDENCE|AND|NOT|OR|WHITESPACE"  # Enable exact phrase matching with quotes
            }
        } if query else {"match_all": {}}

        search_body = {
            "query": query_body,
            "from": skip,
            "size": limit,
            "sort": [
                {"_score": {"order": "desc"}}
            ]
        }

        # Add filters if provided
        if filter_query:
            original_query = search_body["query"]
            search_body["query"] = {
                "bool": {
                    "must": original_query,
                    "filter": filter_query
                }
            }

        try:
            # Convert to lowercase to ensure consistent index naming
            index_name = index_name.lower()
            result = await self.client.search(
                index=index_name,
                body=search_body
            )
            
            # Format the response to match our API's structure
            hits = result["hits"]
            return {
                "total": hits["total"]["value"],
                "results": [
                    {
                        "id": hit["_id"],
                        "score": hit["_score"],
                        **hit["_source"]
                    } for hit in hits["hits"]
                ]
            }
            
            hits = result["hits"]["hits"]
            total = result["hits"]["total"]["value"]
            
            return {
                "results": [hit["_source"] for hit in hits],
                "total": total,
                "page": skip // limit + 1,
                "per_page": limit,
                "_meta": {"scores": [hit["_score"] for hit in hits]}
            }
            
        except Exception as e:
            logger.error(f"Error searching in {index_name}: {e}")
            raise

    async def close(self):
        """Close the Elasticsearch client"""
        await self.client.close()
