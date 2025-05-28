"""
Elasticsearch handler for managing search operations
"""

from elasticsearch import AsyncElasticsearch
import logging

logger = logging.getLogger(__name__)

class ESHandler:
    def __init__(self, host="localhost", port=9200):
        # Configure client for Elasticsearch without security
        self.client = AsyncElasticsearch(
            f"http://{host}:{port}"
        )
        self.index_prefix = "openalex"

    async def initialize(self):
        """Initialize Elasticsearch indices and mappings"""
        # ignore "works", "authors", "concepts", "institutions",  "sources" while experimenting
        indices = ["publishers"]
        
        for index in indices:
            index_name = f"{self.index_prefix}_{index}"
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
        index_name = f"{self.index_prefix}_{index}"
        try:
            await self.client.index(
                index=index_name,
                id=doc_id,
                document=document
            )
        except Exception as e:
            logger.error(f"Error indexing document {doc_id} in {index_name}: {e}")
            raise

    async def search(self, index: str, query: str, skip: int = 0, limit: int = 10, filter_query: dict = None):
        """Search documents in Elasticsearch"""
        index_name = f"{self.index_prefix}_{index}"
        
        # Build the search query for strict full text search
        search_body = {
            "query": {
                "simple_query_string": {
                    "query": query,
                    "fields": ["display_name"],
                    "default_operator": "and",  # Force AND operation between terms
                    "analyze_wildcard": False,  # Disable wildcard analysis
                    "auto_generate_synonyms_phrase_query": False,  # Disable automatic phrase queries
                    "flags": "PHRASE|PRECEDENCE|AND|NOT|OR|WHITESPACE"  # Enable exact phrase matching with quotes
                }
            },
            "from": skip,
            "size": limit,
            "sort": [
                {"_score": {"order": "desc"}}
            ]
        }

        # Add filters if provided
        if filter_query:
            search_body["query"] = {
                "bool": {
                    "must": [search_body["query"]],
                    "filter": filter_query
                }
            }

        try:
            result = await self.client.search(
                index=index_name,
                body=search_body
            )
            
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
