"""
Elasticsearch handler for managing search operations
"""

from elasticsearch import AsyncElasticsearch
import logging

logger = logging.getLogger(__name__)

class ESHandler:
    def __init__(self, host="localhost", port=9200):
        self.client = AsyncElasticsearch([{"host": host, "port": port}])
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
                                        "exact": {
                                            "type": "text",
                                            "analyzer": "standard",
                                            "search_analyzer": "standard",
                                            "search_quote_analyzer": "keyword"
                                        }
                                    }
                                },
                                "search_blob": {
                                    "type": "text",
                                    "analyzer": "standard",
                                    "fields": {
                                        "exact": {
                                            "type": "text",
                                            "analyzer": "standard",
                                            "search_analyzer": "standard",
                                            "search_quote_analyzer": "keyword"
                                        }
                                    }
                                }
                            }
                        },
                        "settings": {
                            "analysis": {
                                "analyzer": {
                                    "default": {
                                        "type": "standard"
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
        
        # Build the search query
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "query_string": {
                                "fields": ["display_name.exact^2", "search_blob.exact"],
                                "query": query,
                                "default_operator": "AND",
                                "quote_field_suffix": ".exact"
                            }
                        }
                    ]
                }
            },
            "from": skip,
            "size": limit,
            "sort": [
                {"_score": {"order": "desc"}},
                {"cited_by_count": {"order": "desc"}}
            ]
        }

        # Add filters if provided
        if filter_query:
            search_body["query"]["bool"]["filter"] = filter_query

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
