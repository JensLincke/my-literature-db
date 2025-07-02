# My Literature DB

Local serving of OpenAlex research literature database.

## Overview

This project provides a FastAPI server to query and serve a local copy of the OpenAlex academic database.

## Setup

1. Install dependencies:
```bash
pip install fastapi uvicorn motor pymongo
```

2. Configure the MongoDB URI in `start.sh` or set the `MONGO_URI` environment variable.

3. Run the server:
```bash
./bin/start.sh
```

## Example API Usage

Once the server is running (default on port 9020), you can interact with the API using these examples:

### Get a Work by ID

```bash
# Get a specific work by its OpenAlex ID
curl "http://localhost:9020/works/W2741809807"

# Get only specific fields
curl "http://localhost:9020/works/W2741809807?select=id,title,publication_year,cited_by_count"

# Include related entities (authors, concepts, etc.)
curl "http://localhost:9020/works/W2741809807?include=authors,concepts"
```

### Search for Works

**Note**: Search endpoints use Elasticsearch for text search and do not support `filter` or `sort` parameters. These parameters will return HTTP 400 errors if used with search. Use the list endpoints (e.g., `/works`) for filtering and sorting.

```bash
# Basic search
curl "http://localhost:9020/works/search?q=machine%20learning"

# Search with pagination
curl "http://localhost:9020/works/search?q=climate%20change&skip=0&limit=10"

# Search with field selection
curl "http://localhost:9020/works/search?q=deep%20learning&select=id,title,publication_year&limit=5"
```

### List and Filter Works

```bash
# List recent works
curl "http://localhost:9020/works?filter=publication_year:2023&per_page=10"

# Filter by citation count
curl "http://localhost:9020/works?filter=cited_by_count:>100&sort=cited_by_count:desc"

# Filter with multiple criteria
curl "http://localhost:9020/works?filter=publication_year:>2020,cited_by_count:>10&per_page=20"
```

**Note:** For performance reasons, filtered queries return `total_count: -1` instead of an exact count. This avoids expensive counting operations on large datasets.

### Other Entity Types

The same patterns work for other entity types (authors, institutions, concepts, etc.):

```bash
# Get an author by ID
curl "http://localhost:9020/authors/A5023888391"

# Search for authors
curl "http://localhost:9020/authors/search?q=John%20Smith"

# Get an institution
curl "http://localhost:9020/institutions/I27837315"

# Search for concepts
curl "http://localhost:9020/concepts/search?q=machine%20learning"
```

## Architecture

The API follows a modular design pattern:
- `serve_openalex.py`: Main entry point and FastAPI application
- `handlers.py`: Core business logic handlers for entity operations
- `entity_router.py`: Factory for creating consistent API endpoints
- `api_utils.py`: Shared utilities, parameter models, and documentation helpers

