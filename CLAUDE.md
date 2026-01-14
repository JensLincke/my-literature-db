# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a FastAPI server that provides a local REST API for querying the OpenAlex academic database. It combines MongoDB for structured data storage with Elasticsearch for full-text search capabilities.

## Development Commands

### Running the Server

```bash
# Start the development server (runs on port 9020)
./bin/start.sh

# Or directly with uvicorn
cd bin && uvicorn serve_openalex:app --host 0.0.0.0 --port 9020 --reload
```

The server connects to MongoDB via the `MONGO_URI` environment variable (defaults to `mongodb://localhost:27017`).

### Testing

```bash
# Run all tests
pytest test/

# Run specific test file
pytest test/test_id_shortcuts.py -v

# Run with coverage
pytest test/ --cov=bin

# Run single test
pytest test/test_id_handling.py::test_direct_work_id -v
```

Tests are configured in `pyproject.toml` with asyncio support enabled automatically.

## Architecture Overview

### Core Components

**serve_openalex.py** - Main FastAPI application entry point
- Initializes MongoDB client and database connection
- Creates entity handlers for all 10 entity types (works, authors, concepts, institutions, publishers, sources, topics, fields, subfields, domains)
- Registers all API routes via `create_entity_routers()`
- Provides two special root-level routes for direct ID access: `/{entity_id}` and `/doi:{doi_path:path}`

**entity_router.py** - Factory pattern for generating standardized REST endpoints
- `EntityRouter` class creates consistent CRUD routes for each entity type
- Generates four standard endpoints per entity:
  1. `GET /{entities}` - List/filter with pagination
  2. `GET /{entities}/search` - Elasticsearch full-text search (if search is in `related_entities`)
  3. `GET /{entities}/{id}` or `GET /{entities}/doi:{path}` - Get single entity by ID
  4. `GET /{entities}/group_by/{field}` - Aggregation/analytics
- Uses `related_entities` parameter to determine which routes to enable (e.g., `["search", "works", "authors"]`)

**handlers.py** - Business logic layer (`BaseEntityHandler` class)
- `list_entities()` - Handles filtering, sorting, pagination, field selection
- `get_entity()` - Retrieves single entity with ID format normalization
- `search_entities()` - Delegates to Elasticsearch for text search
- `group_entities()` - MongoDB aggregation for analytics
- Contains logic for ID format conversion (OpenAlex, DOI, MAG, etc.)

**filter_utils.py** - Query parsing utilities
- `parse_filter_param()` - Converts OpenAlex-style filters to MongoDB queries (e.g., `publication_year:>2020` → `{"publication_year": {"$gt": 2020}}`)
- `parse_sort_param()` - Converts sort strings to MongoDB sort specs
- `parse_select_param()` - Converts field selection to MongoDB projections
- Supports operators: `:`, `>`, `<`, `>=`, `<=`, `!=`, `.search:`, `.equals:`

**api_utils.py** - Shared models and utilities
- Pydantic models for request/response validation: `PaginationParams`, `SearchParams`, `PaginatedResponse`, `SearchResponse`
- Entity-specific filter parameter classes: `WorksFilterParams`, `AuthorsFilterParams`, etc.
- Documentation helper functions for auto-generated API docs

**elastic_index.py** - Elasticsearch integration
- `ESIndex` class provides async Elasticsearch client
- `search_documents()` - Full-text search with optional filters
- `bulk_index_documents()` - Batch indexing for performance
- Used only for `/search` endpoints; regular filtering uses MongoDB

### Data Flow

1. **List/Filter requests** (`GET /works?filter=year:2023`):
   - EntityRouter → BaseEntityHandler.list_entities() → MongoDB query → filter_utils for parsing

2. **Search requests** (`GET /works/search?q=neural+networks`):
   - EntityRouter → BaseEntityHandler.search_entities() → Elasticsearch query → ESIndex

3. **Get by ID** (`GET /works/W123` or `GET /W123`):
   - EntityRouter or root route → BaseEntityHandler.get_entity() → MongoDB find_one with ID normalization

### ID Format Handling

The API supports multiple ID formats for all entities:

- **OpenAlex IDs**: `W1234567890`, `A1234567890`, `C1234567890`, etc.
- **DOI format**: `doi:10.1234/example` (automatically handles URL encoding issues with slashes)
- **MAG IDs**: `mag:1234567890`
- **OpenAlex prefix**: `openalex:W1234567890`

ID normalization happens in `BaseEntityHandler.get_entity()` which tries multiple query strategies to find matches.

### Important Implementation Details

**Search vs Filter Separation**
- Search endpoints (`/entities/search`) use Elasticsearch and do NOT support `filter` or `sort` parameters (returns HTTP 400)
- List endpoints (`/entities`) use MongoDB and support full filter/sort functionality
- This is intentional - search is for text relevance, filtering is for structured queries

**Performance Optimizations**
- Filtered queries return `total_count: -1` instead of expensive exact counts
- Uses `estimated_document_count()` for unfiltered queries only
- 10-second timeout on all database operations (returns HTTP 408 on timeout)
- Author ID filters use indexed `_author_ids` field instead of slow nested array searches

**Async Architecture**
- All database operations use `async`/`await` with Motor (async MongoDB driver)
- Elasticsearch client is also async
- Entity handlers are async throughout

**Field Selection and Inclusion**
- `select` parameter controls which fields to return from base entity
- `include` parameter adds related entities (e.g., `?include=works,authors,concepts`)
- Related entity queries happen after base entity retrieval

## Common Tasks

### Adding a new filter operator

Edit `filter_utils.py` and add to `FILTER_OPERATIONS` dict. Then update `parse_filter_param()` logic to handle the new operator.

### Adding a new entity type

1. Add collection initialization in `serve_openalex.py` startup event
2. Add handler creation: `handlers["newtype"] = BaseEntityHandler(db.newtype, "newtype")`
3. Call `EntityRouter()` in `create_entity_routers()` with appropriate parameters
4. Create filter params class in `api_utils.py` (e.g., `NewTypeFilterParams`)

### Debugging API queries

- Server logs go to `bin/server.log` (rotates at 10MB)
- Enable debug logging by setting log level to DEBUG in serve_openalex.py
- Look for print statements in handlers.py showing query construction
- Check `entity_router.py` for verbose timing logs when debug enabled

### Running import/indexing scripts

```bash
# Import OpenAlex snapshot
python bin/import_openalex.py

# Index to Elasticsearch
python bin/index_to_elasticsearch.py

# Update existing indexes
python bin/update_openalex_index.py
```

## Testing Strategy

- **test_id_shortcuts.py** - API-level acceptance tests for ID format support
- **test_id_handling.py** - Comprehensive ID handling and normalization tests
- **test_handler_unit.py** - Unit tests for BaseEntityHandler methods
- **test_readme.py** - Validates examples in README.md actually work

Tests use pytest with async support (configured in pyproject.toml).
