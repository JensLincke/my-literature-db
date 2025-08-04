# Changelog

## [Initial Foundation] - Pre-2025-08-04 Baseline

### Core Architecture
- **FastAPI Server Framework**: Built comprehensive REST API server using FastAPI 1.0.0
- **MongoDB Integration**: Full MongoDB backend with AsyncIOMotorClient for async database operations
- **Elasticsearch Integration**: Hybrid search architecture combining MongoDB filtering with Elasticsearch text search
- **Modular Design**: Clean separation of concerns across ~3,800 lines of production code

### Entity Management System
- **Multi-Entity Support**: Complete API coverage for 10+ OpenAlex entity types:
  - Works (research papers, books, etc.)
  - Authors (researchers, academics)
  - Concepts (research topics/fields) 
  - Institutions (universities, companies)
  - Publishers, Sources, Topics, Fields, Subfields, Domains
- **EntityRouter Factory**: Standardized route generation with consistent CRUD operations
- **BaseEntityHandler**: Unified data access layer with query optimization

### API Features
- **RESTful Endpoints**: Full CRUD operations for all entity types
- **Advanced Filtering**: OpenAlex-compatible filter syntax with complex query support
- **Pagination**: Efficient pagination with configurable page sizes (max 100 per page)
- **Field Selection**: Selective field return with `select` parameter
- **Related Entity Inclusion**: Deep object expansion with `include` parameter
- **Sorting**: Multi-field sorting with ascending/descending options

### Search Capabilities
- **Hybrid Search Architecture**: 
  - Elasticsearch for full-text search on `/search` endpoints
  - MongoDB for structured filtering on list endpoints
- **Search Validation**: Proper error handling preventing filter/sort misuse in search mode
- **Performance Optimization**: Estimated counts for large datasets to avoid expensive operations

### ID Format Support (Pre-Direct URLs)
- **Multiple ID Formats**: Support for various identifier schemes:
  - OpenAlex IDs (`W1234567890`, `A1234567890`, etc.)
  - DOI handling (`doi:10.1234/example`)
  - MAG IDs (`mag:1234567890`)
  - OpenAlex prefix format (`openalex:W1234567890`)
- **URL-Safe DOI Routing**: Special path handling for DOI slashes in entity routes
- **ID Normalization**: Automatic conversion between ID formats

### Data Import & Management
- **OpenAlex Importer**: Complete data pipeline for importing OpenAlex snapshots
- **Index Management**: MongoDB index optimization and Elasticsearch synchronization
- **Batch Processing**: Efficient bulk import with progress tracking
- **Database Utilities**: MongoDB operations and maintenance scripts

### Development Infrastructure
- **Comprehensive Testing**: pytest-based test suite with asyncio support
- **Debug Utilities**: Development tools for API validation and troubleshooting
- **Logging System**: Structured logging with file rotation and multiple levels
- **Configuration Management**: Environment-based configuration with sensible defaults

### Performance Features
- **Async Architecture**: Full async/await implementation for high concurrency
- **Connection Pooling**: Optimized database connection management
- **Query Optimization**: Efficient MongoDB aggregation pipelines
- **Caching Strategy**: Performance-focused data access patterns

### Documentation & Examples
- **Comprehensive README**: Detailed API usage examples and setup instructions
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation
- **Example Queries**: Ready-to-use curl examples for all major functionality

### Technical Specifications
- **Languages**: Python 3.10+ with type hints throughout
- **Dependencies**: FastAPI, Motor (async MongoDB), Elasticsearch, Pydantic
- **Database**: MongoDB with optimized indexes
- **Search**: Elasticsearch integration for text search
- **Architecture**: Microservice-ready with containerization support

---

## [2025-08-04] - Direct ID URL Support & Test Suite Cleanup

### Added
- **Direct ID URL Access**: Added support for accessing entities directly from root URLs
  - `http://server:9020/W1492801337` - Direct work access
  - `http://server:9020/A5102590311` - Direct author access  
  - `http://server:9020/C123456789` - Direct concept access
  - `http://server:9020/I123456789` - Direct institution access
  - `http://server:9020/doi:10.1007/978-3-540-24614-5_17` - Direct DOI access
  - `http://server:9020/mag:1492801337` - Direct MAG ID access
- Smart entity type detection based on ID prefixes (W=works, A=authors, C=concepts, etc.)
- Fallback mechanism to try multiple entity types if auto-detection fails
- Support for query parameters (`?select=id,title`, `?include=works,authors`)
- Comprehensive test suite with 11 test cases covering all direct ID scenarios

### Fixed
- **HTTP Status Codes for Timeouts**: Query and aggregation timeouts now return proper HTTP 408 (Request Timeout) instead of HTTP 200 with error message
  - Affects all database operations with 10-second timeout limits  
  - Provides proper REST API semantics for client error handling
  - Returns structured error details including timeout duration and error type
- DOI URL routing issues with special characters (colons and slashes)
- Route precedence to ensure existing endpoints (`/works`, `/authors`, etc.) remain unaffected

### Improved
- **Test Suite Consolidation**: Cleaned up and organized test files
  - Reduced from 51 scattered tests to 37 organized tests
  - Created `test_id_handling.py` for comprehensive ID functionality testing
  - Simplified `test_id_shortcuts.py` and `test_handler_unit.py`
  - Removed debug artifacts and duplicate test files
  - Eliminated verbose debug output from test runs

### Removed
- Redundant test files: `test_mag_only.py`, `test_doi_fix.py`, `test_id_formats.py`
- Debug demonstration files: `test_multikey_index.py`, `test_openalex_filtering.py`, `test_cites_filtering.py`, `test_url_encoding.py`
- Interactive debug prints and verbose validation from remaining test files

### Technical Details
- Added two new FastAPI routes: `/doi:{doi_path:path}` and `/{entity_id}`
- Enhanced `serve_openalex.py` with direct ID routing logic
- Maintained backward compatibility with existing API endpoints
- All tests passing (37 tests across 4 organized test modules)
