# Changelog

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
