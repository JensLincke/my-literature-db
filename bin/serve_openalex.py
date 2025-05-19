#!/usr/bin/env python3
"""
OpenAlex API Server

This script creates a simple REST API server to query the local OpenAlex SQLite database.
It provides endpoints to search and retrieve works, authors, and concepts.

Usage:
    python serve_openalex.py [--port PORT] [--host HOST]

Options:
    --port PORT     Port to run the server on (default: 8000)
    --host HOST     Host to bind the server to (default: 127.0.0.1)
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs

from http.server import HTTPServer, BaseHTTPRequestHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("openalex-server")

# SQLite database file path
DB_PATH = os.path.expanduser("~/openalex.db")

# Maximum number of results to return per page
MAX_RESULTS_PER_PAGE = 100

class OpenAlexRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for OpenAlex API server"""
    
    def _set_headers(self, status_code=200, content_type="application/json"):
        """Set response headers"""
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
    
    def _send_json_response(self, data, status_code=200):
        """Send JSON response"""
        self._set_headers(status_code)
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    
    def _send_error(self, message, status_code=400):
        """Send error response"""
        self._send_json_response({"error": message}, status_code)
    
    def _parse_query_params(self):
        """Parse query parameters from the URL"""
        query_string = self.path.split("?", 1)[1] if "?" in self.path else ""
        return parse_qs(query_string)
    
    def _get_db_connection(self):
        """Get a connection to the SQLite database"""
        try:
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            return conn
        except sqlite3.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            return None
    
    def _execute_query(self, query, params=None):
        """Execute a database query and return the results"""
        conn = self._get_db_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = [dict(row) for row in cursor.fetchall()]
            cursor.close()
            return results
        except sqlite3.Error as e:
            logger.error(f"Database query error: {str(e)}")
            return None
        finally:
            conn.close()
    
    def do_GET(self):
        """Handle GET requests"""
        try:
            # Parse the path to determine the endpoint
            path = self.path.split("?")[0] if "?" in self.path else self.path
            
            # Handle different endpoints
            if path == "/":
                self._handle_root()
            elif path == "/works":
                self._handle_works()
            elif path.startswith("/works/"):
                work_id = path[7:]
                if work_id.endswith("/json"):
                    self._handle_work_json(work_id[:-5])
                else:
                    self._handle_work_detail(work_id)
            elif path == "/authors":
                self._handle_authors()
            elif path.startswith("/authors/"):
                author_id = path[9:]
                if author_id.endswith("/json"):
                    self._handle_author_json(author_id[:-5])
                else:
                    self._handle_author_detail(author_id)
            elif path == "/concepts":
                self._handle_concepts()
            elif path.startswith("/concepts/"):
                concept_id = path[10:]
                if concept_id.endswith("/json"):
                    self._handle_concept_json(concept_id[:-5])
                else:
                    self._handle_concept_detail(concept_id)
            elif path == "/search":
                self._handle_search()
            else:
                self._send_error("Endpoint not found", 404)
        
        except Exception as e:
            logger.error(f"Error processing request: {str(e)}")
            self._send_error("Internal server error", 500)
    
    def _handle_root(self):
        """Handle requests to the root endpoint"""
        api_info = {
            "name": "OpenAlex Local API",
            "version": "1.0.0",
            "endpoints": [
                {"path": "/works", "description": "List and search works"},
                {"path": "/works/{id}", "description": "Get details of a specific work"},
                {"path": "/works/{id}/json", "description": "Get original JSON of a specific work"},
                {"path": "/authors", "description": "List and search authors"},
                {"path": "/authors/{id}", "description": "Get details of a specific author"},
                {"path": "/authors/{id}/json", "description": "Get original JSON of a specific author"},
                {"path": "/concepts", "description": "List and search concepts"},
                {"path": "/concepts/{id}", "description": "Get details of a specific concept"},
                {"path": "/concepts/{id}/json", "description": "Get original JSON of a specific concept"},
                {"path": "/search", "description": "Search across all entities"}
            ]
        }
        
        # Check database status
        conn = self._get_db_connection()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM metadata WHERE key = 'last_import'")
                result = cursor.fetchone()
                if result:
                    api_info["last_import"] = result["value"]
                
                # Get entity counts
                for entity in ["works", "authors", "concepts"]:
                    cursor.execute(f"SELECT COUNT(*) as count FROM {entity}")
                    result = cursor.fetchone()
                    if result:
                        api_info[f"{entity}_count"] = result["count"]
                
                cursor.close()
            except sqlite3.Error as e:
                logger.error(f"Database status check error: {str(e)}")
            finally:
                conn.close()
        
        self._send_json_response(api_info)
    
    def _handle_works(self):
        """Handle requests to the /works endpoint"""
        params = self._parse_query_params()
        
        # Parse query parameters
        page = int(params.get("page", ["1"])[0])
        per_page = min(int(params.get("per_page", ["25"])[0]), MAX_RESULTS_PER_PAGE)
        offset = (page - 1) * per_page
        
        # Build the query
        query = "SELECT * FROM works WHERE 1=1"
        query_params = []
        
        # Apply filters if provided
        if "title" in params:
            query += " AND title LIKE ?"
            query_params.append(f"%{params['title'][0]}%")
        
        if "year" in params:
            query += " AND publication_year = ?"
            query_params.append(int(params["year"][0]))
        
        if "type" in params:
            query += " AND type = ?"
            query_params.append(params["type"][0])
        
        # Add order by and pagination
        query += " ORDER BY cited_by_count DESC LIMIT ? OFFSET ?"
        query_params.extend([per_page, offset])
        
        # Execute the query
        results = self._execute_query(query, query_params)
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        # Process JSON fields
        for result in results:
            if "author_ids" in result:
                result["author_ids"] = json.loads(result["author_ids"])
            if "concept_ids" in result:
                result["concept_ids"] = json.loads(result["concept_ids"])
        
        # Count total matching works for pagination info
        count_query = query.split("ORDER BY")[0].replace("SELECT *", "SELECT COUNT(*) as count")
        count_params = query_params[:-2]  # Remove LIMIT and OFFSET
        count_result = self._execute_query(count_query, count_params)
        
        total_count = count_result[0]["count"] if count_result else 0
        
        response = {
            "meta": {
                "count": len(results),
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page
            },
            "results": results
        }
        
        self._send_json_response(response)
    
    def _handle_work_detail(self, work_id):
        """Handle requests to the /works/{id} endpoint"""
        # Clean the work ID
        work_id = work_id.strip()
        
        # Query the database
        query = "SELECT * FROM works WHERE id = ?"
        results = self._execute_query(query, [work_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results:
            self._send_error("Work not found", 404)
            return
        
        work = results[0]
        
        # Process JSON fields
        if "author_ids" in work:
            work["author_ids"] = json.loads(work["author_ids"])
        if "concept_ids" in work:
            work["concept_ids"] = json.loads(work["concept_ids"])
        
        # Remove the json_data field from the response
        if "json_data" in work:
            del work["json_data"]
        
        # Get author details
        if work["author_ids"]:
            # Create placeholders for the IN clause
            placeholders = ", ".join(["?" for _ in work["author_ids"]])
            author_query = f"SELECT id, display_name FROM authors WHERE id IN ({placeholders})"
            author_results = self._execute_query(author_query, work["author_ids"])
            
            if author_results:
                work["authors"] = author_results
        
        # Get concept details
        if work["concept_ids"]:
            # Create placeholders for the IN clause
            placeholders = ", ".join(["?" for _ in work["concept_ids"]])
            concept_query = f"SELECT id, display_name, level FROM concepts WHERE id IN ({placeholders})"
            concept_results = self._execute_query(concept_query, work["concept_ids"])
            
            if concept_results:
                work["concepts"] = concept_results
        
        self._send_json_response(work)
    
    def _handle_work_json(self, work_id):
        """Handle requests to the /works/{id}/json endpoint"""
        # Clean the work ID
        work_id = work_id.strip()
        
        # Query the database for the JSON data
        query = "SELECT json_data FROM works WHERE id = ?"
        results = self._execute_query(query, [work_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results or not results[0].get("json_data"):
            self._send_error("Work not found or no JSON data available", 404)
            return
        
        # Parse the JSON data
        try:
            json_data = json.loads(results[0]["json_data"])
            self._send_json_response(json_data)
        except json.JSONDecodeError:
            self._send_error("Invalid JSON data stored for this work", 500)
    
    def _handle_authors(self):
        """Handle requests to the /authors endpoint"""
        params = self._parse_query_params()
        
        # Parse query parameters
        page = int(params.get("page", ["1"])[0])
        per_page = min(int(params.get("per_page", ["25"])[0]), MAX_RESULTS_PER_PAGE)
        offset = (page - 1) * per_page
        
        # Build the query
        query = "SELECT * FROM authors WHERE 1=1"
        query_params = []
        
        # Apply filters if provided
        if "name" in params:
            query += " AND display_name LIKE ?"
            query_params.append(f"%{params['name'][0]}%")
        
        # Add order by and pagination
        query += " ORDER BY cited_by_count DESC LIMIT ? OFFSET ?"
        query_params.extend([per_page, offset])
        
        # Execute the query
        results = self._execute_query(query, query_params)
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        # Count total matching authors for pagination info
        count_query = query.split("ORDER BY")[0].replace("SELECT *", "SELECT COUNT(*) as count")
        count_params = query_params[:-2]  # Remove LIMIT and OFFSET
        count_result = self._execute_query(count_query, count_params)
        
        total_count = count_result[0]["count"] if count_result else 0
        
        response = {
            "meta": {
                "count": len(results),
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page
            },
            "results": results
        }
        
        self._send_json_response(response)
    
    def _handle_author_detail(self, author_id):
        """Handle requests to the /authors/{id} endpoint"""
        # Clean the author ID
        author_id = author_id.strip()
        
        # Query the database
        query = "SELECT * FROM authors WHERE id = ?"
        results = self._execute_query(query, [author_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results:
            self._send_error("Author not found", 404)
            return
        
        author = results[0]
        
        # Remove the json_data field from the response
        if "json_data" in author:
            del author["json_data"]
        
        # Get the author's works
        works_query = "SELECT id, title, publication_year, cited_by_count, type FROM works " + \
                     "WHERE author_ids LIKE ? ORDER BY cited_by_count DESC LIMIT 100"
        works_results = self._execute_query(works_query, [f'%"{author_id}"%'])
        
        if works_results:
            author["works"] = works_results
        
        self._send_json_response(author)
    
    def _handle_author_json(self, author_id):
        """Handle requests to the /authors/{id}/json endpoint"""
        # Clean the author ID
        author_id = author_id.strip()
        
        # Query the database for the JSON data
        query = "SELECT json_data FROM authors WHERE id = ?"
        results = self._execute_query(query, [author_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results or not results[0].get("json_data"):
            self._send_error("Author not found or no JSON data available", 404)
            return
        
        # Parse the JSON data
        try:
            json_data = json.loads(results[0]["json_data"])
            self._send_json_response(json_data)
        except json.JSONDecodeError:
            self._send_error("Invalid JSON data stored for this author", 500)
    
    def _handle_concepts(self):
        """Handle requests to the /concepts endpoint"""
        params = self._parse_query_params()
        
        # Parse query parameters
        page = int(params.get("page", ["1"])[0])
        per_page = min(int(params.get("per_page", ["25"])[0]), MAX_RESULTS_PER_PAGE)
        offset = (page - 1) * per_page
        
        # Build the query
        query = "SELECT * FROM concepts WHERE 1=1"
        query_params = []
        
        # Apply filters if provided
        if "name" in params:
            query += " AND display_name LIKE ?"
            query_params.append(f"%{params['name'][0]}%")
        
        if "level" in params:
            query += " AND level = ?"
            query_params.append(int(params["level"][0]))
        
        # Add order by and pagination
        query += " ORDER BY works_count DESC LIMIT ? OFFSET ?"
        query_params.extend([per_page, offset])
        
        # Execute the query
        results = self._execute_query(query, query_params)
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        # Count total matching concepts for pagination info
        count_query = query.split("ORDER BY")[0].replace("SELECT *", "SELECT COUNT(*) as count")
        count_params = query_params[:-2]  # Remove LIMIT and OFFSET
        count_result = self._execute_query(count_query, count_params)
        
        total_count = count_result[0]["count"] if count_result else 0
        
        response = {
            "meta": {
                "count": len(results),
                "total_count": total_count,
                "page": page,
                "per_page": per_page,
                "total_pages": (total_count + per_page - 1) // per_page
            },
            "results": results
        }
        
        self._send_json_response(response)
    
    def _handle_concept_detail(self, concept_id):
        """Handle requests to the /concepts/{id} endpoint"""
        # Clean the concept ID
        concept_id = concept_id.strip()
        
        # Query the database
        query = "SELECT * FROM concepts WHERE id = ?"
        results = self._execute_query(query, [concept_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results:
            self._send_error("Concept not found", 404)
            return
        
        concept = results[0]
        
        # Remove the json_data field from the response
        if "json_data" in concept:
            del concept["json_data"]
        
        # Get the top works for this concept
        works_query = "SELECT id, title, publication_year, cited_by_count, type FROM works " + \
                     "WHERE concept_ids LIKE ? ORDER BY cited_by_count DESC LIMIT 100"
        works_results = self._execute_query(works_query, [f'%"{concept_id}"%'])
        
        if works_results:
            concept["works"] = works_results
        
        self._send_json_response(concept)
    
    def _handle_concept_json(self, concept_id):
        """Handle requests to the /concepts/{id}/json endpoint"""
        # Clean the concept ID
        concept_id = concept_id.strip()
        
        # Query the database for the JSON data
        query = "SELECT json_data FROM concepts WHERE id = ?"
        results = self._execute_query(query, [concept_id])
        
        if results is None:
            self._send_error("Database error", 500)
            return
        
        if not results or not results[0].get("json_data"):
            self._send_error("Concept not found or no JSON data available", 404)
            return
        
        # Parse the JSON data
        try:
            json_data = json.loads(results[0]["json_data"])
            self._send_json_response(json_data)
        except json.JSONDecodeError:
            self._send_error("Invalid JSON data stored for this concept", 500)
    
    def _handle_search(self):
        """Handle requests to the /search endpoint"""
        params = self._parse_query_params()
        
        if "q" not in params:
            self._send_error("Search query parameter 'q' is required")
            return
        
        search_query = params["q"][0]
        page = int(params.get("page", ["1"])[0])
        per_page = min(int(params.get("per_page", ["25"])[0]), MAX_RESULTS_PER_PAGE)
        offset = (page - 1) * per_page
        
        # Search in works
        works_query = "SELECT id, title, publication_year, type, 'work' as entity_type " + \
                     "FROM works WHERE title LIKE ? OR abstract LIKE ? " + \
                     "ORDER BY cited_by_count DESC LIMIT ? OFFSET ?"
        works_params = [f"%{search_query}%", f"%{search_query}%", per_page, offset]
        works_results = self._execute_query(works_query, works_params)
        
        # Search in authors
        authors_query = "SELECT id, display_name, 'author' as entity_type " + \
                       "FROM authors WHERE display_name LIKE ? " + \
                       "ORDER BY cited_by_count DESC LIMIT ? OFFSET ?"
        authors_params = [f"%{search_query}%", per_page, offset]
        authors_results = self._execute_query(authors_query, authors_params)
        
        # Search in concepts
        concepts_query = "SELECT id, display_name, level, 'concept' as entity_type " + \
                        "FROM concepts WHERE display_name LIKE ? " + \
                        "ORDER BY works_count DESC LIMIT ? OFFSET ?"
        concepts_params = [f"%{search_query}%", per_page, offset]
        concepts_results = self._execute_query(concepts_query, concepts_params)
        
        # Combine results
        all_results = []
        if works_results:
            all_results.extend(works_results)
        if authors_results:
            all_results.extend(authors_results)
        if concepts_results:
            all_results.extend(concepts_results)
        
        # Sort by relevance (simple implementation - could be improved)
        all_results.sort(key=lambda x: x.get("cited_by_count", 0) if "cited_by_count" in x else x.get("works_count", 0), reverse=True)
        
        response = {
            "meta": {
                "query": search_query,
                "count": len(all_results),
                "page": page,
                "per_page": per_page
            },
            "results": all_results[:per_page]
        }
        
        self._send_json_response(response)

def main():
    parser = argparse.ArgumentParser(description="Start the OpenAlex API server")
    parser.add_argument("--port", type=int, default=8000, help="Port to run the server on")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host to bind the server to")
    args = parser.parse_args()
    
    # Check if the database file exists
    if not os.path.isfile(DB_PATH):
        logger.error(f"Database file not found at {DB_PATH}")
        logger.error("Run import_openalex.py first to create the database")
        sys.exit(1)
    
    # Start the server
    server_address = (args.host, args.port)
    httpd = HTTPServer(server_address, OpenAlexRequestHandler)
    
    logger.info(f"Starting OpenAlex API server at http://{args.host}:{args.port}")
    logger.info("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Server stopped")
    
    httpd.server_close()

if __name__ == "__main__":
    main()