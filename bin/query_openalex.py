#!/usr/bin/env python3
"""
Command Line Interface for OpenAlex Database

This script provides a simple CLI to query the OpenAlex MongoDB database.

Usage:
    python query_openalex.py status
"""
import argparse
import logging
import os
from pymongo import MongoClient

def get_database_status(db):
    """Get the estimated number of entries in each collection."""
    stats = {}
    for collection_name in db.list_collection_names():
        stats[collection_name] = db[collection_name].estimated_document_count()
    return stats

def main():
    parser = argparse.ArgumentParser(description="Query the OpenAlex database")
    parser.add_argument("command", choices=["status"], help="Command to execute")
    parser.add_argument("--mongo-uri", type=str, default="mongodb://localhost:27017", help="MongoDB connection URI")
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("query_openalex")

    # Connect to MongoDB
    client = MongoClient(args.mongo_uri)
    db = client.openalex

    if args.command == "status":
        logger.info("Fetching database status...")
        stats = get_database_status(db)
        print("\nDatabase contents:")
        print("-------------------------")
        total = 0
        for collection, count in stats.items():
            print(f"{collection}: {count:,} documents")
            total += count
        print(f"\nTotal: {total:,} documents")
        print("-------------------------")

if __name__ == "__main__":
    main()