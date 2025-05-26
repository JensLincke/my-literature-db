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

## Architecture

The API follows a modular design pattern:
- `serve_openalex.py`: Main entry point and FastAPI application
- `handlers.py`: Core business logic handlers for entity operations
- `entity_router.py`: Factory for creating consistent API endpoints
- `api_utils.py`: Shared utilities, parameter models, and documentation helpers

