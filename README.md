# icestac

## Overview

This project creates a Python library (`icestac`) that uses rustac to convert STAC item collections to arrow tables and writes them to Apache Iceberg tables.

The goal is a stac-geoparquet-backed system that can be used to maintain a **STAC Catalog** with many collections and support real-time ingestion. It will include an event-driven AWS pipeline for ingesting STAC items into an Iceberg catalog via SNS/SQS and Lambda.

## Development

### Tests

```bash
# Run all tests
uv run pytest
```

### Local Instance

**Environment Configuration** (`.env`):
```bash
# REST catalog endpoint
ICESTAC_CATALOG_NAME=rest_catalog
ICESTAC_CATALOG_TYPE=rest
ICESTAC_CATALOG_URI=http://localhost:8181
ICESTAC_WAREHOUSE_PATH=s3://warehouse/

# S3/MinIO storage for PyIceberg data file I/O
ICESTAC_S3_ENDPOINT=http://localhost:9000
ICESTAC_S3_ACCESS_KEY_ID=admin
ICESTAC_S3_SECRET_ACCESS_KEY=password
ICESTAC_S3_PATH_STYLE_ACCESS=true
```

**Starting the local environment:**
```bash
docker compose up
```

**Note:** [main.py](./main.py) currently uses an older API signature and needs to be updated to match the current `create_item_table` function signature.

## Current Implementation Status

### Core Library (`src/icestac/`)

#### Item Table Module (`src/icestac/item_table.py`) - ✓ IMPLEMENTED

Core functions for managing STAC item Iceberg tables:

**`sanitize_collection_id(collection_id: str) -> str`**
- Converts STAC collection IDs to valid, deterministic Iceberg table names
- Uses lowercase + underscore normalization with 8-character hash suffix for uniqueness

**`create_item_table(arrow_schema: ArrowSchema, collection_id: str, catalog: Catalog, namespace: str) -> Table`**
- Creates or loads Iceberg table from stac-geoparquet Arrow schema
- Converts Arrow schema to Iceberg schema with manual field ID assignment
- Creates table partitioned by datetime month using `MonthTransform`
- Validates schema for required STAC fields

**Limitations:**
- Temporal partitioning is hardcoded to monthly (TODO: make configurable)
- No collection-level metadata management solution yet

#### Schema Module (`src/icestac/schema.py`) - ✓ IMPLEMENTED

Schema validation and enforcement:

**`IcestacItem`** - Pydantic model extending stac-pydantic Item with required `collection` field

**`get_schema_from_item(item: dict) -> Schema`**
- Validates STAC item and returns Arrow schema with enforced required fields

**`enforce_required_fields(schema: Schema) -> Schema`**
- Marks required STAC fields as non-nullable in Arrow schema

**`validate_schema(schema: Schema) -> None`**
- Validates Arrow schema contains all required STAC fields

#### Config Module (`src/icestac/config.py`) - ✓ IMPLEMENTED

**`IcebergCatalogConfig`** - Pydantic settings for catalog configuration
- Loads from environment variables with `ICESTAC_` prefix
- Supports catalog types: `rest`, `glue`, `hive`, `sql`
- Environment variables:
  - Catalog: `CATALOG_NAME`, `CATALOG_TYPE`, `CATALOG_URI`, `WAREHOUSE_PATH`, `AWS_REGION`
  - REST auth: `REST_TOKEN`, `REST_CREDENTIAL`
  - SQL: `SQL_ECHO`
  - S3/MinIO: `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_PATH_STYLE_ACCESS`

**`get_catalog_properties() -> dict[str, str]`**
- Generates PyIceberg catalog properties from settings

**`load_catalog() -> Catalog`**
- Factory method that creates PyIceberg Catalog instance

#### Lambda Handler Module (`src/icestac/lambda_handler.py`) - NOT IMPLEMENTED

Placeholder for AWS Lambda handler.

### Testing Infrastructure (`tests/`)

#### Test Coverage - ✓ IMPLEMENTED

- **`tests/conftest.py`**: Pytest fixtures for test catalog, sample STAC items, and Arrow tables
- **`tests/test_item_table.py`**: Unit tests for `sanitize_collection_id` and `create_item_table`
- **`tests/test_config.py`**: Unit tests for catalog configuration and settings validation
- **`tests/test_schema.py`**: Unit tests for schema validation and enforcement

### Dependencies

**Core** (`pyproject.toml` dependencies):
- `pyarrow>=23.0.0` - Arrow table operations
- `pyiceberg[pyiceberg-core]>=0.10.0` - Iceberg table management
- `rustac[arrow]>=0.9.3` - STAC to Arrow conversion with arro3 schemas
- `stac-pydantic>=3.4.0` - STAC item validation
- `pydantic-settings>=2.12.0` - Environment-based configuration

**Development** (dev dependency group):
- `pytest>=9.0.2` - Testing framework
- `sqlalchemy>=2.0.46` - SQL catalog backend for tests

**Deployment** (deploy dependency group):
- `aws-cdk-lib>=2.236.0` - AWS infrastructure as code

**Still needed for Lambda handler:**
- `boto3` - Lambda/SNS/SQS/S3 interactions
- `aws-lambda-powertools` - Structured logging and tracing
- `moto` - AWS service mocking for tests

## Next Steps

### Immediate Priorities

1. **Design Collection Metadata Management**
   - Determine approach for storing and managing collection-level metadata
   - Options: Separate metadata table, catalog namespace properties, or external store
   - Should track: collection description, temporal extent, spatial extent, schema versions

2. **Complete Lambda Handler** (`src/icestac/lambda_handler.py`)
   - Implement SNS event parsing
   - Add collection grouping logic
   - Integrate `create_item_table` function and config module
   - Add error handling and structured logging
   - Write integration tests

3. **Enhance Item Table Module**
   - Make partitioning strategy configurable (currently hardcoded to monthly)
   - Add support for schema evolution
   - Add write statistics/metadata

### Future Work: AWS Infrastructure (CDK)

**Planned Stack Structure**:
```
infrastructure/
├── app.py
├── stacks/
    ├── stac_ingestion_stack.py    # SNS → SQS → Lambda pipeline
    └── iceberg_catalog_stack.py   # Optional: Glue/DynamoDB catalog
```

**STAC Ingestion Stack Components**:
- SNS Topic for incoming STAC items
- SQS Queue with batching and DLQ
- Lambda Function with icestac library
- CloudWatch Alarms for monitoring

**Additional Dependencies Needed**:
- `constructs`
- `aws-cdk.aws-lambda-python-alpha` (Python Lambda bundling)

## Development Workflow

### Local Testing Strategy

**Implemented:**
- PyIceberg with SQL catalog (SQLite) for unit tests
- Pytest for test framework
- Docker Compose with MinIO and Iceberg REST catalog for local development

**Planned:**
- Moto for mocking AWS services in Lambda handler tests
- Helper script to simulate SNS events locally
- Optional: LocalStack for complete AWS simulation

### Local Development Environment

**Docker Compose Services**:
- **Iceberg REST Catalog** - `localhost:8181` for metadata operations
- **MinIO** - S3-compatible storage at `localhost:9000` (API) and `localhost:9001` (Console)
  - Credentials: `admin` / `password`
  - Warehouse bucket: `s3://warehouse/`
- **MinIO Client (mc)** - Initializes warehouse bucket on startup

## Key Design Decisions

### Decided

1. **Table naming**: Sanitized collection ID with 8-character hash suffix for uniqueness
2. **Partitioning**: Monthly partitioning by datetime field (hardcoded, to be made configurable)
3. **Schema conversion**: Manual field ID assignment to avoid pyiceberg limitations
4. **Schema validation**: Required STAC fields marked as non-nullable using Pydantic models
5. **Configuration**: Environment variables with `ICESTAC_` prefix using Pydantic settings
6. **Testing catalog**: In-memory SQL catalog with SQLite for unit tests
7. **STAC to Arrow conversion**: Use rustac library with arro3 schemas

### To Be Decided

1. **Collection metadata management**: How to store and query collection-level metadata (description, extents, schema versions)?
2. **Iceberg catalog type for production**: AWS Glue (managed AWS) vs REST (self-hosted/managed) vs SQL (RDS)?
3. **Configurable partitioning strategies**: Support daily, monthly, yearly, or custom partitioning?
4. **Schema evolution policy**: Strict or flexible? How to handle schema changes across items in same collection?
5. **Batch size**: How many STAC items per SQS batch for optimal performance?
6. **Error handling**: Retry strategy for failed items? DLQ processing?
7. **S3 bucket structure**: How to organize Iceberg table data and metadata?
>>>>>>> a2b56ec (initial commit)
