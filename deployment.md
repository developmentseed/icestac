# Iceberg Catalog Deployment Options

This document evaluates Iceberg catalog options for production deployment of `icestac`. The criteria are deployment simplicity, vendor lock-in, cost, and DuckDB read access — since DuckDB is the primary query engine for downstream consumers of the Iceberg tables this project writes.

## Overview

| | AWS Glue | Self-Hosted REST | Apache Polaris | Project Nessie |
|---|---|---|---|---|
| Deployment complexity | Low | Medium | Medium | Medium |
| Vendor lock-in | High | Very Low | Low | Low |
| Cost (catalog infra) | Pay-per-request | Compute + DB (~$10–30/mo) | Compute + DB (~$25–50/mo) | Compute only; free managed tier |
| Cloud support | AWS only | Any | AWS / GCP / Azure | AWS / GCP / Azure + MinIO |
| DuckDB auth | IAM / access keys | Bearer token / OAuth2 | OAuth2 | Bearer token / OAuth2 |
| Object storage | S3 only | S3 / GCS / Azure / MinIO | S3 / GCS / Azure | S3 / GCS / Azure / MinIO |
| Special features | Native AWS integration | Flexible backends | Multi-cloud design | Git-like versioning |

All four options expose the Iceberg REST Catalog API that DuckDB's `iceberg` extension supports. The differences are in who manages the catalog infrastructure and what backends are available.

---

## Option 1: AWS Glue

### Description

AWS Glue is a fully managed data catalog service. When used as an Iceberg catalog, it stores table metadata natively and exposes a REST-compatible endpoint. No catalog server to run or manage.

### Architecture

- Glue Data Catalog stores Iceberg table metadata
- S3 stores data files and Iceberg metadata files
- IAM controls all access
- PyIceberg uses the `glue` catalog type

PyIceberg configuration:

```bash
ICESTAC_CATALOG_TYPE=glue
ICESTAC_AWS_REGION=us-east-1
ICESTAC_WAREHOUSE_PATH=s3://your-bucket/warehouse/
```

### Deployment Complexity: Low

Enable Glue in your AWS account and point PyIceberg at it. No containers, no databases to manage. AWS CDK can create the Glue database and IAM roles.

### Vendor Lock-In: High

- Glue API is proprietary; no standard interface below the REST layer
- Object storage locked to S3
- IAM is the only auth mechanism
- Migrating away requires re-creating catalog metadata in a new system

### Cost

- ~$1 per 100,000 catalog API requests
- S3 storage at standard rates
- Glue crawlers and ETL jobs are billed separately (not needed for icestac)
- Costs grow with table count and request volume, but are typically low for a STAC catalog with moderate traffic

### DuckDB Connection

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL aws; LOAD aws;

-- Option A: Use environment credentials or instance role
CALL load_aws_credentials();

-- Option B: Explicit access key
CREATE OR REPLACE SECRET glue_auth (
    TYPE S3,
    KEY_ID 'AKIA...',
    SECRET 'your-secret-key',
    REGION 'us-east-1'
);

ATTACH 'https://glue.us-east-1.amazonaws.com/iceberg' AS catalog (
    TYPE ICEBERG,
    WAREHOUSE 'your-glue-database-name'
);

SELECT * FROM catalog.icestac.your_table LIMIT 10;
```

### Object Storage

S3 only.

### Best Fit

Teams already on AWS that want zero catalog infrastructure overhead and are comfortable with full AWS lock-in.

---

## Option 2: Self-Hosted REST Catalog

### Description

Run an open-source Iceberg REST catalog server as a container. The REST spec is the standard that all major Iceberg engines (DuckDB, Spark, Trino, PyIceberg) use, so this option has essentially no lock-in. The catalog server needs a metadata backend — DynamoDB (AWS-native) or PostgreSQL are the most common choices.

Common implementations:

- [`tabulario/iceberg-rest`](https://github.com/tabular-io/iceberg-rest-image) — minimal, reference implementation
- [`lakekeeper`](https://github.com/lakekeeper/lakekeeper) — more production-ready with auth, multi-warehouse support

### Architecture

```
DuckDB / PyIceberg
       |
       | REST API
       v
REST Catalog Server (Docker / ECS / K8s)
       |
       |-- Metadata --> DynamoDB or PostgreSQL
       |-- Data ------> S3 / GCS / Azure Blob / MinIO
```

PyIceberg configuration for REST with token auth:

```bash
ICESTAC_CATALOG_TYPE=rest
ICESTAC_CATALOG_URI=https://catalog.example.com
ICESTAC_REST_TOKEN=your-bearer-token
ICESTAC_WAREHOUSE_PATH=s3://your-bucket/warehouse/
```

For OAuth2 credential flow:

```bash
ICESTAC_REST_CREDENTIAL=client-id:client-secret
```

### Deployment Complexity: Medium

Run a container and provision a metadata backend. On AWS this looks like:

- ECS Fargate task (or Lambda for low traffic) running the catalog image
- DynamoDB table for metadata (serverless, no DB to manage)
- Application Load Balancer or API Gateway for HTTPS
- Secrets Manager for auth tokens

A basic ECS + DynamoDB deployment can be done with ~100 lines of CDK.

### Vendor Lock-In: Very Low

- REST spec is engine-agnostic; any Iceberg-compatible engine connects the same way
- DynamoDB backend can be swapped for PostgreSQL without changing the client interface
- Data files stay in standard Iceberg format; migration is a metadata-only operation

### Cost

- ECS Fargate: ~$10–30/month for a small always-on task (0.25 vCPU / 0.5 GB)
- DynamoDB on-demand: near-zero for catalog traffic (metadata reads/writes)
- PostgreSQL on RDS: ~$15–25/month for the smallest instance (db.t4g.micro)
- Cheapest option at scale if catalog traffic is low

### DuckDB Connection

```sql
INSTALL iceberg; LOAD iceberg;

-- Bearer token auth
CREATE OR REPLACE SECRET rest_auth (
    TYPE BEARER,
    TOKEN 'your-bearer-token'
);

ATTACH 'https://catalog.example.com' AS catalog (
    TYPE ICEBERG,
    WAREHOUSE 's3://your-bucket/warehouse/',
    SECRET rest_auth
);

SELECT * FROM catalog.icestac.your_table LIMIT 10;
```

For OAuth2 client credentials, DuckDB handles the token exchange automatically when the catalog server supports it.

### Object Storage

S3, GCS, Azure Blob, MinIO. The catalog server passes storage credentials to the client via the REST credential vending API.

### Best Fit

Teams that want flexibility and low lock-in, especially on AWS where ECS + DynamoDB keeps the stack cohesive without coupling to Glue. Also the best option for multi-cloud or hybrid environments.

---

## Option 3: Apache Polaris

### Description

Apache Polaris is an open-source, multi-cloud Iceberg catalog originally developed by Snowflake and donated to the Apache Software Foundation. It implements the Iceberg REST spec and adds access control, principal management, and multi-warehouse support.

A managed offering (Snowflake Open Catalog) is available if you prefer not to self-host.

### Architecture

- Polaris server exposes the Iceberg REST API
- Uses PostgreSQL for metadata storage
- Supports AWS, GCP, and Azure object storage
- Auth is OAuth2; Polaris issues tokens to clients

### Deployment Complexity: Medium

Polaris provides deployment scripts targeting managed PostgreSQL on each cloud (RDS on AWS, Azure Database for PostgreSQL, Cloud SQL on GCP). A production setup needs:

- Compute to run the Polaris server (ECS, GKE, AKS, or VM)
- Managed PostgreSQL instance
- OAuth2 client registration for each service/principal

### Vendor Lock-In: Low

Polaris is an Apache project with a broad contributor base. The REST API it exposes is the same spec used by all other options. Data stays in standard Iceberg format.

### Cost

- Compute + managed PostgreSQL: ~$25–50/month minimum for a small setup
- Snowflake Open Catalog managed offering: available with per-query pricing (no self-hosted infrastructure)

### DuckDB Connection

```sql
INSTALL iceberg; LOAD iceberg;

-- OAuth2 is handled during ATTACH via credential configuration
ATTACH 'https://polaris.example.com/api/catalog' AS catalog (
    TYPE ICEBERG,
    WAREHOUSE 'your-warehouse-name',
    CLIENT_ID 'your-client-id',
    CLIENT_SECRET 'your-client-secret'
);

SELECT * FROM catalog.icestac.your_table LIMIT 10;
```

### Object Storage

S3, GCS, Azure Blob. Polaris does not support MinIO or other S3-compatible stores — it targets major cloud object stores only. This is a meaningful constraint if your environment uses MinIO.

### Best Fit

Teams that need multi-cloud catalog governance with a strong open-source backing and are on AWS, GCP, or Azure (not MinIO). Also suitable if you want a managed option through Snowflake without self-hosting.

---

## Option 4: Project Nessie

### Description

Project Nessie is an open-source (Apache 2.0), Dremio-backed Iceberg catalog that adds Git-like branching and tagging to table metadata. You can create branches of your catalog, commit changes, and merge — similar to version control for data. This makes it well-suited for workflows that need data governance, experimentation isolation, or point-in-time rollback beyond Iceberg's built-in time-travel.

A managed offering (Dremio Arctic) is available with a free tier.

### Architecture

- Nessie server exposes the Iceberg REST API (plus its own versioning API)
- Metadata stored in an embedded RocksDB (simple) or external store (Postgres, DynamoDB, MongoDB)
- Supports S3, GCS, Azure ADLS, and S3-compatible storage including MinIO
- Can run as a standalone JAR, Docker container, or AWS Lambda

### Deployment Complexity: Medium

Simpler to start than Polaris (single JAR or container with embedded storage). Production setups benefit from an external metadata backend (DynamoDB or PostgreSQL) for durability and horizontal scaling.

Docker Compose example (extends the local dev setup in this project):

```yaml
nessie:
  image: ghcr.io/projectnessie/nessie:latest
  ports:
    - "19120:19120"
  environment:
    - nessie.catalog.default-warehouse.location=s3://warehouse/
```

PyIceberg configuration:

```bash
ICESTAC_CATALOG_TYPE=rest
ICESTAC_CATALOG_URI=http://localhost:19120/iceberg
ICESTAC_WAREHOUSE_PATH=s3://warehouse/
```

### Vendor Lock-In: Low

Nessie metadata features (branching, tagging) are optional additions on top of the standard REST catalog API. Data files are standard Iceberg; a migration is a metadata-only operation. The Nessie-specific branching features are only accessible via Nessie clients — but simply ignoring them leaves you with a standard Iceberg REST catalog.

### Cost

- Self-hosted: compute + storage only (similar to self-hosted REST)
- Dremio Arctic managed: free tier available; usage-based pricing above that

### DuckDB Connection

```sql
INSTALL iceberg; LOAD iceberg;

-- Bearer token auth (Nessie can be run with or without auth)
CREATE OR REPLACE SECRET nessie_auth (
    TYPE BEARER,
    TOKEN 'your-bearer-token'
);

ATTACH 'https://nessie.example.com/iceberg' AS catalog (
    TYPE ICEBERG,
    WAREHOUSE 's3://your-bucket/warehouse/',
    SECRET nessie_auth
);

-- Query from the default (main) branch
SELECT * FROM catalog.icestac.your_table LIMIT 10;
```

For OIDC / OAuth2, the token exchange follows the same pattern as the self-hosted REST option.

### Object Storage

S3, GCS, Azure ADLS, and S3-compatible stores including MinIO. This is the broadest storage support of the four options.

### Best Fit

Teams that want open-source flexibility with Git-like data governance, or that need MinIO support alongside a managed fallback option (Dremio Arctic). Also a good fit for local development parity since Nessie runs as a single container.

---

## Choosing a Catalog

For `icestac` — a project with a primary AWS target, MinIO in local dev, and DuckDB as the downstream query engine — the decision comes down to operational overhead versus flexibility:

**Start with the self-hosted REST catalog (Option 2)** if you are on AWS and want to keep infrastructure simple. ECS + DynamoDB is a natural fit alongside the Lambda ingestion pipeline already planned for this project, avoids Glue lock-in, and the same REST interface works in local dev (which already uses a REST catalog via Docker Compose). Bearer token auth keeps DuckDB integration straightforward.

**Use AWS Glue (Option 1)** if you want zero catalog infrastructure to manage and are comfortable with the AWS lock-in. Glue is the simplest path to production if your data consumers are primarily AWS-native.

**Use Project Nessie (Option 4)** if you want the local development catalog (Docker Compose) and the production catalog to be the same software, value Git-like versioning, or need MinIO support in production. Nessie's Dremio Arctic managed tier provides a no-infrastructure path with a free tier.

**Use Apache Polaris (Option 3)** if you need multi-cloud catalog governance with fine-grained access control and do not require MinIO support. Best suited for organizations already invested in Snowflake's ecosystem or that need the Open Catalog managed offering.

For all options: DuckDB uses the same `INSTALL iceberg; LOAD iceberg;` extension and the same `ATTACH` syntax. The only difference between options is the endpoint URL and the auth mechanism (IAM vs Bearer token vs OAuth2).
