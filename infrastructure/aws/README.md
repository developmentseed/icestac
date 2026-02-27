# Deployment

This directory contains an example deployment using AWS Glue.

To synthesize the stack:
```bash
cd infrastructure/aws
uv run --group deploy python cdk synth
```

To deploy the stack:
```bash
export AWS_REGION=...
cd infrastructure/aws
uv run --group deploy python cdk deploy
```

To load some sample items:
```bash
source .env-dev
uv run main.py
```

To query the new table:

```sql
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
INSTALL spatial; LOAD spatial;

CREATE SECRET (
  TYPE s3,
  PROVIDER credential_chain
);

ATTACH '{account-id}' AS catalog (
    TYPE iceberg,
    ENDPOINT_TYPE 'glue'
);


SELECT * FROM catalog.icestac.icesat2_boreal_v3_1_agb LIMIT 10;
```
