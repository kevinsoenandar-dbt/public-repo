# Glue Iceberg Metadata Swap Promotion — Sandbox Setup

This guide walks through the manual setup needed to run the
`glue_promote` DAG end-to-end against real AWS, Snowflake, and dbt
Cloud resources.

The DAG itself and the promotion module are ready to deploy as-is
(see `dags/glue_promote_dag.py` and `plugins/glue_promote.py`). What's
left is configuring the cloud resources they depend on.

## Architecture

```
   ┌──────────────┐                    ┌──────────────────────┐
   │  Airflow     │                    │  dbt Cloud Job:      │
   │  DAG         │ ─ trigger ───────▶ │  build_staging       │
   │  glue_promote│                    │  (writes to staging) │
   └──────────────┘ ◀─ block ────────  └──────────────────────┘
        │                                       │
        │ on success                            │ writes Iceberg
        ▼                                       ▼ via REST catalog
   ┌──────────────┐                    ┌──────────────────────┐
   │ boto3        │ ─ pointer swap ──▶ │  AWS Glue Iceberg    │
   │ glue.update_ │                    │  REST catalogue      │
   │ table()      │                    │  (staging + prod DBs)│
   └──────────────┘                    └──────────┬───────────┘
                                                  │
                                                  ▼
                                       ┌──────────────────────┐
                                       │  S3 bucket           │
                                       │  (data + metadata)   │
                                       └──────────────────────┘
                                                  ▲
                                                  │ reads via
                                                  │ catalog-linked
                                                  │ database
                                       ┌──────────────────────┐
                                       │  Snowflake           │
                                       │  (consumer)          │
                                       └──────────────────────┘
```

## Prerequisites

- AWS account with admin access (for IAM, Glue, S3)
- Snowflake account on Enterprise edition or higher (for external volumes
  and catalog integrations)
- dbt Cloud account (this sandbox uses account ID `127732`)
- Airflow 2.7+ (MWAA, Astro, or self-hosted)

---

## A. AWS setup

All AWS resources should live in a single region for the sandbox.
This guide assumes `ap-southeast-2`; substitute your preferred region.

### A.1 Create the S3 bucket for Iceberg data

```bash
aws s3api create-bucket \
  --bucket cba-sandbox-iceberg \
  --region ap-southeast-2 \
  --create-bucket-configuration LocationConstraint=ap-southeast-2
```

Enable versioning (recommended for any Iceberg storage):

```bash
aws s3api put-bucket-versioning \
  --bucket cba-sandbox-iceberg \
  --versioning-configuration Status=Enabled
```

### A.2 Create two Glue databases

```bash
aws glue create-database \
  --database-input '{"Name":"analytics_staging"}' \
  --region ap-southeast-2

aws glue create-database \
  --database-input '{"Name":"analytics_prod"}' \
  --region ap-southeast-2
```

The DAG's `promote_all()` function enumerates all Iceberg tables in
`analytics_staging` and updates the same-named entries in
`analytics_prod`.

### A.3 Enable the AWS Glue Iceberg REST endpoint

The Glue Iceberg REST endpoint is what Snowflake's catalog-linked
database talks to. Enable it on the Glue Data Catalog:

1. AWS console → Glue → Data Catalog Settings → Catalogs
2. Select your default catalog (account ID)
3. Under **Catalog encryption / settings**, enable the **Iceberg REST endpoint**
4. Note the endpoint URL — typically `https://glue.ap-southeast-2.amazonaws.com/iceberg`

Reference: [AWS Glue Iceberg REST endpoint docs](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html)

### A.4 Create the IAM role for Snowflake → Glue/S3

Snowflake assumes this role when reading and writing Iceberg tables
in your account.

Create a trust policy (`snowflake-trust.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "PLACEHOLDER_SNOWFLAKE_IAM_USER"},
      "Action": "sts:AssumeRole",
      "Condition": {"StringEquals": {"sts:ExternalId": "PLACEHOLDER_EXTERNAL_ID"}}
    }
  ]
}
```

The two `PLACEHOLDER_*` values are returned by Snowflake when you
create the external volume and catalog integration in section B.
Initially, set them to your AWS account's root ARN (`arn:aws:iam::ACCOUNT:root`)
and a temporary external ID; you'll update both after step B.2.

Permissions policy (`snowflake-permissions.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::cba-sandbox-iceberg",
        "arn:aws:s3:::cba-sandbox-iceberg/*"
      ]
    },
    {
      "Sid": "GlueIcebergCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetCatalog",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:catalog",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:database/analytics_staging",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:database/analytics_prod",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:table/analytics_staging/*",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:table/analytics_prod/*"
      ]
    }
  ]
}
```

Create the role:

```bash
aws iam create-role \
  --role-name SnowflakeIcebergRole \
  --assume-role-policy-document file://snowflake-trust.json

aws iam put-role-policy \
  --role-name SnowflakeIcebergRole \
  --policy-name SnowflakeIcebergAccess \
  --policy-document file://snowflake-permissions.json
```

Note the role ARN — you'll need it in section B.

Reference: [Snowflake — Configure access to AWS Glue catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-glue)

### A.5 Create the IAM role for Airflow → Glue

Airflow's `boto3.client("glue")` calls in the promote step need
permission to read and update Glue table entries.

If running on MWAA, attach a permissions policy to the MWAA execution
role; if self-hosted, create a dedicated IAM user / role for the
Airflow worker.

Permissions policy (`airflow-glue-permissions.json`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetTables",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:catalog",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:database/analytics_staging",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:database/analytics_prod",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:table/analytics_staging/*",
        "arn:aws:glue:ap-southeast-2:ACCOUNT_ID:table/analytics_prod/*"
      ]
    }
  ]
}
```

Note that Airflow does **not** need S3 access — only catalogue-level
manipulation. The S3 access is Snowflake's responsibility.

---

## B. Snowflake setup

### B.1 Create the external volume

```sql
CREATE EXTERNAL VOLUME cba_sandbox_iceberg_vol
  STORAGE_LOCATIONS = (
    (
      NAME = 'cba-sandbox-iceberg-loc'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://cba-sandbox-iceberg/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/SnowflakeIcebergRole'
      STORAGE_AWS_EXTERNAL_ID = 'cba-sandbox-external-id'
    )
  );

DESC EXTERNAL VOLUME cba_sandbox_iceberg_vol;
```

`DESC` returns `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`
values. Update the `SnowflakeIcebergRole` trust policy in A.4 to use
these values, replacing the placeholders.

Reference: [Snowflake — Configure an external volume for Amazon S3](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3)

### B.2 Create the catalog integration

For the AWS Glue Iceberg REST endpoint:

```sql
CREATE CATALOG INTEGRATION glue_rest_integration
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'analytics_staging'      -- default namespace; per-DB overrides below
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.ap-southeast-2.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_GLUE
    WAREHOUSE = 'ACCOUNT_ID:s3tablescatalog'
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/SnowflakeIcebergRole'
    SIGV4_SIGNING_REGION = 'ap-southeast-2'
  )
  ENABLED = TRUE;
```

Reference: [Snowflake — Configure a catalog integration for AWS Glue Iceberg REST](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-glue-rest)

### B.3 Create two catalog-linked databases

One for staging, one for prod, each pointing at its corresponding
Glue database.

```sql
CREATE DATABASE analytics_staging
  LINKED_CATALOG = (
    CATALOG = 'glue_rest_integration'
    ALLOWED_NAMESPACES = ('analytics_staging')
    AUTO_REFRESH = TRUE
    REFRESH_INTERVAL_SECONDS = 30
  )
  EXTERNAL_VOLUME = 'cba_sandbox_iceberg_vol';

CREATE DATABASE analytics_prod
  LINKED_CATALOG = (
    CATALOG = 'glue_rest_integration'
    ALLOWED_NAMESPACES = ('analytics_prod')
    AUTO_REFRESH = TRUE
    REFRESH_INTERVAL_SECONDS = 30
  )
  EXTERNAL_VOLUME = 'cba_sandbox_iceberg_vol';
```

`AUTO_REFRESH = TRUE` is what makes the metadata pointer swap visible
to Snowflake without an explicit `ALTER ICEBERG TABLE … REFRESH` after
the Airflow promote task. The 30-second interval is a sandbox setting;
production may want longer.

Reference: [Snowflake — Catalog-linked databases](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)

### B.4 Create a service user and role for dbt Cloud

```sql
USE ROLE SECURITYADMIN;

CREATE ROLE dbt_cloud_runner;
CREATE USER dbt_cloud_svc
  RSA_PUBLIC_KEY = 'PASTE_PUBLIC_KEY_HERE'
  DEFAULT_ROLE = dbt_cloud_runner
  DEFAULT_WAREHOUSE = sandbox_wh;

GRANT ROLE dbt_cloud_runner TO USER dbt_cloud_svc;

-- Grants: dbt Cloud writes only to staging
GRANT USAGE ON DATABASE analytics_staging TO ROLE dbt_cloud_runner;
GRANT USAGE ON ALL SCHEMAS IN DATABASE analytics_staging TO ROLE dbt_cloud_runner;
GRANT INSERT, UPDATE, DELETE, SELECT, TRUNCATE
  ON ALL ICEBERG TABLES IN DATABASE analytics_staging TO ROLE dbt_cloud_runner;
GRANT INSERT, UPDATE, DELETE, SELECT, TRUNCATE
  ON FUTURE ICEBERG TABLES IN DATABASE analytics_staging TO ROLE dbt_cloud_runner;

-- Read-only on prod
GRANT USAGE ON DATABASE analytics_prod TO ROLE dbt_cloud_runner;
GRANT USAGE ON ALL SCHEMAS IN DATABASE analytics_prod TO ROLE dbt_cloud_runner;
GRANT SELECT ON ALL ICEBERG TABLES IN DATABASE analytics_prod TO ROLE dbt_cloud_runner;
GRANT SELECT ON FUTURE ICEBERG TABLES IN DATABASE analytics_prod TO ROLE dbt_cloud_runner;

-- Warehouse
GRANT USAGE ON WAREHOUSE sandbox_wh TO ROLE dbt_cloud_runner;
GRANT USAGE ON EXTERNAL VOLUME cba_sandbox_iceberg_vol TO ROLE dbt_cloud_runner;
GRANT USAGE ON INTEGRATION glue_rest_integration TO ROLE dbt_cloud_runner;
```

The key principle: `dbt_cloud_runner` can write to staging but not to
prod. Promotion to prod is exclusively via the platform team's role
operating the Glue catalogue from Airflow.

Generate the RSA key pair locally and paste the public key into the
`CREATE USER` statement above:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out dbt_cloud.pem -nocrypt
openssl rsa -in dbt_cloud.pem -pubout -out dbt_cloud.pub
```

Reference: [Snowflake — Key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth)

---

## C. dbt Cloud setup

This sandbox already has dbt Cloud project `192225` configured under
account `127732` (see `config/dbt_jobs.yml`). You'll add one new job
for the staging build.

### C.1 Add a new connection (if needed)

In dbt Cloud → Account settings → Connections, ensure a Snowflake
connection exists pointing at your account, using the `dbt_cloud_svc`
user with key-pair authentication. The private key (from B.4) goes in
the credentials field.

### C.2 Create a new environment

dbt Cloud → Environments → Create environment:

- **Type**: Deployment
- **Name**: `Build to Staging (Iceberg)`
- **Connection**: the Snowflake connection from C.1
- **Default database**: `analytics_staging`
- **Default schema**: choose per your dbt project conventions
- **Role**: `dbt_cloud_runner`

### C.3 Create the build_staging job

dbt Cloud → Jobs → Create job:

- **Name**: `build_staging`
- **Environment**: the one you just created
- **Commands**:
  ```
  dbt deps
  dbt build
  ```
- **Triggers**: leave all unchecked (Airflow triggers it via API)

Note the job's numeric ID (visible in the URL after creation) — you'll
need it for the Airflow Variable in section D.

### C.4 Create a service token for Airflow

dbt Cloud → Account settings → Service tokens → New token:

- **Name**: `airflow-glue-promote`
- **Permission set**: `Job Admin` (or narrower if your dbt Cloud tier
  supports per-project tokens)
- **Project access**: limit to the snowflake-sandbox project

Save the token; you'll paste it into the Airflow Connection in D.2.

Reference: [dbt Cloud — Service tokens](https://docs.getdbt.com/docs/dbt-cloud-apis/service-tokens)

---

## D. Airflow setup

### D.1 Variables

Set these via the Airflow UI (Admin → Variables) or CLI:

| Key | Value |
|---|---|
| `glue_catalog_id` | Your AWS account ID (the Glue Catalog ID) |
| `aws_region` | `ap-southeast-2` |
| `glue_staging_database` | `analytics_staging` |
| `glue_prod_database` | `analytics_prod` |
| `dbt_cloud_account_id` | `127732` |
| `dbt_cloud_build_staging_job_id` | The job ID from C.3 |

### D.2 Connection: `dbt_cloud_default`

Admin → Connections → New connection:

- **Connection ID**: `dbt_cloud_default`
- **Connection Type**: `dbt Cloud`
- **API Token**: the service token from C.4
- **Account ID**: `127732`
- **Tenant**: leave default (`cloud.getdbt.com`) unless you're on a
  regional dbt Cloud deployment

### D.3 AWS credentials

The Airflow worker needs credentials to assume / use the IAM permissions
from A.5.

- **MWAA**: attach the policy to the MWAA execution role; no extra
  configuration in Airflow.
- **Self-hosted on EC2/ECS**: use the instance/task role.
- **Local dev or other**: configure `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` env vars on the
  worker, or set up an `aws_default` Airflow Connection.

The `glue_promote.py` module uses `boto3.client("glue")` with default
credential resolution, so the standard AWS SDK credential chain
applies.

### D.4 Deploy the DAG

Copy the contents of this folder into your Airflow deployment:

- `airflow/dags/glue_promote_dag.py` → `$AIRFLOW_HOME/dags/`
- `airflow/plugins/glue_promote.py` → `$AIRFLOW_HOME/plugins/`

For MWAA: place these in your S3 DAGs bucket under the standard layout.

Install the requirements (`airflow/requirements.txt`) into the worker
environment.

---

## E. Smoke test

Once everything above is in place:

1. **Verify Snowflake can see staging.**

   ```sql
   SHOW ICEBERG TABLES IN DATABASE analytics_staging;
   ```

   Should be empty initially.

2. **Run the dbt build manually** (outside the DAG, just to seed the
   staging database):

   In dbt Cloud, manually trigger the `build_staging` job. After it
   completes, Snowflake should see the new tables in
   `analytics_staging` (auto-refresh picks up the catalogue change).

3. **Verify Glue side:**

   ```bash
   aws glue get-tables \
     --database-name analytics_staging \
     --query 'TableList[*].[Name,Parameters.metadata_location]'
   ```

   You should see entries with populated `metadata_location` values.

4. **Trigger the Airflow DAG** (`glue_promote`) manually.

   The DAG will:
   - Skip pre-flight (stub)
   - Trigger the same `build_staging` job
   - Skip DQ posting (stub)
   - Run the promotion (boto3 metadata swap)
   - Skip post-load assurance (stub)

5. **Verify prod sees the promoted data:**

   ```sql
   SELECT * FROM analytics_prod.<schema>.<table> LIMIT 10;
   ```

   You should see the same rows that were in staging after the build.

6. **Verify rollback metadata is in place:**

   ```bash
   aws glue get-table \
     --database-name analytics_prod \
     --name <table_name> \
     --query 'Table.Parameters'
   ```

   Should show both `metadata_location` and `previous_metadata_location`.

7. **(Optional) Test rollback manually:**

   ```python
   from glue_promote import rollback
   rollback(catalog_id="ACCOUNT_ID", prod_db="analytics_prod", table_name="<table>")
   ```

---

## F. Local testing

The promotion logic can be tested without any of the above setup,
using `moto` to mock the Glue API:

```bash
cd snowflake-sandbox
pip install -r airflow/requirements.txt
pytest airflow/tests/ -v
```

This validates the swap, rollback, table filtering, and edge cases
(no-op when locations match, non-Iceberg tables ignored, empty staging,
etc.) against in-memory mocks. Useful as a pre-deploy smoke test for
the module itself.

---

## Notes

- **Auto-refresh latency.** With `REFRESH_INTERVAL_SECONDS = 30`, there
  is up to a 30-second gap between the Glue pointer swap and Snowflake
  reflecting the change. For tighter SLAs, lower the interval or call
  `ALTER ICEBERG TABLE … REFRESH` explicitly as a final DAG step.
- **Schema-atomic vs per-table.** This implementation is per-table
  atomic. If model A's swap succeeds and model B's swap fails, A is
  promoted but B is not. The DAG raises on the first failure, leaving
  earlier tables already promoted. Section 5 of the recommendation doc
  covers the atomicity tradeoff in detail.
- **Production hardening.** This sandbox does not implement: OMS
  integration (steps 1, 3, 5 are stubs), audit logging beyond Airflow's
  defaults, structured rollback on partial failure, or alerting on the
  refresh latency window. Each of these is a productionisation step
  beyond the scope of this proof of concept.
