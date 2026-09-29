-- Databricks notebook source
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(catalog)
print(schema)

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_table_inventory (
    execution_id string,
    s3_bucket_name string,
    bucket_prefix string,
    key string,
    partition_key string,
    extension string,
    size bigint,
    edp_run_id string,
    period string,
    load_status string,
    last_modified_time timestamp
)
CLUSTER BY (s3_bucket_name, bucket_prefix);

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_dataset_mapping (
    dataset_name string,
    s3_bucket_name string,
    bucket_prefix string,
    volume_location string,
    dbx_catalog string,
    dbx_managed_table_schema string,
    datetime timestamp
);

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_table_candidates (
    execution_id string,
    s3_bucket_name string,
    bucket_prefix string,
    volume_location string,
    table_name string,
    total_file_count int,
    table_file_size_mb double,
    structured_file_count bigint,
    candidate_for_managed_table_creation string,
    managed_table_created string,
    recon_job_run boolean,
    recon_status string,
    recon_execution_time timestamp,
    error_message string
);

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_status (
    execution_id string,
    s3_bucket_name string,
    bucket_prefix string,
    table_name string,
    migration_status string,
    datetime timestamp
);

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_source_partition_counts (
    dataset_name string,
    edp_run_id string,
    period string,
    source_partition_count int
);

-- COMMAND ----------

create table if not exists ${catalog}.${schema}.fdo_migration_target_partition_counts (
    dataset_name string,
    edp_run_id string,
    period string,
    target_partition_count int
);