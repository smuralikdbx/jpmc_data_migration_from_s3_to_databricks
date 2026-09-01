# Databricks notebook source
# DBTITLE 1,Imports
import boto3
import os
import re
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, IntegerType, DateType
from datetime import timezone
from pyspark.sql.functions import col, regexp_extract, lit, lower, count, sum, when, regexp_extract_all, size, array_join, current_timestamp, split, expr, concat
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Widget Parameters
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("s3_inventory_table", "")
dbutils.widgets.text("candidate_table", "")
dbutils.widgets.text("dataset_mapping_file_location", "")
dbutils.widgets.text("archive_dataset_mapping_file_location", "")
dbutils.widgets.text("dataset_mapping_table", "")
dbutils.widgets.text("job_id", "")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("dataset_mapping_file", "")

# COMMAND ----------

# DBTITLE 1,Retrieve Widget Values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
s3_inventory_table = dbutils.widgets.get("s3_inventory_table")
candidate_table = dbutils.widgets.get("candidate_table")
dataset_mapping_file_location = dbutils.widgets.get("dataset_mapping_file_location")
archive_dataset_mapping_file_location = dbutils.widgets.get("archive_dataset_mapping_file_location")
dataset_mapping_table = dbutils.widgets.get("dataset_mapping_table")
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
dataset_mapping_file = dbutils.widgets.get("dataset_mapping_file")
s3_bucket_name = dataset_mapping_file.replace("dataset_mapping_", "").replace(".csv", "")

# COMMAND ----------

# DBTITLE 1,Construct Fully Qualified Names
execution_id = job_id + "-" + run_id
candidate_table = catalog + "." + schema + "." + candidate_table
s3_inventory_table = catalog + "." + schema + "." + s3_inventory_table
dataset_mapping_table = catalog + "." + schema + "." + dataset_mapping_table
dataset_mapping_tbl = dataset_mapping_table
print(dataset_mapping_tbl)

# COMMAND ----------

# DBTITLE 1,Definition - Get list of files from S3
from datetime import datetime

def list_s3_files_recursive(path, s3_bucket_name, bucket_prefix):
    all_files = []
    try:
        files = dbutils.fs.ls(path)
        #   print(path)
    except Exception as e:
        print(f"Error reading path {path}: {e}")
        return all_files

    for f in files:
        if f.path.endswith("/"):
            all_files.extend(list_s3_files_recursive(f.path, s3_bucket_name, bucket_prefix))
        else:
            ext = f.name.split(".")[-1] if "." in f.name else None
            all_files.append({
              "execution_id": execution_id,
              "s3_bucket_name": s3_bucket_name,
              "bucket_prefix": bucket_prefix,
              "path": f.path,
              "file_name": f.name,
              "last_modified_time": datetime.fromtimestamp(f.modificationTime / 1000),
              "size": f.size,
              "extension": ext
          })
    return all_files

# COMMAND ----------

# DBTITLE 1,Dataset Mapping
dataset_mapping_schema = StructType([
    StructField("dataset_name", StringType(), True),
    StructField("s3_bucket_name", StringType(), True),
    StructField("bucket_prefix", StringType(), True),
    StructField("volume_location", StringType(), True),
    StructField("dbx_catalog", StringType(), True),
    StructField("dbx_managed_table_schema", StringType(), True),
    StructField("datetime", TimestampType(), True)
])

df_dataset_mapping = spark.createDataFrame([], dataset_mapping_schema)

def file_exists_in_volume(file_path: str) -> bool:
    try:
        # Split into folder and filename
        folder, file_name = file_path.rsplit("/", 1)
        files = dbutils.fs.ls(folder)
        return any(f.name == file_name for f in files)
    except Exception:
        return False
    
if file_exists_in_volume(f"{dataset_mapping_file_location}/{dataset_mapping_file}"):
    df_dataset_mapping = spark.read \
        .option("header", "true") \
        .schema(dataset_mapping_schema) \
        .csv(f"{dataset_mapping_file_location}/{dataset_mapping_file}")\
        .withColumn("datetime", current_timestamp())

    display(df_dataset_mapping)

    dataset_mapping_table = DeltaTable.forName(spark, dataset_mapping_table)

    dataset_mapping_insert_cols = ["dataset_name", "s3_bucket_name", "bucket_prefix", "volume_location", "dbx_catalog", "dbx_managed_table_schema", "datetime"]

    dataset_mapping_table.alias("t").merge(
        df_dataset_mapping.alias("s"),
        "t.dataset_name = s.dataset_name AND \
        t.s3_bucket_name = s.s3_bucket_name AND \
        t.bucket_prefix = s.bucket_prefix"
    ).whenNotMatchedInsert(values={col: f"s.{col}" for col in dataset_mapping_insert_cols}).execute()

# COMMAND ----------

# DBTITLE 1,Read Files from S3 Location and Load into Inventory Table
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

prefixes = []
df_dataset_inventory = None
total_missing_partitions = 0  # Track total missing partitions loaded

s3_files_schema = StructType([
    StructField("execution_id",       StringType(),      True),
    StructField("s3_bucket_name",     StringType(),      True),
    StructField("bucket_prefix",      StringType(),      True),
    StructField("path",               StringType(),      True),
    StructField("file_name",          StringType(),      True),
    StructField("last_modified_time", TimestampType(),   True),
    StructField("size",               LongType(),        True),
    StructField("extension",          StringType(),      True),
])

for df in df_dataset_mapping.select("s3_bucket_name", "bucket_prefix", "volume_location").distinct().collect():

    try:
        print(f"Processing dataset: {df['bucket_prefix']}")
        # path = f"s3://{df['s3_bucket_name']}/{df['bucket_prefix']}"
        volume_path=f"{df['volume_location']}"
        file_list = list_s3_files_recursive(volume_path, df['s3_bucket_name'], df['bucket_prefix'])

        # Convert to DataFrame
        s3_files_df = spark.createDataFrame(file_list, s3_files_schema)

        # Add key column and extract partitions
        s3_files_df = (
                    s3_files_df
                    .withColumn("key", F.expr("regexp_replace(path, 'dbfs:', '')"))
                    .withColumn("edp_run_id", F.regexp_extract(F.lower(F.col("key")), r"edp_run_id=([^/]+)",1))
                    # Extract all partition segments (xxx=yyy)
                    .withColumn("partition_segments", F.expr("filter(split(key, '/'), x -> x like '%=%')"))
                    # Remove edp_run_id segment
                    .withColumn("period_segments", F.expr("filter(partition_segments, x -> lower(x) not like 'edp_run_id=%')"))
                    # Get value of the deepest remaining partition
                    .withColumn("period", F.when(F.size("period_segments") > 0, F.regexp_extract(F.element_at("period_segments", -1), r"=(.*)", 1)).otherwise(F.lit(None)))
                    .drop("partition_segments", "period_segments")
        )

        print(f"Total files in S3: {s3_files_df.count()}")

        # Get existing partitions (edp_run_id + period) from inventory
        existing_partitions_df = spark.sql(f"""
            SELECT DISTINCT edp_run_id, period
            FROM {s3_inventory_table}
            WHERE s3_bucket_name = "{df['s3_bucket_name']}" and bucket_prefix = "{df['bucket_prefix']}"
        """)

        existing_partition_count = existing_partitions_df.count()
        print(f"Existing partitions in inventory: {existing_partition_count}")

        # Find missing partitions using left_anti join on both edp_run_id AND period
        s3_files_df = s3_files_df.alias("s3").join(
            existing_partitions_df.alias("inv"),
            (col("s3.edp_run_id") == col("inv.edp_run_id")) &
            (col("s3.period") == col("inv.period")),
            "left_anti"
        )

        if s3_files_df.count() == 0:
            print(f"No new files to process for  {df['bucket_prefix']}")
            continue
        else:
            print(f"New files to load: {s3_files_df.count()}")

        df_dataset_inventory = s3_files_df\
        .withColumn(
            "key",
            expr("regexp_replace(path, 'dbfs:', '')")
        )

        df_dataset_inventory_filtered = df_dataset_inventory.alias("inventory") \
            .join(
                df_dataset_mapping.alias("mapping"),
                (col("inventory.s3_bucket_name") == col("mapping.s3_bucket_name")) &
                (col("inventory.bucket_prefix") == col("mapping.bucket_prefix")),
                "inner"
            ) \
            .select("inventory.*")

        partition_expr = regexp_extract_all(lower(col("key")), lit(r"([a-zA-Z0-9_]+)="))

        df_dataset_inventory_filtered = (
            df_dataset_inventory_filtered
                    .withColumn("partition_key", when(size(partition_expr) > 0, array_join(partition_expr, ", ")).otherwise(F.lit(None)))
                    .withColumn("edp_run_id", F.regexp_extract(F.lower(F.col("key")), r"edp_run_id=([^/]+)",1))
                    # Extract all partition segments (xxx=yyy)
                    .withColumn("partition_segments", F.expr("filter(split(key, '/'), x -> x like '%=%')"))
                    # Remove edp_run_id segment
                    .withColumn("period_segments", F.expr("filter(partition_segments, x -> lower(x) not like 'edp_run_id=%')"))
                    # Get value of the deepest remaining partition
                    .withColumn("period", F.when(F.size("period_segments") > 0, F.regexp_extract(F.element_at("period_segments", -1), r"=(.*)", 1)).otherwise(F.lit(None)))
                    .drop("partition_segments", "period_segments")
        )

        # ADD execution_id back as a literal
        df_dataset_inventory_filtered = df_dataset_inventory_filtered.withColumn("execution_id", lit(execution_id))

        df_dataset_inventory_filtered = df_dataset_inventory_filtered.select("execution_id", "s3_bucket_name", "bucket_prefix", "key","extension", "size", "last_modified_time","partition_key","edp_run_id","period")

        df_dataset_inventory_filtered=df_dataset_inventory_filtered.withColumn("period",F.col("period").cast(StringType()))

        # Count missing partitions before loading
        missing_partitions_count = df_dataset_inventory_filtered.select("edp_run_id", "period").distinct().count()
        total_missing_partitions += missing_partitions_count

        df_dataset_inventory_filtered.write \
        .mode("append") \
        .format("delta") \
        .saveAsTable(s3_inventory_table)

        print(f"Processing dataset completed for: {df['bucket_prefix']}")
        print(f"New partitions loaded for this dataset: {missing_partitions_count}")
        prefixes.append(df['bucket_prefix'])

    except Exception as e:
        print(f"Error Processing dataset for {df['bucket_prefix']}")
        import traceback
        traceback.print_exc()

if not prefixes:
    print("No new records for any of the datasets")
else:
    print(f"Inventory table processing completed for : {prefixes}")
    print(f"\n{'='*80}")
    print(f"TOTAL NEW PARTITIONS LOADED TO INVENTORY: {total_missing_partitions}")
    print(f"{'='*80}")

# COMMAND ----------

# DBTITLE 1,Process Candidate Table by Prefix
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import math

# Initialize Delta table for candidate operations
candidate_table_delta = DeltaTable.forName(spark, candidate_table)

for prefix in prefixes:
    print(f"Processing candidate table for prefix: {prefix}")

    try:
        # Read inventory and filter by prefix with load_status IS NULL
        df = spark.sql(f"""
            SELECT inv.s3_bucket_name, inv.bucket_prefix, map.volume_location, inv.extension, inv.size
            FROM {s3_inventory_table} inv
            JOIN {dataset_mapping_tbl} map
            ON inv.s3_bucket_name = map.s3_bucket_name
            AND inv.bucket_prefix = map.bucket_prefix
            WHERE inv.s3_bucket_name = '{s3_bucket_name}' AND inv.bucket_prefix = '{prefix}' AND inv.load_status IS NULL
        """)

        # Skip if no unprocessed records
        if df.count() == 0:
            print(f"No records with null load_status for {prefix}")
            continue

        # Filter for structured files
        #STRUCTURED_EXTS = [".parquet"]
        #structured_list = [e.lstrip('.').lower() for e in STRUCTURED_EXTS]
        df_structured = df.filter(col("extension").isNotNull())
        display(df_structured)
        # Extract s3_bucket_name and table name
        s3_bucket_name = df.select("s3_bucket_name").first()[0]
        volume_location = df.select("volume_location").first()[0]
        table_name = os.path.basename(os.path.normpath(prefix))

        # Validate bucket and prefix
        if not s3_bucket_name or not prefix:
            print(f"ERROR: Invalid bucket or prefix")
            continue

        # Calculate metrics from inventory table (fresh counts)
        total_file_count = df.count()
        structured_file_count = df_structured.count()
        total_size_bytes = df.agg(sum("size")).collect()[0][0] or 0
        table_file_size_mb = total_size_bytes / (1024 * 1024)

        # Handle edge cases
        if math.isnan(table_file_size_mb) or math.isinf(table_file_size_mb):
            table_file_size_mb = 0.0
        table_file_size_mb = round(table_file_size_mb, 2)

        print(f"Metrics calculated:")
        print(f"    Total files: {total_file_count}")
        print(f"    Structured files: {structured_file_count}")
        print(f"    Total size: {table_file_size_mb:.2f} MB")

        # Check if record exists
        existing_candidate = spark.table(candidate_table).filter(
            (col("s3_bucket_name") == s3_bucket_name) &
            (col("bucket_prefix") == prefix) &
            (col("managed_table_created").isNull())
        )

        existing_count = existing_candidate.count()

        if existing_count > 0:
            # UPDATE existing record
            print(f"Updating existing candidate record for {prefix}")

            # Create source DataFrame with explicit schema
            source_schema = StructType([
                StructField("s3_bucket_name", StringType(), False),
                StructField("bucket_prefix", StringType(), False),
                StructField("new_total_file_count", LongType(), False),
                StructField("new_table_file_size_mb", DoubleType(), False),
                StructField("new_structured_file_count", LongType(), False)
            ])

            source_data = [(
                s3_bucket_name,
                prefix,
                int(total_file_count),
                float(table_file_size_mb),
                int(structured_file_count)
            )]

            source_df = spark.createDataFrame(source_data, schema=source_schema)

            candidate_table_delta.alias("t").merge(
                source_df.alias("s"),
                """t.s3_bucket_name = s.s3_bucket_name
                    AND t.bucket_prefix = s.bucket_prefix
                    AND t.managed_table_created IS NULL"""
            ).whenMatchedUpdate(
                set={
                    "total_file_count": "s.new_total_file_count",
                    "table_file_size_mb": "s.new_table_file_size_mb",
                    "structured_file_count": "s.new_structured_file_count"
                }
            ).execute()

            print(f"Successfully updated candidate record for {prefix}")

        else:
            # LOGIC 2: INSERT new record
            print(f"Inserting new candidate record for {prefix}")

            # Get the exact schema from the candidate table
            candidate_table_df = spark.table(candidate_table)
            candidate_schema = candidate_table_df.schema

            # Create a row with all columns from the schema
            row_data = {}
            for field in candidate_schema.fields:
                if field.name == "execution_id":
                    row_data[field.name] = execution_id
                elif field.name == "s3_bucket_name":
                    row_data[field.name] = s3_bucket_name
                elif field.name == "bucket_prefix":
                    row_data[field.name] = prefix
                elif field.name == "volume_location":
                    row_data[field.name] = volume_location
                elif field.name == "table_name":
                    row_data[field.name] = table_name
                elif field.name == "total_file_count":
                    row_data[field.name] = int(total_file_count)
                elif field.name == "table_file_size_mb":
                    row_data[field.name] = float(table_file_size_mb)
                elif field.name == "structured_file_count":
                    row_data[field.name] = int(structured_file_count)
                elif field.name == "candidate_for_managed_table_creation":
                    row_data[field.name] = None
                else:
                    # Set default value for any other columns
                    row_data[field.name] = None

            # Create DataFrame with exact schema
            new_candidate_df = spark.createDataFrame([row_data], schema=candidate_schema)

            new_candidate_df.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(candidate_table)

            print(f"Successfully inserted candidate record for {prefix}")

        print(f"Completed candidate processing for {prefix}\n")

    except Exception as e:
        print(f"ERROR processing candidate for {prefix}:")
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        continue

print("Candidate table processing completed!")

# COMMAND ----------

# DBTITLE 1,Archive Dataset Mapping File
try:    
    if file_exists_in_volume(f"{dataset_mapping_file_location}/{dataset_mapping_file}"):
        archive_file_name=dataset_mapping_file.replace(".csv",f"_{execution_id}.csv")
        source_path=f"{dataset_mapping_file_location}/{dataset_mapping_file}"
        archive_path=f"{archive_dataset_mapping_file_location}/{archive_file_name}"
        dbutils.fs.mv(source_path,archive_path)
except Exception as e:
    print (f"{e}")