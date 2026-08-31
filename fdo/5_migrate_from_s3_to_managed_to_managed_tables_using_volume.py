# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import (
    col,
    regexp_extract,
    collect_list,
    lit,
    to_date,
    last_day,
    date_format,
    current_timestamp,
    when,
    current_date
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    DecimalType,
    DateType,
    TimestampType,
    BinaryType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType
)
from delta.tables import DeltaTable
import json
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
import time
import traceback

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema", "", "Schema")
dbutils.widgets.text("bucket_name", "", "Bucket Name")
dbutils.widgets.text("candidate_table", "", "Candidate Table")
dbutils.widgets.text("parquet_schema_table", "", "Parquet Schema Table")
dbutils.widgets.text("s3_inventory_table", "", "S3 Inventory Table")
dbutils.widgets.text("dataset_mapping_table", "", "Dataset Mapping Table")
dbutils.widgets.text("partition_table", "", "Partition Table")
dbutils.widgets.text("file_counts", "", "File Counts")
dbutils.widgets.text("migration_status_table", "", "Migration Status Table")
dbutils.widgets.text("source_partition_record_count_table", "", "Source Partition Record Count Table")
dbutils.widgets.text("target_partition_record_count_table", "", "Target Partition Record Count Table")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
bucket_name = dbutils.widgets.get("bucket_name")
candidate_table = dbutils.widgets.get("candidate_table")
parquet_schema_table = dbutils.widgets.get("parquet_schema_table")
s3_inventory_table = dbutils.widgets.get("s3_inventory_table")
dataset_mapping_table = dbutils.widgets.get("dataset_mapping_table")
partition_table = dbutils.widgets.get("partition_table")
file_counts = dbutils.widgets.get("file_counts")
migration_status_table = dbutils.widgets.get("migration_status_table")
source_partition_record_count_table = dbutils.widgets.get("source_partition_record_count_table")
target_partition_record_count_table = dbutils.widgets.get("target_partition_record_count_table")

# COMMAND ----------

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "false")
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

# COMMAND ----------

# Function to convert type strings to Spark types
def parse_dtype(dtype):
  dtype = dtype.lower().strip()

  if dtype.startswith("decimal"):
    scale = dtype[dtype.find("(") + 1 : dtype.find(")")].split(",")
    return DecimalType(int(scale[0]), int(scale[1]))

  if dtype in ("int8", "byte"):
    return ByteType()

  if dtype.startswith("bytetype"):
    return ByteType()

  if dtype in ("int16", "smallint"):
    return ShortType()

  if dtype.startswith("shorttype"):
    return ShortType()

  if dtype in ("int32", "integer"):
    return IntegerType()

  if dtype.startswith("integertype"):
    return IntegerType()

  if dtype == "int64":
    return LongType()

  if dtype.startswith("longtype"):
    return LongType()

  if dtype.startswith("floattype"):
    return FloatType()

  if dtype.startswith("doubletype"):
    return DoubleType()

  if dtype in ("string"):
    return StringType()

  if dtype.startswith("stringtype"):
    return StringType()

  if dtype.startswith("date32") or dtype == "date":
      return DateType()

  if dtype.startswith("datetype"):
    return DateType()

  if dtype.startswith("timestamp"):
    # Handles "timestamp", "timestamp[ms]", "timestamp[us]" etc.
    return TimestampType()

  if dtype == "bool":
    return BooleanType()

  if dtype.startswith("booleantype"):
    return BooleanType()

  if dtype == "binary":
    return BinaryType()

  if dtype.startswith("binarytype"):
    return BinaryType()

  raise ValueError(f"Unsupported type: {dtype}")

# COMMAND ----------

def process_partition(partition_path, edp_run_id, period, bucket_name, bucket_prefix, dataset_name, volume_location, managed_table_name, source_partition_record_count_table_name, partition_key_combination,  max_retries=3):
  
  """
  Process a single partition: read parquet and write to Delta table with retry logic
  """

  # path = f"{s3_path}edp_run_id={edp_run_id}/snapshot_date={snapshot_date}"
  # path = partition_path

  try:
    print(f"Reading partition path: {partition_path}")

    partition_key_combination = ",".join(
        [c.strip().lower() for c in partition_key_combination.split(",")]
    )

    if partition_key_combination == "edp_run_id,load_date":
      filtered_df = (
          spark.read
              .option("basePath", volume_location)
              .parquet(partition_path)
      )
    else:
      filtered_df = (
          spark.read
              .option("basePath", volume_location)
              .parquet(partition_path)
      )

    filtered_df = filtered_df.toDF(*[c.lower() for c in filtered_df.columns])

    target_table_schema = spark.table(managed_table_name).schema

    target_schema_types = {f.name.lower(): f.dataType for f in target_table_schema.fields}

    for col_name in filtered_df.columns:
      if col_name in target_schema_types:
        filtered_df = filtered_df.withColumn(col_name, col(col_name).try_cast(target_schema_types[col_name]))

    filtered_df = (filtered_df
            .withColumn("__txn_id_long", lit(99999999).cast(LongType()))
            .withColumn("__START_AT", current_timestamp())
            .withColumn("__END_AT", lit(None).cast(TimestampType()))
    )

    if partition_key_combination == "edp_run_id":
      # edp_run_id format: 2024-02-18_123456
      # period_expr = regexp_extract(col("edp_run_id"), r"^(\d{4}-\d{2}-\d{2})", 1).cast("string")
      # period = edp_run_id[:10] if edp_run_id else None

      # If edp_run_id matches YYYY-MM-DD_xxxxxx, extract YYYY-MM-DD.
      # Otherwise, use the current date.
      period_expr = (
          when(
              col("edp_run_id").rlike(r"^\d{4}-\d{2}-\d{2}_\d+$"),
              regexp_extract(col("edp_run_id"), r"^(\d{4}-\d{2}-\d{2})", 1)
          )
          .otherwise(date_format(current_date(), "yyyy-MM-dd"))
          .cast("string")
      )

      if edp_run_id and re.match(r"^\d{4}-\d{2}-\d{2}_\d+$", edp_run_id):
        period = edp_run_id[:10]
      else:
        period = datetime.now().strftime("%Y-%m-%d")

      filtered_df = filtered_df.withColumn("snapshot_date", period_expr)

    elif partition_key_combination == "edp_run_id,load_date":
      filtered_df = (filtered_df
              .withColumn("snapshot_date", lit("9999-12-31").cast("string"))
              .withColumn("edp_run_id", lit(edp_run_id))
      )

      period = "9999-12-31"

    elif partition_key_combination == "edp_run_id,run_yr_mo,run_yr_mo_dt":
      # run_yr_mo_dt format: yyyymmdd
      filtered_df = filtered_df.withColumn(
          "snapshot_date",
          date_format(to_date(col("run_yr_mo_dt").cast("string"), "yyyyMMdd"), "yyyy-MM-dd").cast("string")
      )

    elif partition_key_combination in ["edp_run_id,run_yr_mo", "edp_run_id,run_yr_mo_nb"]:
      partition_cols = [c.strip() for c in partition_key_combination.split(",")]
      for c in partition_cols:
        if c in ['run_yr_mo', 'run_yr_mo_nb']:
          filtered_df = (filtered_df.withColumn
              ("snapshot_date", date_format(last_day(to_date(col(c).cast("string"), "yyyyMM")), "yyyy-MM-dd").cast("string"))
          )

    elif partition_key_combination == "run_yr_mo":
      #run_yr_mo format: YYYYMM
      # Create snapshot_date as the last day of the month (YYYY-MM-DD)
      filtered_df = (filtered_df
          .withColumn("snapshot_date", date_format(last_day(to_date(col("run_yr_mo").cast("string"), "yyyyMM")), "yyyy-MM-dd").cast("string"))
          .withColumn("edp_run_id", date_format(current_timestamp(), "yyyyMMddHHmmss"))
      )
      #edp_run_id = datetime.now().strftime("%Y%m%d%H%M%S")
      edp_run_id = None

    elif partition_key_combination == "edp_run_id,ren_morn":
      filtered_df = (
          filtered_df.withColumn("snapshot_date", col("REN_MORN").cast("string"))
              .drop("REN_MORN")
      )

    elif partition_key_combination == "edp_run_id,ren_eve":
      #filtered_df = filtered_df.withColumnRenamed("REN_EVE", "snapshot_date").cast("string")
      filtered_df = (
          filtered_df.withColumn("snapshot_date", col("REN_EVE").cast("string"))
              .drop("REN_EVE")
      )

    elif partition_key_combination in ["edp_run_id,snapshot_date", "edp_run_id,bus_data_dt"]:
      # snapshot_date already exists or bus_data_dt should be used as-is.
      pass

    # Write the dataframe to the Delta table (mergeSchema ensures evolution safety)
    try:
      (
          filtered_df.write
          .format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(managed_table_name)
      )

      print(f"Successfully processed for {partition_path}")

    except Exception as e:
      #error_message = str(e)
      #print(error_message)
      print(f"Failed to process {partition_path}")
      return (partition_path, bucket_name, bucket_prefix, edp_run_id, period, "failed")

    try:
      source_partition_count = filtered_df.count()

      source_partition_count_schema = StructType([
          StructField("dataset_name", StringType(), True),
          StructField("edp_run_id", StringType(), True),
          StructField("period", StringType(), True),
          StructField("source_partition_count", IntegerType(), True)
      ])

      source_partition_count_df = spark.createDataFrame(
          [(dataset_name, edp_run_id, period, int(source_partition_count))],
          source_partition_count_schema
      )

      if source_partition_count > 0:
        (
            source_partition_count_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(source_partition_record_count_table_name)
        )

    except Exception as e:
      print(f"Failed to write source partition count for " 
          f"{partition_path}: {e}"
      )

    return (partition_path, bucket_name, bucket_prefix, edp_run_id, period, "loaded")

  except Exception as e:
    #error_message = str(e)
    #print(error_message)
    print(f"Failed to process {partition_path}")
    return (partition_path, bucket_name, bucket_prefix, edp_run_id, period, "failed")

  #return (partition_path, bucket_name, bucket_prefix, edp_run_id, period, "failed")

# COMMAND ----------

df_table_candidates = spark.sql(f"""
    select distinct cand.execution_id, cand.s3_bucket_name, cand.bucket_prefix
    , cand.table_name, 'picked_up_for_migration' as migration_status, current_timestamp() as datetime from
        (
            select distinct execution_id, s3_bucket_name, bucket_prefix, table_name
            from  {catalog}.{schema}.{candidate_table}
            where s3_bucket_name = '{bucket_name}'
            --and candidate_for_managed_table_creation in ('true', 'false')
            and managed_table_created is null
            and structured_file_count between {file_counts}
        ) cand
        left join
            (
                select distinct execution_id, s3_bucket_name, bucket_prefix
                from {catalog}.{schema}.{migration_status_table}
                where migration_status = 'picked_up_for_migration'
            ) mig_status
        on cand.execution_id = mig_status.execution_id
        and cand.s3_bucket_name = mig_status.s3_bucket_name
        and cand.bucket_prefix = mig_status.bucket_prefix
        where mig_status.s3_bucket_name is null
        and mig_status.bucket_prefix is null
    """)

# Collect all rows into Python memory
rows = df_table_candidates.collect()

(df_table_candidates.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.{schema}.{migration_status_table}")
)

inventory_table_name = f"{catalog}.{schema}.{s3_inventory_table}"
#print(inventory_table_name)

inventory_table = DeltaTable.forName(spark, inventory_table_name)

for row in rows:
  execution_id = row["execution_id"]
  bucket_name = row["s3_bucket_name"]
  bucket_prefix = row["bucket_prefix"]
  dataset_name = row["table_name"]

  try:
    load_status_schema = StructType([
        StructField("partition_path", StringType(), True),
        StructField("bucket_name", StringType(), True),
        StructField("bucket_prefix", StringType(), True),
        StructField("edp_run_id", StringType(), True),
        StructField("period", StringType(), True),
        StructField("load_status", StringType(), True)
    ])

    load_status_df = spark.createDataFrame([], load_status_schema)

    partition_key_sql = spark.sql(f"""
                    select distinct partition_key from {inventory_table_name}
                    where s3_bucket_name = '{bucket_name}'
                    and bucket_prefix = '{bucket_prefix}'
                    and load_status is null
                    """)

    partition_key_combination = partition_key_sql.first()["partition_key"]
    print(partition_key_combination)
    #partition_key_combination = "edp_run_id"
    print(f"Processing {dataset_name}")

    inventory_df = spark.sql(f"""SELECT distinct partition_path, edp_run_id, period
                  FROM
                  (
                      select distinct left(key, LENGTH(key) - POSITION('/' IN REVERSE(key))) as partition_path, edp_run_id, period
                      from {catalog}.{schema}.{s3_inventory_table}
                      where 1 = 1
                      and lower(partition_key) = '{partition_key_combination}'
                      and s3_bucket_name = '{bucket_name}'
                      and bucket_prefix = '{bucket_prefix}'
                      and load_status is null
                      and key not like '%/_SUCCESS'
                  ) inventory
                  """)

    target_df = spark.sql(f"""select
                    concat(dbx_catalog, '.', dbx_managed_table_schema, '.', dataset_name) as managed_table_name,
                    volume_location
                    from {catalog}.{schema}.{dataset_mapping_table}
                    where 1 = 1
                    and s3_bucket_name = '{bucket_name}'
                    and bucket_prefix = '{bucket_prefix}'
                """)

    #display(target_df)

    target_row = target_df.first()

    if target_row:
      managed_table_name, volume_location = target_row["managed_table_name"], target_row["volume_location"]

    #Check if tracker has partitions for this dataset
    if inventory_df.count() == 0:
      print("No files found – No data to write.")

    else:
      print("Files identified – Starting the data load.")
      print(f"Total Partition to Load: {inventory_df.count()}")
      # Collect partition values as tuples
      partition_filters = [(row.partition_path, row.edp_run_id, row.period) for row in inventory_df.collect()]

      #print(f"Creating Table: {managed_table_name}")

      #spark.sql(f"""
      #    CREATE TABLE IF NOT EXISTS {managed_table_name} (
      #        edp_run_id string,
      #        snapshot_date date
      #    )
      #    USING DELTA
      #    TBLPROPERTIES (
      #        'delta.columnMapping.mode' = 'name',
      #        'delta.enableIcebergCompatV2' = 'true',
      #        'delta.universalFormat.enabledFormats' = 'iceberg'
      #    )
      #    cluster by (edp_run_id, snapshot_date)
      #""")

      # ======================================================
      # PARALLELIZED PARTITION PROCESSING WITH 6 THREADS
      # ======================================================

      source_partition_record_count_table_name = f"{catalog}.{schema}.{source_partition_record_count_table}"
      max_workers = 10  # no. of threads for parallel processing
      successful_partitions = []
      failed_partitions = []

      print(f"Starting parallel processing of {len(partition_filters)} partitions with {max_workers} threads")

      with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all partition processing tasks
        future_to_partition = {
            executor.submit(
                process_partition,
                partition_path,
                edp_run_id,
                period,
                bucket_name,
                bucket_prefix,
                dataset_name,
                volume_location,
                managed_table_name,
                source_partition_record_count_table_name,
                partition_key_combination
            ): (partition_path, edp_run_id, period)
            for partition_path, edp_run_id, period in partition_filters
        }

        # Process completed tasks as they finish
        completed = 0
        for future in as_completed(future_to_partition):
          partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status = future.result()

          if load_status == "loaded":
            successful_partitions.append((partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status))
            completed += 1
          else:
            failed_partitions.append((partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status))

          # Progress update every 50 partitions
          if completed % 50 == 0 or completed == len(partition_filters):
            print(f"Progress: {completed}/{len(partition_filters)} partitions processed")

      # Retry failed partitions if any
      if failed_partitions:
        print(f"\nRetrying {len(failed_partitions)} failed partitions...")
        retry_filters = [(partition_path, edp_run_id, period) for partition_path, _, _, edp_run_id, period, _ in failed_partitions]
        failed_partitions = []  # Reset

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
          future_to_partition = {
              executor.submit(
                  process_partition,
                  partition_path,
                  edp_run_id,
                  period,
                  bucket_name,
                  bucket_prefix,
                  dataset_name,
                  volume_location,
                  managed_table_name,
                  source_partition_record_count_table_name,
                  partition_key_combination
              ): (partition_path, edp_run_id, period)
              for partition_path, edp_run_id, period in retry_filters
          }

          for future in as_completed(future_to_partition):
            partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status = future.result()

            if load_status == "loaded":
              successful_partitions.append((partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status))
              completed += 1
            else:
              failed_partitions.append((partition_path, bucket_name, bucket_prefix, edp_run_id, period, load_status))
              completed += 1

            # Progress update every 50 partitions
            if completed % 50 == 0 or completed == len(partition_filters):
              print(f"Progress: {completed}/{len(partition_filters)} partitions processed")

      successful_partitions_df = spark.createDataFrame(successful_partitions, load_status_schema)
      failed_partitions_df = spark.createDataFrame(failed_partitions, load_status_schema)

      load_status_df = (
          load_status_df
              .union(successful_partitions_df)
              .union(failed_partitions_df)
              .drop("partition_path")
              .dropDuplicates()
      )
      #display(load_status_df)

      # Perform merge to update the target table
      if partition_key_combination in ["edp_run_id", "edp_run_id, load_date"]:
        inventory_table.alias("target").merge(
            load_status_df.alias("updates"),
            """
            target.s3_bucket_name = updates.bucket_name AND
            target.bucket_prefix = updates.bucket_prefix AND
            target.edp_run_id = updates.edp_run_id
            """
        ).whenMatchedUpdate(
            condition="target.load_status is null",
            set={"load_status": col("updates.load_status")}
        ).execute()
      elif (partition_key_combination == "run_yr_mo"):
        inventory_table.alias("target").merge(
            load_status_df.alias("updates"),
            """
            target.s3_bucket_name = updates.bucket_name AND
            target.bucket_prefix = updates.bucket_prefix AND
            target.period = updates.period
            """
        ).whenMatchedUpdate(
            condition="target.load_status is null",
            set={"load_status": col("updates.load_status")}
        ).execute()
      else:
        inventory_table.alias("target").merge(
            load_status_df.alias("updates"),
            """
            target.s3_bucket_name = updates.bucket_name AND
            target.bucket_prefix = updates.bucket_prefix AND
            target.edp_run_id = updates.edp_run_id AND
            target.period = updates.period
            """
        ).whenMatchedUpdate(
            condition="target.load_status is null",
            set={"load_status": col("updates.load_status")}
        ).execute()


      # Summary
      print("\n" + "="*80)
      print(f"Processing Complete for {dataset_name}")
      print(f"Total partitions: {len(partition_filters)}")
      print(f"Successful: {len(successful_partitions)}")
      print(f"Failed: {len(failed_partitions)}")
      print("="*80)

      partition_cols = [c.strip() for c in partition_key_combination.split(",")]

      if (partition_cols == ["edp_run_id"] or partition_cols == ["edp_run_id", "load_date"]):
        period_expr = "snapshot_date"
      elif partition_cols == ["run_yr_mo"]:
        period_expr = "run_yr_mo"
      elif "snapshot_date" in partition_cols:
        period_expr = "snapshot_date"
      elif "ren_morn" in partition_cols:
        period_expr = "snapshot_date"
      elif "ren_eve" in partition_cols:
        period_expr = "snapshot_date"
      elif "bus_data_dt" in partition_cols:
        period_expr = "bus_data_dt"
      elif "run_yr_mo_dt" in partition_cols:
        period_expr = "run_yr_mo_dt"
      elif "run_yr_mo" in partition_cols:
        period_expr = "run_yr_mo"
      elif "run_yr_mo_nb" in partition_cols:
        period_expr = "run_yr_mo_nb"
      else:
        raise ValueError(
            f"Could not determine period column from {partition_key_combination}"
        )

      spark.sql(f"""
              INSERT INTO {catalog}.{schema}.{target_partition_record_count_table}
              SELECT
              '{dataset_name}' as dataset_name,
              edp_run_id,
              {period_expr} as period,
              COUNT(*) AS target_partition_count
              FROM {managed_table_name}
              GROUP BY
              edp_run_id,
              {period_expr}
              """)

      spark.sql(f"""update {catalog}.{schema}.{candidate_table}
          set managed_table_created = 'true'
          where 1 = 1
          and managed_table_created is null
          and s3_bucket_name = '{bucket_name}'
          and bucket_prefix = '{bucket_prefix}'""")

      print(f"Migration Completed")

  except Exception as e:
    error_message = str(e)
    stack = traceback.format_exc()
    print(f"Failed to process {dataset_name}: {error_message}")
    spark.sql(f"""update {catalog}.{schema}.{candidate_table}
                set error_message = 'Failed to Process'
                where 1 = 1
                and managed_table_created is null
                and s3_bucket_name = '{bucket_name}'
                and bucket_prefix = '{bucket_prefix}'""")

    spark.sql(f"""delete from {catalog}.{schema}.{migration_status_table}
                where 1 = 1
                and execution_id = '{execution_id}'
                and s3_bucket_name = '{bucket_name}'
                and bucket_prefix = '{bucket_prefix}'""")
    continue  # move to next dataset