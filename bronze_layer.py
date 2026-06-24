import sys
import os
from pathlib import Path
from pyspark.sql.functions import col, lit, current_timestamp, monotonically_increasing_id, md5, concat_ws

# Ensure root paths are accessible for config imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DB_CONFIG, MARKETING_LEADS_FILE

from audit_trails import log_job_run_start, log_job_run_success, log_job_run_failure,  update_pipeline_state

JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
    "batchsize": "25000",
    "rewriteBatchedInserts": "true"
}

def align_schema(spark, df, target_table):
    """
    Dynamically aligns Spark DataFrame column names and datatypes to match the target 
    PostgreSQL table schemas exactly. This maps matching/aliased columns (e.g., company_id to 
    source_company_id) and casts them, bypassing strict validation.
    """
    try:
        # Fetch target schema with a fast LIMIT 0 query
        target_df = spark.read.jdbc(JDBC_URL, target_table, properties=JDBC_PROPS).limit(0)
        target_schema = target_df.schema
        src_cols = {c.lower(): c for c in df.columns}   
        aligned_cols = []
        for field in target_schema:
            target_col_name = field.name
            target_col_lower = target_col_name.lower()
            
            # Try exact match first
            matched_src_col = None
            if target_col_lower in src_cols:
                matched_src_col = src_cols[target_col_lower]
            else:
                for src_lower, src_orig in src_cols.items():
                    if src_lower in target_col_lower or target_col_lower in src_lower:
                        matched_src_col = src_orig
                        break
            
            if matched_src_col:
                aligned_cols.append(col(matched_src_col).cast(field.dataType).alias(target_col_name))
        
        if aligned_cols:
            df = df.select(*aligned_cols)
            
    except Exception as e:
        print(f"Warning: Dynamic schema alignment failed for {target_table}: {e}")
    return df


def run_bronze_layer(spark, cursor, batch_id):
    active_job_run_id = log_job_run_start(cursor, batch_id, 'bronze_ingestion_layer')
    update_pipeline_state(cursor, 'daily_medallion_load', 'bronze_ingestion_layer', batch_id, 'RUNNING')
    print("Extracting data to Bronze Layer...")
    
    try:
       
        print("Truncating Bronze tables to prepare for clean full load")
        cursor.execute("""
            TRUNCATE TABLE 
                bronze.b2b_companies, 
                bronze.b2b_end_customers, 
                bronze.b2b_products, 
                bronze.b2b_orders, 
                bronze.b2b_order_items, 
                bronze.weblogs, 
                bronze.marketing_leads
            CASCADE;
        """)

        # Partitioned reads on large transactional tables
        df_orders = spark.read.jdbc(
            url=JDBC_URL, table="source.orders", column="order_id",
            lowerBound=1, upperBound=100000, numPartitions=4, properties=JDBC_PROPS
        )
        df_order_items = spark.read.jdbc(
            url=JDBC_URL, table="source.order_items", column="order_item_id",
            lowerBound=1, upperBound=100000, numPartitions=4, properties=JDBC_PROPS
        )
        df_weblogs = spark.read.jdbc(
            url=JDBC_URL, table="source.weblogs", column="log_id",
            lowerBound=1, upperBound=100000, numPartitions=4, properties=JDBC_PROPS
        )
        
        # Lookup tables
        df_companies = spark.read.jdbc(JDBC_URL, "source.companies", properties=JDBC_PROPS)
        df_customers = spark.read.jdbc(JDBC_URL, "source.end_customers", properties=JDBC_PROPS)
        df_products = spark.read.jdbc(JDBC_URL, "source.products", properties=JDBC_PROPS)
        
        # Windows Path formatting for marketing leads
        leads_csv_path = MARKETING_LEADS_FILE.as_posix()
        if not leads_csv_path.startswith("file://"):
            leads_csv_path = f"file:///{leads_csv_path.lstrip('/')}"
        
        df_leads = spark.read.option("header", "true").csv(leads_csv_path)

        # FIX: Explicitly append the NOT NULL metadata columns expected by bronze.marketing_leads
        # Added md5(concat_ws(...)) to properly satisfy the row_hash requirement
        df_leads = df_leads.withColumn("source_file", lit("manual_upload")) \
                           .withColumn("source_line_number", monotonically_increasing_id()) \
                           .withColumn("row_hash", md5(concat_ws("||", *df_leads.columns))) \
                           .withColumn("ingested_at", current_timestamp()) \
                           .withColumn("is_processed", lit(False))

        # Append Batch ID
        df_orders = df_orders.withColumn("batch_id", lit(batch_id))
        df_order_items = df_order_items.withColumn("batch_id", lit(batch_id))
        df_companies = df_companies.withColumn("batch_id", lit(batch_id))
        df_customers = df_customers.withColumn("batch_id", lit(batch_id))
        df_products = df_products.withColumn("batch_id", lit(batch_id))
        df_weblogs = df_weblogs.withColumn("batch_id", lit(batch_id))
        df_leads_bronze = df_leads.withColumn("batch_id", lit(batch_id))

        df_companies = align_schema(spark, df_companies, "bronze.b2b_companies")
        df_customers = align_schema(spark, df_customers, "bronze.b2b_end_customers")
        df_products = align_schema(spark, df_products, "bronze.b2b_products")
        df_orders = align_schema(spark, df_orders, "bronze.b2b_orders")
        df_order_items = align_schema(spark, df_order_items, "bronze.b2b_order_items")
        df_weblogs = align_schema(spark, df_weblogs, "bronze.weblogs")
        df_leads_bronze = align_schema(spark, df_leads_bronze, "bronze.marketing_leads")

 
        df_companies.write.jdbc(JDBC_URL, "bronze.b2b_companies", mode="append", properties=JDBC_PROPS)
        df_customers.write.jdbc(JDBC_URL, "bronze.b2b_end_customers", mode="append", properties=JDBC_PROPS)
        df_products.write.jdbc(JDBC_URL, "bronze.b2b_products", mode="append", properties=JDBC_PROPS)
        df_orders.write.jdbc(JDBC_URL, "bronze.b2b_orders", mode="append", properties=JDBC_PROPS)
        df_order_items.write.jdbc(JDBC_URL, "bronze.b2b_order_items", mode="append", properties=JDBC_PROPS)
        df_weblogs.write.jdbc(JDBC_URL, "bronze.weblogs", mode="append", properties=JDBC_PROPS)
        df_leads_bronze.write.jdbc(JDBC_URL, "bronze.marketing_leads", mode="append", properties=JDBC_PROPS)

        log_job_run_success(cursor, active_job_run_id)
        update_pipeline_state(cursor, 'daily_medallion_load', 'bronze_ingestion_layer', batch_id, 'SUCCESS')
    except Exception as e:
        log_job_run_failure(cursor, active_job_run_id, str(e))
        update_pipeline_state(cursor, 'daily_medallion_load', 'bronze_ingestion_layer', batch_id, 'FAILED')
        raise e