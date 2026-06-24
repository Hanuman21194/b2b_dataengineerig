import sys
from pathlib import Path
from pyspark.sql.functions import col, lit, expr, when, to_json, struct, regexp_extract, to_timestamp
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DB_CONFIG
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

def get_col(df, options):
    """Helper to dynamically resolve column names from Bronze regardless of casing or schema drift."""
    df_cols_lower = [c.lower() for c in df.columns]
    for opt in options:
        if opt.lower() in df_cols_lower:
            return next(c for c in df.columns if c.lower() == opt.lower())
    return options[0]

def run_silver_layer(spark, cursor, batch_id):
    active_job_run_id = log_job_run_start(cursor, batch_id, 'silver_cleansing_layer')
    update_pipeline_state(cursor, 'daily_medallion_load', 'silver_cleansing_layer', batch_id, 'RUNNING')
    print("Cleansing data for Silver Layer & Routing to DLQ...")

    try:
        df_bronze_orders = spark.read.jdbc(JDBC_URL, "bronze.b2b_orders", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_order_items = spark.read.jdbc(JDBC_URL, "bronze.b2b_order_items", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_weblogs = spark.read.jdbc(JDBC_URL, "bronze.weblogs", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_companies = spark.read.jdbc(JDBC_URL, "bronze.b2b_companies", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_customers = spark.read.jdbc(JDBC_URL, "bronze.b2b_end_customers", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_products = spark.read.jdbc(JDBC_URL, "bronze.b2b_products", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)
        df_bronze_leads = spark.read.jdbc(JDBC_URL, "bronze.marketing_leads", properties=JDBC_PROPS).filter(col("batch_id") == batch_id)

        c_id = get_col(df_bronze_companies, ["source_company_id", "company_id"])
        cu_id = get_col(df_bronze_customers, ["source_customer_id", "customer_id"])
        p_id = get_col(df_bronze_products, ["source_product_id", "product_id"])
        s_id = get_col(df_bronze_products, ["source_supplier_id", "supplier_id"])
        wl_id = get_col(df_bronze_weblogs, ["source_log_id", "log_id"])
        
        o_o_id = get_col(df_bronze_orders, ["source_order_id", "order_id"])
        o_c_id = get_col(df_bronze_orders, ["source_company_id", "company_id"])
        o_cu_id = get_col(df_bronze_orders, ["source_customer_id", "customer_id"])
        
        i_oi_id = get_col(df_bronze_order_items, ["source_order_item_id", "order_item_id"])
        i_o_id = get_col(df_bronze_order_items, ["source_order_id", "order_id"])
        i_p_id = get_col(df_bronze_order_items, ["source_product_id", "product_id"])
        df_silver_companies = df_bronze_companies.selectExpr(
            f"{c_id} as source_company_id", "cuit", "company_name", "is_supplier", "batch_id"
        )
        
        df_silver_customers = df_bronze_customers.selectExpr(
            f"{cu_id} as source_customer_id", "document_number", "full_name", "date_of_birth", 
            "floor(months_between(current_date(), date_of_birth) / 12) as age", "batch_id"
        )
        
        df_silver_products = df_bronze_products.selectExpr(
            f"{p_id} as source_product_id", f"{s_id} as source_supplier_id", 
            "product_name", "default_price", "batch_id"
        )

        df_sales_joined = df_bronze_order_items.alias("i").join(
            df_bronze_orders.alias("o"), col(f"i.{i_o_id}") == col(f"o.{o_o_id}"), "inner"
        ).selectExpr(
            f"i.{i_oi_id} as source_order_item_id", 
            f"o.{o_o_id} as source_order_id",
            f"o.{o_c_id} as source_company_id", 
            f"o.{o_cu_id} as source_customer_id",
            f"i.{i_p_id} as source_product_id", 
            "o.order_timestamp", 
            "i.quantity", 
            "i.unit_price", 
            "(i.quantity * i.unit_price) as total_amount", 
            "o.batch_id"
        )

        df_bad_sales = df_sales_joined.filter("quantity < 0 OR unit_price < 0")
        df_silver_sales = df_sales_joined.filter("quantity >= 0 AND unit_price >= 0")

        df_weblogs_transformed = df_bronze_weblogs.selectExpr(
            f"{wl_id} as source_log_id", "client_ip", "username", "request_time", "user_agent", "batch_id"
        ).withColumn(
            "mapped_customer_id", 
            when(col("username").startswith("cust"), regexp_extract("username", r"cust(\d+)", 1).cast("int"))
            .otherwise(None)
        ).withColumn(
            "device_type",
            when(col("user_agent").ilike("%Mobile%"), "Mobile")
            .when(col("user_agent").ilike("%iPad%"), "Tablet")
            .otherwise("Desktop")
        ).withColumn(
            "country", expr("array('United States', 'India', 'Germany', 'Brazil', 'Japan', 'UK')[abs(hash(client_ip)) % 6]")
        ).withColumn(
            "city", expr("array('New York', 'Mumbai', 'Berlin', 'São Paulo', 'Tokyo', 'London')[abs(hash(client_ip)) % 6]")
        )

        df_bad_logs = df_weblogs_transformed.filter("client_ip = 'bad_ip_address'")
        df_silver_logs = df_weblogs_transformed.filter("client_ip != 'bad_ip_address'")

        ls_col = get_col(df_bronze_leads, ["lead_score", "lead_score_raw"])
        aa_col = get_col(df_bronze_leads, ["acquired_at", "acquired_at_raw"])
        
        df_leads_transformed = df_bronze_leads.withColumn(
            "lead_score", col(ls_col).cast("int")
        ).withColumn(
            "acquired_at", to_timestamp(col(aa_col), "yyyy-MM-dd HH:mm:ss")
        )
        
        if ls_col != "lead_score":
            df_leads_transformed = df_leads_transformed.drop(ls_col)
        if aa_col != "acquired_at":
            df_leads_transformed = df_leads_transformed.drop(aa_col)

        df_bad_leads = df_leads_transformed.filter(
            "lead_score < 0 OR email NOT LIKE '%@%' OR company_name IS NULL OR trim(company_name) = ''"
        )
        df_silver_leads = df_leads_transformed.filter(
            "lead_score >= 0 AND email LIKE '%@%' AND company_name IS NOT NULL AND trim(company_name) != ''"
        )

        # Write Clean Records
        df_silver_companies.write.jdbc(JDBC_URL, "silver.companies", mode="overwrite", properties=JDBC_PROPS)
        df_silver_customers.write.jdbc(JDBC_URL, "silver.customers", mode="overwrite", properties=JDBC_PROPS)
        df_silver_products.write.jdbc(JDBC_URL, "silver.products", mode="overwrite", properties=JDBC_PROPS)
        df_silver_sales.write.jdbc(JDBC_URL, "silver.sales_order_lines", mode="overwrite", properties=JDBC_PROPS)
        df_silver_logs.write.jdbc(JDBC_URL, "silver.web_activity", mode="overwrite", properties=JDBC_PROPS)
        df_silver_leads.write.jdbc(JDBC_URL, "silver.marketing_leads", mode="overwrite", properties=JDBC_PROPS)
        
        quarantine_sales_count = df_bad_sales.count()
        quarantine_logs_count = df_bad_logs.count()
        quarantine_leads_count = df_bad_leads.count()
        quarantine_count = quarantine_sales_count + quarantine_logs_count + quarantine_leads_count
        print(f"Caught {quarantine_count} dirty records! Routing to Quarantine...")
        
        df_bad_sales_json = df_bad_sales.select(
            lit(batch_id).alias("batch_id"), lit("daily_medallion_load").alias("job_name"),
            lit("B2B Sales").alias("source_name"), lit("Negative Value").alias("error_reason"), 
            to_json(struct("*")).alias("raw_record")
        )
        df_bad_logs_json = df_bad_logs.select(
            lit(batch_id).alias("batch_id"), lit("daily_medallion_load").alias("job_name"),
            lit("Weblogs").alias("source_name"), lit("Invalid IP").alias("error_reason"), 
            to_json(struct("*")).alias("raw_record")
        )
        df_bad_leads_json = df_bad_leads.select(
            lit(batch_id).alias("batch_id"), lit("daily_medallion_load").alias("job_name"),
            lit("Marketing Leads").alias("source_name"), lit("Invalid Email/Score/Company").alias("error_reason"), 
            to_json(struct("*")).alias("raw_record")
        )
        
        df_quarantine_union = df_bad_sales_json.union(df_bad_logs_json).union(df_bad_leads_json)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit.quarantine_records_staging (
                batch_id BIGINT, job_name VARCHAR, source_name VARCHAR, error_reason VARCHAR, raw_record TEXT
            );
        """)
        cursor.execute("TRUNCATE TABLE audit.quarantine_records_staging;")
        df_quarantine_union.write.jdbc(JDBC_URL, "audit.quarantine_records_staging", mode="append", properties=JDBC_PROPS)
        cursor.execute("""
            INSERT INTO audit.quarantine_records (batch_id, job_name, source_name, error_reason, raw_record)
            SELECT batch_id, job_name, source_name, error_reason, raw_record::jsonb
            FROM audit.quarantine_records_staging;
        """)
        cursor.execute("TRUNCATE TABLE audit.quarantine_records_staging;")

       
        log_job_run_success(cursor, active_job_run_id)
        update_pipeline_state(cursor, 'daily_medallion_load', 'silver_cleansing_layer', batch_id, 'SUCCESS')
    except Exception as e:
        log_job_run_failure(cursor, active_job_run_id, str(e))
        update_pipeline_state(cursor, 'daily_medallion_load', 'silver_cleansing_layer', batch_id, 'FAILED')
        raise e