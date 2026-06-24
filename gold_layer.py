import sys
from pathlib import Path
from pyspark.sql.functions import col, lit, expr, when, to_json, struct, regexp_extract, to_timestamp

# Ensure root paths are accessible for config imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DB_CONFIG, MARKETING_LEADS_FILE
from audit_trails import log_job_run_start, log_job_run_success, log_job_run_failure, update_pipeline_state

JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
    "batchsize": "25000",
    "rewriteBatchedInserts": "true"
}

def run_gold_layer(cursor, batch_id):
    active_job_run_id = log_job_run_start(cursor, batch_id, 'gold_star_schema_layer')
    update_pipeline_state(cursor, 'daily_medallion_load', 'gold_star_schema_layer', batch_id, 'RUNNING')
    print(" Merging Silver Data into Gold Star Schema (Idempotent Upserts)...")
    
    try:
        gold_merge_queries = [
            ("gold.dim_company", """
                INSERT INTO gold.dim_company (source_company_id, cuit, company_name, is_supplier)
                SELECT source_company_id, cuit, company_name, is_supplier FROM silver.companies
                ON CONFLICT (source_company_id) DO UPDATE 
                SET cuit=EXCLUDED.cuit, company_name=EXCLUDED.company_name, is_supplier=EXCLUDED.is_supplier;
            """),
            ("gold.dim_customer", """
                INSERT INTO gold.dim_customer (source_customer_id, document_number, full_name, date_of_birth, age)
                SELECT source_customer_id, document_number, full_name, date_of_birth, age FROM silver.customers
                ON CONFLICT (source_customer_id) DO UPDATE 
                SET document_number=EXCLUDED.document_number, full_name=EXCLUDED.full_name, age=EXCLUDED.age;
            """),
            ("gold.dim_product", """
                INSERT INTO gold.dim_product (source_product_id, source_supplier_id, product_name, default_price)
                SELECT source_product_id, source_supplier_id, product_name, default_price FROM silver.products
                ON CONFLICT (source_product_id) DO UPDATE 
                SET product_name=EXCLUDED.product_name, default_price=EXCLUDED.default_price;
            """),
            ("gold.dim_device", """
                INSERT INTO gold.dim_device (device_type)
                SELECT DISTINCT device_type FROM silver.web_activity
                ON CONFLICT (device_type) DO NOTHING;
            """),
            ("gold.dim_geo", """
                INSERT INTO gold.dim_geo (ip_address, country, city)
                SELECT DISTINCT client_ip, country, city FROM silver.web_activity WHERE client_ip IS NOT NULL
                ON CONFLICT (ip_address) DO UPDATE SET country=EXCLUDED.country, city=EXCLUDED.city;
            """),
            ("gold.dim_date", """
                INSERT INTO gold.dim_date (date_key, full_date, year, quarter, month, month_name, day)
                SELECT TO_CHAR(d, 'YYYYMMDD')::INT, d::DATE, EXTRACT(YEAR FROM d)::INT, EXTRACT(QUARTER FROM d)::INT,
                       EXTRACT(MONTH FROM d)::INT, TRIM(TO_CHAR(d, 'Month')), EXTRACT(DAY FROM d)::INT
                FROM generate_series(CURRENT_DATE - INTERVAL '2 years', CURRENT_DATE + INTERVAL '1 year', '1 day'::interval) d
                ON CONFLICT (date_key) DO NOTHING;
            """),
            ("gold.fact_sales", """
                INSERT INTO gold.fact_sales (source_order_item_id, source_order_id, date_key, company_key, customer_key, product_key, quantity, unit_price, total_amount, etl_batch_id)
                SELECT sol.source_order_item_id, sol.source_order_id, TO_CHAR(sol.order_timestamp, 'YYYYMMDD')::INT,
                       c.company_key, cust.customer_key, p.product_key, sol.quantity, sol.unit_price, sol.total_amount, sol.batch_id
                FROM silver.sales_order_lines sol
                JOIN gold.dim_company c ON sol.source_company_id = c.source_company_id
                JOIN gold.dim_customer cust ON sol.source_customer_id = cust.source_customer_id
                JOIN gold.dim_product p ON sol.source_product_id = p.source_product_id
                ON CONFLICT (source_order_item_id) DO UPDATE 
                SET quantity=EXCLUDED.quantity, total_amount=EXCLUDED.total_amount, etl_batch_id=EXCLUDED.etl_batch_id, updated_at=CURRENT_TIMESTAMP;
            """),
            ("gold.fact_web_activity", """
                INSERT INTO gold.fact_web_activity (source_log_id, date_key, geo_key, device_key, customer_key, request_time, etl_batch_id)
                SELECT wa.source_log_id, TO_CHAR(wa.request_time, 'YYYYMMDD')::INT, g.geo_key, d.device_key, cust.customer_key, wa.request_time, wa.batch_id
                FROM silver.web_activity wa
                JOIN gold.dim_geo g ON wa.client_ip = g.ip_address
                JOIN gold.dim_device d ON wa.device_type = d.device_type
                LEFT JOIN gold.dim_customer cust ON wa.mapped_customer_id = cust.source_customer_id
                ON CONFLICT (source_log_id) DO NOTHING;
            """),
            ("gold.fact_marketing_lead", f"""
                INSERT INTO gold.fact_marketing_lead (source_lead_id, date_key, company_name, lead_score, etl_batch_id)
                SELECT ml.lead_id, TO_CHAR(ml.acquired_at, 'YYYYMMDD')::INT, ml.company_name, ml.lead_score, ml.batch_id
                FROM silver.marketing_leads ml
                WHERE ml.batch_id = {batch_id}
                ON CONFLICT (source_lead_id) DO UPDATE 
                SET date_key = EXCLUDED.date_key,
                    company_name = EXCLUDED.company_name,
                    lead_score = EXCLUDED.lead_score,
                    etl_batch_id = EXCLUDED.etl_batch_id;
            """)
        ]
        
        for table_name, qry in gold_merge_queries:
            cursor.execute(qry)
            affected_rows = max(0, cursor.rowcount)
            # log_metric(cursor, batch_id, active_job_run_id, table_name, rows_inserted=affected_rows)

        log_job_run_success(cursor, active_job_run_id)
        update_pipeline_state(cursor, 'daily_medallion_load', 'gold_star_schema_layer', batch_id, 'SUCCESS')
    except Exception as e:
        log_job_run_failure(cursor, active_job_run_id, str(e))
        update_pipeline_state(cursor, 'daily_medallion_load', 'gold_star_schema_layer', batch_id, 'FAILED')
        raise e