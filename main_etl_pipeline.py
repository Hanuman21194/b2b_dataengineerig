#!/usr/bin/env python3
import sys
import os
import socketserver
import tempfile
from pathlib import Path
from pyspark.sql import SparkSession
sys.path.append(str(Path(__file__).resolve().parent.parent)) 
sys.path.append(str(Path(__file__).resolve().parent))

import db

from audit_trails import evaluate_restart_state
from bronze_layer import run_bronze_layer
from silver_layer import run_silver_layer
from gold_layer import run_gold_layer


def initialize_spark_session():
    """Initializes Spark environment with platform-specific hotfixes."""
    if sys.platform == "win32" and not hasattr(socketserver, "UnixStreamServer"):
        class _DummyUnixStreamServer:
            pass
        socketserver.UnixStreamServer = _DummyUnixStreamServer

    try:
        import pyspark
        pyspark_install_dir = os.path.dirname(pyspark.__file__)
        os.environ["SPARK_HOME"] = pyspark_install_dir
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        
        spark_conf_temp_dir = os.path.join(tempfile.gettempdir(), "spark-runtime-conf")
        os.makedirs(spark_conf_temp_dir, exist_ok=True)
        log4j_file_path = os.path.join(spark_conf_temp_dir, "log4j2.properties")
        with open(log4j_file_path, "w", encoding="utf-8") as f:
            f.write("rootLogger.level = OFF\n")
        os.environ["SPARK_CONF_DIR"] = spark_conf_temp_dir
    except ImportError:
        pass

    spark_temp_dir = os.path.join(tempfile.gettempdir(), "spark-pipeline-temp")
    spark_warehouse_dir = os.path.join(tempfile.gettempdir(), "spark-pipeline-warehouse")
    os.makedirs(spark_temp_dir, exist_ok=True)
    os.makedirs(spark_warehouse_dir, exist_ok=True)
    
    sanitized_warehouse_dir = spark_warehouse_dir.replace('\\', '/')
    clean_warehouse_path = f"file:///{sanitized_warehouse_dir}"

    spark = SparkSession.builder \
        .appName("B2B_Medallion_Architecture") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", clean_warehouse_path) \
        .config("spark.local.dir", spark_temp_dir) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "200") \
        .getOrCreate()
        
    return spark


def run_pipeline():
    """Main Orchestrator tying all pipeline functions together."""
    print("Initializing Medallion PySpark Pipeline...")
    spark = initialize_spark_session()

    conn = db.get_connection(autocommit=True)
    cursor = conn.cursor()

    try:
      
        batch_id, resume_from_layer = evaluate_restart_state(cursor)

        if resume_from_layer == 'bronze':
            run_bronze_layer(spark, cursor, batch_id)
            resume_from_layer = 'silver'
        else:
            print(" Skipping Bronze Ingestion Layer (Already successfully completed).")

        if resume_from_layer == 'silver':
            run_silver_layer(spark, cursor, batch_id)
            resume_from_layer = 'gold'
        else:
            print(" Skipping Silver Cleansing Layer (Already successfully completed).")

        if resume_from_layer == 'gold':
            run_gold_layer(cursor, batch_id)
        else:
            print("Skipping Gold Star Schema Layer (Already successfully completed).")

        # Phase 4: Audit Finalization
        cursor.execute("""
            UPDATE audit.etl_batches 
            SET status = 'SUCCESS', finished_at = CURRENT_TIMESTAMP 
            WHERE batch_id = %s;
        """, (batch_id,))
        
        print(f"Pipeline Completed Successfully! Batch ID {batch_id} marked as SUCCESS.")

    except BaseException as e:
        conn.autocommit = False
        conn.rollback()
        print(f" Pipeline Failed: {e}")
        if 'batch_id' in locals() and batch_id:
            try:
                cursor.execute("""
                    UPDATE audit.etl_batches 
                    SET status = 'FAILED', error_message = %s, finished_at = CURRENT_TIMESTAMP 
                    WHERE batch_id = %s;
                """, (str(e)[:255], batch_id))
                conn.commit()
            except Exception:
                pass
    finally:
        cursor.close()
        conn.close()
        spark.stop()


if __name__ == "__main__":
    run_pipeline()