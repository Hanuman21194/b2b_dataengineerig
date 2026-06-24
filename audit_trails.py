import psycopg2

def log_job_run_start(cursor, batch_id, job_name):
    cursor.execute("""
        INSERT INTO audit.etl_job_runs (batch_id, job_name, status, started_at)
        VALUES (%s, %s, 'RUNNING', CURRENT_TIMESTAMP)
        RETURNING job_run_id;
    """, (batch_id, job_name))
    return cursor.fetchone()[0]

def log_job_run_success(cursor, job_run_id):
    cursor.execute("""
        UPDATE audit.etl_job_runs 
        SET status = 'SUCCESS', finished_at = CURRENT_TIMESTAMP 
        WHERE job_run_id = %s;
    """, (job_run_id,))

def log_job_run_failure(cursor, job_run_id, error_msg):
    try:
        cursor.execute("""
            UPDATE audit.etl_job_runs 
            SET status = 'FAILED', finished_at = CURRENT_TIMESTAMP 
            WHERE job_run_id = %s;
        """, (job_run_id,))
    except Exception:
        pass

def update_pipeline_state(cursor, pipeline_name, layer_name, batch_id, status, watermark=None):
    cursor.execute("""
        INSERT INTO audit.pipeline_state (pipeline_name, layer_name, last_watermark, last_batch_id, status, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name, layer_name) 
        DO UPDATE SET 
            last_watermark = COALESCE(EXCLUDED.last_watermark, pipeline_state.last_watermark),
            last_batch_id = EXCLUDED.last_batch_id,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP;
    """, (pipeline_name, layer_name, watermark, batch_id, status))

def evaluate_restart_state(cursor):
    """Evaluates audit catalog to determine if the pipeline is resuming from a crash."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit.pipeline_state (
            pipeline_name VARCHAR(255),
            layer_name VARCHAR(50),
            last_watermark TIMESTAMP,
            last_batch_id BIGINT,
            status VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (pipeline_name, layer_name)
        );
    """)

    for l_name in ['bronze_ingestion_layer', 'silver_cleansing_layer', 'gold_star_schema_layer']:
        cursor.execute("""
            INSERT INTO audit.pipeline_state (pipeline_name, layer_name, last_watermark, last_batch_id, status)
            VALUES ('daily_medallion_load', %s, NULL, 0, 'PENDING')
            ON CONFLICT DO NOTHING;
        """, (l_name,))

    cursor.execute("""
        SELECT batch_id, status FROM audit.etl_batches 
        WHERE pipeline_name = 'daily_medallion_load' 
        ORDER BY batch_id DESC LIMIT 1;
    """)
    last_batch = cursor.fetchone()

    restart_batch_id = None
    resume_from_layer = 'bronze'

    if last_batch:
        last_batch_id = last_batch[0]
        last_status = last_batch[1]
        
        if last_status == 'FAILED':
            print(f"Detected previous failed batch ID: {last_batch_id}. Evaluating checkpoints...")
            restart_batch_id = last_batch_id
            
            cursor.execute("""
                SELECT layer_name, status FROM audit.pipeline_state 
                WHERE pipeline_name = 'daily_medallion_load' AND last_batch_id = %s;
            """, (last_batch_id,))
            layer_states = {row[0]: row[1] for row in cursor.fetchall()}
            
            if layer_states.get('gold_star_schema_layer') == 'SUCCESS':
                restart_batch_id = None
                resume_from_layer = 'bronze'
            elif layer_states.get('silver_cleansing_layer') == 'SUCCESS':
                resume_from_layer = 'gold'
            elif layer_states.get('bronze_ingestion_layer') == 'SUCCESS':
                resume_from_layer = 'silver'
            else:
                resume_from_layer = 'bronze'

    if restart_batch_id is not None:
        batch_id = restart_batch_id
        cursor.execute("""
            UPDATE audit.etl_batches 
            SET status = 'RUNNING', finished_at = NULL, error_message = NULL 
            WHERE batch_id = %s;
        """, (batch_id,))
        print(f" Resuming Batch ID: {batch_id} from {resume_from_layer} layer.")
    else:
        cursor.execute("""
            INSERT INTO audit.etl_batches (pipeline_name, status, started_at) 
            VALUES ('daily_medallion_load', 'RUNNING', CURRENT_TIMESTAMP) 
            RETURNING batch_id;
        """)
        batch_id = cursor.fetchone()[0]
        resume_from_layer = 'bronze'
        print(f"Started Fresh ETL Batch ID: {batch_id}")
        
    return batch_id, resume_from_layer