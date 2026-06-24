#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

# Dynamically determine the project root directory
# If this script is inside the 'etl' folder, the root is its parent.
CURRENT_DIR = Path(__file__).resolve().parent
if CURRENT_DIR.name == 'etl':
    PROJECT_ROOT = CURRENT_DIR.parent
else:
    PROJECT_ROOT = CURRENT_DIR

def run_script(script_relative_path, description):
    """Executes a Python script using absolute paths and halts if it fails"""
    # Resolve the absolute path to the script
    script_path = PROJECT_ROOT / script_relative_path
    
    # Run the process
    result = subprocess.run([sys.executable, str(script_path)])
    
    if result.returncode != 0:
        print("Halting master execution.")
        sys.exit(1)
    
    print(f"\nStep '{description}' completed successfully.\n")

if __name__ == "__main__":
  
    print("This script will run your entire project from start to finish.")
    
    # 1. Setup the Database
    run_script("etl/setup_database.py", "Initialize PostgreSQL Schemas & Tables")
    
    # 2. Generate Source Data (Using your existing 3 scripts)
    run_script("data_generation/generate_b2b.py", "Generate Mock B2B Data (Orders, Companies, etc.)")
    run_script("data_generation/generate_weblogs.py", "Generate Mock Weblogs Data")
    run_script("data_generation/generate_csv.py", "Generate Mock CSV Data (Marketing Leads)")
    
    # 3. Run the ETL Pipeline
    run_script("etl/main_etl_pipeline.py", "Execute Medallion Pipeline (Bronze -> Silver -> Gold)")
    
    # 4. Calculate and Load KPIs
    run_script("etl/load_kpis.py", "Compute Reporting Dashboards & KPIs")
