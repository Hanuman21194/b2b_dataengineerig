import sys
import os
import csv
import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2

# Ensure root paths are accessible for config imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import DB_CONFIG, MARKETING_LEADS_FILE

def generate_mock_data():
    """Generates mock transactional data in PostgreSQL and writes the marketing leads CSV."""
    print("🧬 Generating Mock Source Data...")
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        # 1. Clear existing source data
        print("   🧹 Truncating old source data...")
        cursor.execute("""
            TRUNCATE TABLE 
                source.order_items, 
                source.orders, 
                source.weblogs, 
                source.products, 
                source.end_customers, 
                source.companies 
            CASCADE;
        """)

        # 2. Insert Companies
        print("   🏢 Inserting Companies...")
        for i in range(1, 11):
            cursor.execute("""
                INSERT INTO source.companies (cuit, company_name, is_supplier)
                VALUES (%s, %s, %s) RETURNING company_id;
            """, (f"30-000000{i}-9", f"Tech Corp {i}", i % 3 == 0))
            
        # 3. Insert Customers
        print("   🧑‍🤝‍🧑 Inserting Customers...")
        for i in range(1, 51):
            dob = datetime.now() - timedelta(days=random.randint(7000, 20000))
            cursor.execute("""
                INSERT INTO source.end_customers (document_number, full_name, date_of_birth)
                VALUES (%s, %s, %s);
            """, (f"DOC-{1000+i}", f"User {i}", dob.date()))

        # 4. Insert Products
        print("   📦 Inserting Products...")
        for i in range(1, 21):
            cursor.execute("""
                INSERT INTO source.products (supplier_id, product_name, default_price)
                VALUES ((SELECT company_id FROM source.companies WHERE is_supplier = TRUE ORDER BY RANDOM() LIMIT 1), %s, %s);
            """, (f"Product {i}", round(random.uniform(10.0, 500.0), 2)))

        # 5. Insert Orders & Order Items
        print("   🛒 Inserting Orders & Items...")
        for i in range(1, 101):
            order_time = datetime.now() - timedelta(days=random.randint(0, 365))
            cursor.execute("""
                INSERT INTO source.orders (company_id, customer_id, order_timestamp)
                VALUES (
                    (SELECT company_id FROM source.companies ORDER BY RANDOM() LIMIT 1),
                    (SELECT customer_id FROM source.end_customers ORDER BY RANDOM() LIMIT 1),
                    %s
                ) RETURNING order_id;
            """, (order_time,))
            order_id = cursor.fetchone()[0]

            for _ in range(random.randint(1, 5)):
                cursor.execute("""
                    INSERT INTO source.order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, (SELECT product_id FROM source.products ORDER BY RANDOM() LIMIT 1), %s, %s);
                """, (order_id, random.randint(1, 10), round(random.uniform(10.0, 500.0), 2)))

        # 6. Insert Weblogs
        print("   🌐 Inserting Weblogs...")
        for i in range(1, 301):
            req_time = datetime.now() - timedelta(hours=random.randint(1, 720))
            cursor.execute("""
                INSERT INTO source.weblogs (client_ip, username, request_time, user_agent)
                VALUES (%s, %s, %s, %s);
            """, (
                f"192.168.1.{random.randint(1, 255)}", 
                f"cust{random.randint(1, 50)}", 
                req_time, 
                random.choice(["Mozilla/5.0 (Windows NT 10.0)", "Mozilla/5.0 (iPhone; CPU OS 14_0)", "Mozilla/5.0 (iPad)"])
            ))

        # 7. Generate CSV Leads File
        print("   📄 Generating Marketing Leads CSV...")
        os.makedirs(MARKETING_LEADS_FILE.parent, exist_ok=True)
        with open(MARKETING_LEADS_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["lead_id", "full_name", "email", "company_name", "lead_score", "acquired_at"])
            for i in range(1, 51):
                writer.writerow([
                    str(uuid.uuid4()),
                    f"Lead Name {i}",
                    f"lead{i}@example.com" if i % 10 != 0 else "invalid_email", # Include some dirty data
                    f"Target Company {i}",
                    random.randint(-10, 100), # Include some negative scores (dirty data)
                    (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d %H:%M:%S")
                ])

        print("✅ Mock Data Generation Complete!")

    except Exception as e:
        print(f"❌ Data Generation Failed: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    generate_mock_data()