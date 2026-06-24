#!/usr/bin/env python3
import sys
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import DB_CONFIG

def get_pg_connection():
    return psycopg2.connect(**DB_CONFIG)

def run_kpi_calculations():
    print("                B2B METRICS & KPI CALCULATION SUITE")
    conn = get_pg_connection()
    conn.autocommit = True
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
       
        # KPI 1: Most Popular Used Devices (Top 5)
      
        cursor.execute("TRUNCATE TABLE gold.report_top_devices;")
        cursor.execute("""
            INSERT INTO gold.report_top_devices (rank_no, device_type, total_logins)
            SELECT 
                (ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, d.device_type DESC))::INT as rank_no,
                d.device_type,
                COUNT(*) as total_logins
            FROM gold.fact_web_activity f
            JOIN gold.dim_device d ON f.device_key = d.device_key
            GROUP BY d.device_type
            LIMIT 5;
        """)
        
      
        # KPI 2: Most Popular Products in the Country with the Most Logins
        
        cursor.execute("TRUNCATE TABLE gold.report_top_products_in_top_country;")
        cursor.execute("""
            WITH top_active_country AS (
                SELECT g.country, COUNT(*) as logins
                FROM gold.fact_web_activity f
                JOIN gold.dim_geo g ON f.geo_key = g.geo_key
                GROUP BY g.country
                ORDER BY logins DESC
                LIMIT 1
            ),
            customer_primary_country AS (
                SELECT DISTINCT ON (customer_key) 
                    customer_key, 
                    g.country
                FROM gold.fact_web_activity f
                JOIN gold.dim_geo g ON f.geo_key = g.geo_key
                WHERE customer_key IS NOT NULL
                GROUP BY customer_key, g.country
                ORDER BY customer_key, COUNT(*) DESC
            )
            INSERT INTO gold.report_top_products_in_top_country (rank_no, country, product_name, total_units_sold, total_revenue)
            SELECT 
                (ROW_NUMBER() OVER (ORDER BY SUM(f.quantity) DESC, p.product_name DESC))::INT as rank_no,
                tc.country,
                p.product_name,
                SUM(f.quantity)::BIGINT as total_units_sold,
                SUM(f.total_amount)::NUMERIC(14, 2) as total_revenue
            FROM gold.fact_sales f
            JOIN gold.dim_product p ON f.product_key = p.product_key
            JOIN customer_primary_country cc ON f.customer_key = cc.customer_key
            CROSS JOIN top_active_country tc
            WHERE cc.country = tc.country
            GROUP BY tc.country, p.product_name;
        """)
        
       
        # KPI 3: Monthly Sales of B2B Platform for Last Year
      
        cursor.execute("TRUNCATE TABLE gold.report_monthly_sales_last_year;")
        cursor.execute("""
            INSERT INTO gold.report_monthly_sales_last_year (month_start, year, month, month_name, total_orders, total_items_sold, monthly_revenue)
            SELECT 
                DATE_TRUNC('month', d.full_date)::DATE as month_start,
                d.year,
                d.month,
                d.month_name,
                COUNT(DISTINCT f.source_order_id)::BIGINT as total_orders,
                SUM(f.quantity)::BIGINT as total_items_sold,
                SUM(f.total_amount)::NUMERIC(14, 2) as monthly_revenue
            FROM gold.fact_sales f
            JOIN gold.dim_date d ON f.date_key = d.date_key
            WHERE d.full_date >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY d.year, d.month, d.month_name, DATE_TRUNC('month', d.full_date)::DATE
            ORDER BY month_start DESC;
        """)
        
        print(" All KPI reporting tables calculated and refreshed successfully")
    
     
        print("KPI 1: TOP 5 DEVICES BY CLIENT LOGINS")
     
        cursor.execute("SELECT rank_no, device_type, total_logins FROM gold.report_top_devices ORDER BY rank_no ASC;")
        for r in cursor.fetchall():
            print(f" Rank {r['rank_no']}: {r['device_type']:<15} | Total Logins: {r['total_logins']}")
            
        print("                     KPI 2: TOP PRODUCTS IN MOST ACTIVE COUNTRY")
     
        cursor.execute("SELECT rank_no, country, product_name, total_units_sold, total_revenue FROM gold.report_top_products_in_top_country LIMIT 5;")
        for r in cursor.fetchall():
            print(f" Rank {r['rank_no']}: {r['country']:<15} | {r['product_name']:<25} | Units: {r['total_units_sold']:<5} | Revenue: ${r['total_revenue']:,}")


        print("                     KPI 3: MONTHLY B2B PLATFORM SALES PERFORMANCE")
   
        cursor.execute("SELECT month_start, month_name, total_orders, total_items_sold, monthly_revenue FROM gold.report_monthly_sales_last_year ORDER BY month_start DESC LIMIT 5;")
        for r in cursor.fetchall():
            print(f" Month: {r['month_name']:<10} | Orders: {r['total_orders']:<5} | Items: {r['total_items_sold']:<5} | Monthly Revenue: ${r['monthly_revenue']:,}")


    except Exception as e:
        print(f" Calculation Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_kpi_calculations()