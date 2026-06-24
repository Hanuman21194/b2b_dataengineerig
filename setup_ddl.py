#!/usr/bin/env python3
from pathlib import Path
from db import get_connection


def execute_sql_file(cursor, file_path: Path):
    sql = file_path.read_text(encoding="utf-8-sig")
    cursor.execute(sql)
    print(f"executed {file_path.name}")


def setup_databases():
    base_dir = Path(__file__).resolve().parent.parent
    ddl_dir = base_dir / "ddl"
    files = [ddl_dir / "01_source_schema.sql", ddl_dir / "02_target_schema.sql"]

    with get_connection(autocommit=True) as conn:
        with conn.cursor() as cursor:
            for file_path in files:
                execute_sql_file(cursor, file_path)

    print("database setup complete")


if __name__ == "__main__":
    setup_databases()

