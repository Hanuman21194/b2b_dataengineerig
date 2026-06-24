#!/usr/bin/env python3
import argparse
import re
from datetime import datetime
from pathlib import Path

from config import WEBLOG_FILE
from db import get_connection

LOG_PATTERN = re.compile(
    r'^(?P<ip>\S+)\s+\S+\s+(?P<username>\S+)\s+\[(?P<time>.*?)\]\s+'
    r'"(?P<request>.*?)"\s+(?P<status>\d{3})\s+(?P<size>\S+)\s+'
    r'"(?P<referer>.*?)"\s+"(?P<user_agent>.*?)"$'
)


def parse_time(value):
    try:
        return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        return None


def load_weblogs_to_source(log_file: Path):
    if not log_file.exists():
        raise FileNotFoundError(f"missing weblog file: {log_file}")

    inserted = 0
    skipped = 0
    source_file = str(log_file.resolve())

    with get_connection() as conn:
        with conn.cursor() as cursor:
            with log_file.open("r", encoding="utf-8") as f:
                for line_number, raw_line in enumerate(f, start=1):
                    raw_line = raw_line.rstrip("\n")
                    match = LOG_PATTERN.match(raw_line)
                    if match:
                        client_ip = match.group("ip")
                        username = match.group("username")
                        request_time = parse_time(match.group("time"))
                        user_agent = match.group("user_agent")
                        if username == "-":
                            username = None
                        if user_agent == "-":
                            user_agent = None
                    else:
                        client_ip = None
                        username = None
                        request_time = None
                        user_agent = None

                    cursor.execute(
                        """
                        INSERT INTO source.weblogs (
                            client_ip, username, request_time, user_agent,
                            raw_line, source_file, source_line_number
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_file, source_line_number) DO NOTHING
                        """,
                        (client_ip, username, request_time, user_agent, raw_line, source_file, line_number),
                    )
                    if cursor.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1
        conn.commit()

    print(f"loaded {inserted} new weblog rows into source.weblogs; skipped {skipped} existing rows")


def main():
    parser = argparse.ArgumentParser(description="Load Apache weblog file into source.weblogs.")
    parser.add_argument("--file", default=str(WEBLOG_FILE))
    args = parser.parse_args()
    load_weblogs_to_source(Path(args.file))


if __name__ == "__main__":
    main()
