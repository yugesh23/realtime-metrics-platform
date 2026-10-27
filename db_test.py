
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def main():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "realtime_metrics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT current_database(), version();
            """)
            database, version = cursor.fetchone()

        print(f"Connected to database: {database}")
        print(f"PostgreSQL version: {version.split(',')[0]}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
