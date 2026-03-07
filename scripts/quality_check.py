import os
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()

## --- DB Parameters ---
db_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PWD")
}

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def check_crypto_data_quality(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM crypto_data WHERE load_date = %s"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]

    if count == 0:
        raise ValueError("No data loaded.")

    logging.info(f"Quality check done: Found {count} rows for load date {load_date} on crypto_data table.")
    cursor.close()


def check_crypto_daily_summary_quality(conn, summary_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM crypto_daily_summary WHERE summary_date = %s"
    cursor.execute(check_query, (summary_date,))
    count = cursor.fetchone()[0]

    if count == 0:
        raise ValueError("No data loaded.")

    logging.info(f"Quality check done: Found {count} row(s) for summary date {summary_date} on crypto_daily_summary table.")
    cursor.close()


def run_quality_check(date):

    try:
        db = psycopg2.connect(**db_config)
        logging.info("Database connection successful")
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise Exception("Database connection failed")
    
    check_crypto_data_quality(db, date)

    check_crypto_daily_summary_quality(db, date)

    db.close()
