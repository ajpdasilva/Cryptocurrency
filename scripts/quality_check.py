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


def run_quality_check(load_date):

    try:
        conn = psycopg2.connect(**db_config)
        logging.info("Database connection successful")
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise Exception("Database connection failed")
    
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM crypto_data WHERE load_date = %s"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]

    if count == 0:
        raise ValueError("No data loaded.")

    logging.info(f"Quality check done: Found {count} rows for load date {load_date}")

    cursor.close()
    conn.close()
