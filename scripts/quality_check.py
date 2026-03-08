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


def check_fact_crypto(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM fact_crypto WHERE date_key = %s"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]
    cursor.close()

    cursor = conn.cursor()
    check_query = "INSERT INTO data_quality_log (check_name, check_result) VALUES ('Fact Crypto Check', %s);"
    cursor.execute(check_query, (count,))
    cursor.close()
    
    conn.commit()

    if count == 0:
        logging.warning(f"Quality check: Found {count} records loaded on fact_crypto table for {load_date}.")
        raise ValueError("Fact Crypto: No data loaded.")
    else:
        logging.info(f"Quality check: Found {count} records loaded on fact_crypto table for {load_date}.")


def check_crypto_daily_summary_quality(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM crypto_daily_summary WHERE summary_date = %s"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]
    cursor.close()

    cursor = conn.cursor()
    check_query = "INSERT INTO data_quality_log (check_name, check_result) VALUES ('Crypto Daily Summary Check', %s);"
    cursor.execute(check_query, (count,))
    cursor.close()
    
    conn.commit()

    if count == 0:
        logging.warning(f"Quality check: Found {count} record(s) on crypto_daily_summary table for summary date {load_date}.")
        raise ValueError("Crypto Daily Summary: No data loaded.")
    else:
        logging.info(f"Quality check: Found {count} record(s) on crypto_daily_summary table for summary date {load_date}.")


def check_no_null_market_cap(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM fact_crypto WHERE date_key = %s AND market_cap IS NULL"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]
    cursor.close()

    cursor = conn.cursor()
    check_query = "INSERT INTO data_quality_log (check_name, check_result) VALUES ('No Null Market Cap', %s);"
    cursor.execute(check_query, (count,))
    cursor.close()

    conn.commit()

    if count == 0:
        logging.info(f"Quality check: Found {count} record(s) with null market cap for {load_date}.")
    else:
        logging.warning(f"Quality check: Found {count} record(s) with null market cap for {load_date}.")
        raise ValueError("Null market cap values found.")


def check_positive_market_cap(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT COUNT(*) FROM fact_crypto WHERE date_key = %s AND market_cap <= 0"
    cursor.execute(check_query, (load_date,))
    count = cursor.fetchone()[0]
    cursor.close()

    cursor = conn.cursor()
    check_query = "INSERT INTO data_quality_log (check_name, check_result) VALUES ('Market Cap Must Be Positive', %s);"
    cursor.execute(check_query, (count,))
    cursor.close()

    conn.commit()

    if count == 0:
        logging.info(f"Quality check: Found {count} record(s) with negative market cap entries for {load_date}.")
    else:
        logging.warning(f"Quality check: Found {count} record(s) with negative market cap entries for {load_date}.")
        raise ValueError("Market cap values are not positive.")


def check_rank_uniqueness(conn, load_date):
    cursor = conn.cursor()
    check_query = "SELECT date_key, market_cap_rank, COUNT(*) FROM fact_crypto WHERE date_key = %s GROUP BY date_key, market_cap_rank HAVING COUNT(*) > 1"
    cursor.execute(check_query, (load_date,))
    result = cursor.fetchone()
    cursor.close()

    if result is not None:
        count = result[0]
    else:
        count = 0

    cursor = conn.cursor()
    check_query = "INSERT INTO data_quality_log (check_name, check_result) VALUES ('Rank Uniqueness Per Day', %s);"
    cursor.execute(check_query, (count,))
    cursor.close()

    conn.commit()

    if count == 0:
        logging.info(f"Quality check done: Found {count} record(s) with Rank Uniqueness for {load_date}.")
    else:
        logging.warning(f"Quality check: Found {count} record(s) with duplicate Rank entries for {load_date}.")
        raise ValueError("Market cap values are not positive.")


def run_quality_check(check_date):

    try:
        db = psycopg2.connect(**db_config)
        logging.info("Database connection successful")
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise Exception("Database connection failed")
    
    check_fact_crypto(db, check_date)
    check_crypto_daily_summary_quality(db, check_date)
    check_no_null_market_cap(db, check_date)
    check_positive_market_cap(db, check_date)
    check_rank_uniqueness(db, check_date)

    db.close()
