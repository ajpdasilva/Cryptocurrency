import os
from datetime import date
import psycopg2
import requests
import logging
from dotenv import load_dotenv
import json

load_dotenv()

# Database connection parameters from environment variables
## --- DB Parameters ---
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PWD")

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def check_db_connect(host_db, user_db, pwd_db, port_db, name_db):
    """Check database connection and return connection object."""
    try:
        # Connect to Database
        db_conn = psycopg2.connect(
            host=host_db,
            port=port_db,
            database=name_db,
            user=user_db, 
            password=pwd_db
        )
        logging.info("Database connection successful")
        return db_conn
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise Exception("Database connection failed")


def check_if_data_exists(conn):
    """Check if data for the current date already exists in the database."""
    check_query = "SELECT COUNT(*) FROM crypto_raw WHERE DATE(insert_time) = %s"
    cursor = conn.cursor()
    cursor.execute(check_query, [date.today()])
    already_exists = cursor.fetchone()[0] > 0
    cursor.close()
    
    return already_exists         


def fetch_crypto_data():
    """Fetch cryptocurrency market data from CoinGecko API."""
    logging.info("Starting API request to CoinGecko")

    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        logging.error(f"API request failed: {response.status_code}")
        raise Exception("API request failed")

    logging.info("Data extracted successfully")

    return response.json()


def load_to_raw_table(conn, data):
    """Load raw data as JSON into the crypto_raw table."""
    try:
        cursor = conn.cursor()

        # Insert the raw data as JSON into the crypto_raw table
        insert_query = "INSERT INTO crypto_raw (raw_data) VALUES (%s)"
        cursor.execute(insert_query, [json.dumps(data)])
        
        cursor.close()

        # Commit and close
        conn.commit()
        logging.info("Data successfully inserted into crypto_raw table.")

        conn.close()
        logging.info("Database connection closed.")
    except Exception as e:
        logging.error(f"Error inserting data into crypto_raw table: {e}")
        conn.close()


def run_extract():
    """Main extraction function."""
    db = check_db_connect(db_host, db_user, db_pass, db_port, db_name)
    data_check = check_if_data_exists(db)

    if data_check:
        logging.warning(f"Skipping data extraction: Data has already been inserted for {date.today()}.")
        db.close()
        logging.info("Database connection closed.")
        return None
    else:
        raw_data = fetch_crypto_data()
        load_to_raw_table(db, raw_data)
        # logging.info(f"\n{raw_data}")
        return raw_data
