import os
import psycopg2
import logging
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


def load_to_staging(df):

    try:
        # Connect to Database
        conn = psycopg2.connect(**db_config)
        logging.info("Database connection successful")
    except psycopg2.Error as e:
        logging.error(f"Unable to connect to the database: {e}")
        raise Exception("Database connection failed")
    
    cursor = conn.cursor()

    try:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crypto_data (
                    coin_id, symbol, name, current_price, market_cap, market_cap_rank, total_volume,
                    high_24h, low_24h, price_change_24h, price_change_percentage_24h, circulating_supply,
                    total_supply, ath, ath_date, last_updated, load_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (coin_id, load_date) DO NOTHING;
            """, tuple(row[
                [
                    "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank", "total_volume",
                    "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h", "circulating_supply",
                    "total_supply", "ath", "ath_date", "last_updated", "load_date"
                ]
            ]))
        
        cursor.close()
        conn.commit()
        logging.info("Data loaded successfully. Committing transaction.")
        conn.close()
        logging.info("Database connection closed.")
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        conn.close()
        raise Exception("Data loading failed")
