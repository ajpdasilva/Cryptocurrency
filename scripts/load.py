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


def load_to_crypto_data(conn, df):
    """Load transformed data into the crypto_data table."""
    try:
        cursor = conn.cursor()
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
        logging.info("Data loaded successfully into crypto_data. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        conn.close()
        raise Exception("Data loading failed")
    

def load_to_crypto_daily_summary(conn, load_date):    
    """ Load summary data into the crypto_daily_summary table."""
    try:
        cursor = conn.cursor()
        query = ("""
                INSERT INTO crypto_daily_summary (summary_date, total_market_cap,total_volume, avg_price, top_gainer, top_loser)
                SELECT 
                    cd.load_date, SUM(cd.market_cap), SUM(cd.total_volume), AVG(cd.current_price),
                    MAX(cd.name) FILTER (
                        WHERE cd.price_change_percentage_24h =
                            (SELECT MAX(price_change_percentage_24h)
                                FROM crypto_data
                                WHERE load_date = cd.load_date)
                    ) AS top_gainer,
                    MAX(cd.name) FILTER (
                        WHERE cd.price_change_percentage_24h =
                            (SELECT MIN(price_change_percentage_24h)
                                FROM crypto_data
                                WHERE load_date = cd.load_date)
                    ) AS top_loser
                FROM crypto_data cd
                WHERE cd.load_date = %s
                GROUP BY cd.load_date
                ON CONFLICT (summary_date)
                DO UPDATE SET
                    total_market_cap = EXCLUDED.total_market_cap,
                    total_volume = EXCLUDED.total_volume,
                    avg_price = EXCLUDED.avg_price,
                    top_gainer = EXCLUDED.top_gainer,
                    top_loser = EXCLUDED.top_loser,
                    created_at = CURRENT_TIMESTAMP
                """)
        cursor.execute(query, (load_date,))
        cursor.close()
        conn.commit()
        logging.info("Data loaded successfully into crypto_daily_summary. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading data: {e}")
        conn.close()
        raise Exception("Data loading failed")


def load_to_data(df, load_date):
    """Load transformed data into the crypto_data table and summary data into crypto_daily_summary table."""

    if df.empty:
        logging.warning("DataFrame is empty. Skipping data loading.")
        return
    else:
        try:
            # Connect to Database
            db = psycopg2.connect(**db_config)
            logging.info("Database connection successful")
            
        except psycopg2.Error as e:
            logging.error(f"Unable to connect to the database: {e}")
            raise Exception("Database connection failed")
        
        try:
            load_to_crypto_data(db, df)
            load_to_crypto_daily_summary(db, load_date)
        finally:
            db.close()
