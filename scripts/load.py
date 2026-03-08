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
        logging.info("Data loaded successfully into crypto_data table. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading Crypto data: {e}")
        conn.close()
        raise Exception("Crypto data loading failed")
    

def load_to_dim_coin(conn, load_date):
    """ Load unique coin information into the dim_coin table."""
    try:
        cursor = conn.cursor()
        query = ("""
                INSERT INTO dim_coin (coin_id, symbol, name, ath, ath_date)
                SELECT
                    coin_id,
                    symbol,
                    name,
                    ath,
                    ath_date
                FROM crypto_data
                WHERE load_date = %s
                ON CONFLICT (coin_id) DO UPDATE
                SET
                    symbol       = EXCLUDED.symbol,
                    name         = EXCLUDED.name,
                    ath          = EXCLUDED.ath,
                    ath_date     = EXCLUDED.ath_date
                """)
        cursor.execute(query, (load_date,))
        cursor.close()
        conn.commit()
        logging.info("Data loaded successfully into dim_coin table. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading coin data: {e}")
        conn.close()
        raise Exception("Loading of Coin data failed")


def load_to_dim_date(conn):
    """ Load unique date information into the dim_date table."""
    try:
        cursor = conn.cursor()
        query = ("""
                INSERT INTO dim_date (date_key, year, month, day, week, quarter)
                SELECT DISTINCT
                    load_date,
                    EXTRACT(YEAR FROM load_date),
                    EXTRACT(MONTH FROM load_date),
                    EXTRACT(DAY FROM load_date),
                    EXTRACT(WEEK FROM load_date),
                    EXTRACT(QUARTER FROM load_date)
                FROM crypto_data
                ON CONFLICT (date_key) DO NOTHING
                """)
        cursor.execute(query)
        cursor.close()
        conn.commit()
        logging.info("Data loaded successfully into dim_date table. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading date data: {e}")
        conn.close()
        raise Exception("Loading of Date data failed")


def load_to_crypto_fact(conn, load_date):
    """ Load fact data into the fact_crypto table."""
    try:
        cursor = conn.cursor()
        query = ("""
                INSERT INTO fact_crypto (
                    coin_id,
                    date_key,
                    current_price,
                    market_cap,
                    market_cap_rank,
                    total_volume,
                    high_24h,
                    low_24h,
                    price_change_24h,
                    price_change_percentage_24h,
                    circulating_supply,
                    total_supply
                )
                SELECT
                    cd.coin_id,
                    cd.load_date,
                    cd.current_price,
                    cd.market_cap,
                    cd.market_cap_rank,
                    cd.total_volume,
                    cd.high_24h,
                    cd.low_24h,
                    cd.price_change_24h,
                    cd.price_change_percentage_24h,
                    cd.circulating_supply,
                    cd.total_supply
                FROM crypto_data cd
                JOIN dim_date dt ON dt.date_key = cd.load_date
                WHERE cd.load_date = %s
                ON CONFLICT (coin_id, date_key) DO UPDATE
                SET
                    current_price = EXCLUDED.current_price,
                    market_cap = EXCLUDED.market_cap,
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    total_volume = EXCLUDED.total_volume,
                    high_24h = EXCLUDED.high_24h,
                    low_24h = EXCLUDED.low_24h,
                    price_change_24h = EXCLUDED.price_change_24h,
                    price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
                    circulating_supply = EXCLUDED.circulating_supply,
                    total_supply = EXCLUDED.total_supply,
                    inserted_at = CURRENT_TIMESTAMP
                """)
        cursor.execute(query, (load_date,))
        cursor.close()
        conn.commit()
        logging.info("Data loaded successfully into fact_crypto_data table. Committing transaction.")

    except Exception as e:
        logging.error(f"Error loading crypto fact data: {e}")
        conn.close()
        raise Exception("Loading of Crypto fact data failed")


def connect_db():
    """Establish a connection to the PostgreSQL database."""
    try:
        # Connect to Database
        db = psycopg2.connect(**db_config)
        logging.info("Database connection successful")
        return db
    except psycopg2.Error as e:
        logging.error(f"Error connecting to the database: {e}")
        raise Exception("Database connection failed")
    

def load_to_data(df, summary_date):
    """Load transformed data into the crypto_data table and summary data into crypto_daily_summary table."""

    if df.empty:
        logging.warning("DataFrame is empty. Skipping data loading.")
        return
    else:
        try:
            db = connect_db()
            load_to_crypto_data(db, df)
            load_to_dim_coin(db, summary_date)
            load_to_dim_date(db)
            load_to_crypto_fact(db, summary_date)
        finally:
            db.close()
