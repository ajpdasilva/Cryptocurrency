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


def load_to_crypto_daily_summary(conn, load_date):    
    """ Load summary data into the crypto_daily_summary table."""
    try:
        cursor = conn.cursor()
        query = ("""
                INSERT INTO crypto_daily_summary (summary_date, total_market_cap,total_volume, avg_price, top_gainer, top_loser)
                SELECT 
                    cd.load_date AS summary_date,
                    SUM(cd.market_cap) AS total_market_cap,
                    SUM(cd.total_volume) AS total_volume,
                    AVG(cd.current_price) AS avg_price,
                    MAX(CASE WHEN cd.price_change_percentage_24h = (SELECT MAX(price_change_percentage_24h) FROM crypto_data WHERE load_date = cd.load_date) THEN name END) AS top_gainer,
                    MAX(CASE WHEN cd.price_change_percentage_24h = (SELECT MIN(price_change_percentage_24h) FROM crypto_data WHERE load_date = cd.load_date) THEN name END) AS top_loser
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


def create_update_price_history(conn):
    """Create or refresh the materialized view for price history."""
    create_view_query = """
		CREATE MATERIALIZED VIEW IF NOT EXISTS mv_price_history AS
		SELECT
			coin_id,
			load_date AS date,
			MIN(current_price) AS daily_low,
			MAX(current_price) AS daily_high,
			FIRST_VALUE(current_price) OVER (PARTITION BY coin_id, load_date ORDER BY last_updated) AS open_price,
			LAST_VALUE(current_price) OVER (PARTITION BY coin_id, load_date ORDER BY last_updated ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS close_price,
			SUM(total_volume) AS daily_volume
		FROM crypto_data
		GROUP BY coin_id, load_date;

		CREATE UNIQUE INDEX IF NOT EXISTS idx_price_history_coin_date_id 
		ON mv_price_history (coin_id, date);
	"""
    
    try:
        cur = conn.cursor()

        # Check if the materialized view exists
        cur.execute("SELECT count(*) FROM pg_matviews WHERE matviewname = 'mv_price_history'")
        exists = cur.fetchone()[0] > 0

        if not exists:
            cur.execute(create_view_query)
            logging.info("Materialized View 'mv_price_history' was created successfully.")
        else:
            logging.info("Materialized View exists. Refreshing data concurrently...")
            try:
                # CONCURRENTLY allows users to query the view while it refreshes
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_price_history;")
                logging.info("Refresh of Materialized View 'mv_price_history' completed successfully.")
            except psycopg2.Error as e:
                logging.error(f"Error refreshing materialized view: {e}")
                raise Exception("Materialized view refresh failed")
        
        cur.close()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        cur.close()


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


def load_to_analytics(run_date):
    """Load transformed data into the crypto_data table and summary data into crypto_daily_summary table."""  
    db = connect_db()

    try:
        load_to_crypto_daily_summary(db, run_date)
        create_update_price_history(db)
    finally:
        db.close()
