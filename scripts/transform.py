import pandas as pd
import logging

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def transform_crypto_data(raw_data, load_date):
    """Transform the raw cryptocurrency data."""
    if not raw_data:
        logging.warning("The variable is empty, no data to process.")
        return pd.DataFrame()  # Return an empty DataFrame if raw_data is empty or false
    else:
        df = pd.DataFrame(raw_data)

        logging.info("Printing the first 5 rows of the DataFrame:")
        logging.info(f"\n{df.head(5).to_string()}")

        df["last_updated"] = pd.to_datetime(df["last_updated"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        df["atl_date"] = pd.to_datetime(df["ath_date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        df["ath_date"] = pd.to_datetime(df["ath_date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        df["load_date"] = load_date

        numeric_columns = [
            "current_price", "market_cap", "market_cap_rank",
            "total_volume", "high_24h", "low_24h",
            "price_change_24h", "price_change_percentage_24h",
            "circulating_supply", "total_supply", "ath"
        ]
        df[numeric_columns] = df[numeric_columns].fillna(0)
        logging.info("Data transformed successfully")

        try:
            filename = f"/opt/airflow/output/transformed_crypto_data_{load_date}.csv"
            df.to_csv(filename, index=False)
            logging.info(f"Transformed data saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving transformed data: {e}")

        return df
