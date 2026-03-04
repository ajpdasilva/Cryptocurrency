--
-- PostgreSQL database
--

CREATE TABLE public.crypto_raw (
	id serial4 NOT NULL,
	insert_time timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	raw_data jsonb NULL,
	CONSTRAINT crypto_raw_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_crypto_raw_timestamp ON public.crypto_raw (insert_time);


CREATE TABLE public.crypto_data (
	coin_id text NOT NULL,
	symbol text NOT NULL,
	"name" text NOT NULL,
	current_price numeric(20, 8) NULL,
	market_cap numeric(20, 2) NULL,
	market_cap_rank int4 NULL,
	total_volume numeric(20, 2) NULL,
	high_24h numeric(20, 8) NULL,
	low_24h numeric(20, 8) NULL,
	price_change_24h numeric(20, 8) NULL,
	price_change_percentage_24h numeric(10, 4) NULL,
	circulating_supply numeric(20, 2) NULL,
	total_supply numeric(20, 2) NULL,
	ath numeric(20, 8) NULL,
	ath_date timestamp NULL,
	last_updated timestamp NULL,
	load_date date NOT NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT crypto_data_pkey PRIMARY KEY (coin_id, load_date)
);
CREATE INDEX idx_crypto_data_rank ON crypto_staging (market_cap_rank);

CREATE INDEX idx_crypto_data_load_date ON crypto_staging (load_date);


CREATE TABLE public.crypto_daily_summary (
    summary_date date NOT NULL,
    total_market_cap numeric(20,2),
    total_volume numeric(20,2),
    avg_price numeric(20,8),
    top_gainer text,
    top_loser text,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT crypto_daily_summary_pkey PRIMARY KEY (summary_date)
);

GRANT ALL ON SCHEMA public TO airflow;
