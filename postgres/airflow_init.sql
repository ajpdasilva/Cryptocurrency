CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL ON SCHEMA public TO airflow;
CREATE DATABASE airflow_db OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
GRANT CONNECT ON DATABASE airflow_db TO airflow; 

--GRANT ALL ON SCHEMA public TO airflow;
--CREATE DATABASE airflow_db;
--ALTER DATABASE airflow_db OWNER TO airflow;
--GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
--GRANT CONNECT ON DATABASE airflow_db TO airflow;   
