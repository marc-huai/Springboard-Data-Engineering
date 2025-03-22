# Apache Airflow Mini Project – Market Data Pipeline & Log Analyzer

This submission combines two mini projects:
1. Part 1: DAG Scheduling – Uses Airflow to build a data pipeline that extracts 1-minute stock data from Yahoo Finance.
2. Part 2: Log Analyzer – Parses Airflow log files to extract and summarize error messages.

## Project Structure

├── dags/
│   └── marketvol.py         # Airflow DAG definition for AAPL/TSLA pipeline
│
├── logs/                    # Directory containing Airflow logs
│
├── docker-compose.yml       # (Optional) Docker setup for Airflow
├── .env                     # Environment variables
├── README.md                # Project documentation (this file)
├── log_analyzer.py     	 # Python script to analyze Airflow logs


## Part 1: DAG Scheduling – marketvol.py

### DAG Overview

This DAG:
- Downloads 1-minute stock price data for AAPL and TSLA from Yahoo Finance
- Stores the data as CSVs in a target directory
- Runs a query on both files to calculate statistics (e.g., average close)
- Uses Celery Executor for parallel task execution

### DAG Schedule

- Runs every weekday at 6 PM
- Retries 2 times on failure with a 5-minute delay

### Task Summary

| Task ID           | Description                          | Operator         | Depends On          |
|------------------|--------------------------------------|------------------|---------------------|
| create_temp_dir  | Create date-based temp directory     | BashOperator     | —                   |
| download_AAPL    | Download AAPL data                   | PythonOperator   | create_temp_dir     |
| download_TSLA    | Download TSLA data                   | PythonOperator   | create_temp_dir     |
| move_AAPL        | Move AAPL file to target dir         | BashOperator     | download_AAPL       |
| move_TSLA        | Move TSLA file to target dir         | BashOperator     | download_TSLA       |
| analyze_data     | Run analysis on both stock datasets  | PythonOperator   | move_AAPL, move_TSLA|

### Running the DAG

1. Initialize Airflow
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
