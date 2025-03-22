from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import yfinance as yf
import pandas as pd
import os

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # current_dateTime = datetime.now()
    # 'start_date': current_dateTime
    'start_date': datetime(2025, 3, 1, 18, 0),  # Adjust as needed
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'marketvol',
    default_args=default_args,
    description='Stock Market Data Pipeline',
    schedule_interval='0 18 * * 1-5',  # Run at 6 PM every weekday
)

# Define the temporary data directory
data_dir = "/tmp/data"

# Task 0: Create the temporary directory
t0 = BashOperator(
    task_id='create_temp_dir',
    bash_command=f"mkdir -p {data_dir}/$(date +%Y-%m-%d)",
    dag=dag,
)

# Function to download market data
def download_stock_data(stock_symbol):
    start_date = date.today()
    end_date = start_date + timedelta(days=1)
    df = yf.download(stock_symbol, start=start_date, end=end_date, interval='1m')
    
    # Define the output directory
    date_dir = f"{data_dir}/{start_date}"
    os.makedirs(date_dir, exist_ok=True)
    
    # Save to CSV
    file_path = f"{date_dir}/{stock_symbol}.csv"
    df.to_csv(file_path, index=False)
    print(f"Data for {stock_symbol} saved to {file_path}")

# Task 1 & 2: Download AAPL and TSLA data
t1 = PythonOperator(
    task_id='download_AAPL',
    python_callable=download_stock_data,
    op_kwargs={'stock_symbol': 'AAPL'},
    dag=dag,
)

t2 = PythonOperator(
    task_id='download_TSLA',
    python_callable=download_stock_data,
    op_kwargs={'stock_symbol': 'TSLA'},
    dag=dag,
)

# Task 3 & 4: Move data files to storage
t3 = BashOperator(
    task_id='move_AAPL',
    bash_command=f"mv {data_dir}/$(date +%Y-%m-%d)/AAPL.csv /tmp/data_storage/",
    dag=dag,
)

t4 = BashOperator(
    task_id='move_TSLA',
    bash_command=f"mv {data_dir}/$(date +%Y-%m-%d)/TSLA.csv /tmp/data_storage/",
    dag=dag,
)

# Function to run an analysis query
def analyze_stock_data():
    aapl_df = pd.read_csv("/tmp/data_storage/AAPL.csv")
    tsla_df = pd.read_csv("/tmp/data_storage/TSLA.csv")

    avg_aapl_close = aapl_df['close'].mean()
    avg_tsla_close = tsla_df['close'].mean()

    print(f"AAPL Average Close Price: {avg_aapl_close}")
    print(f"TSLA Average Close Price: {avg_tsla_close}")

# Task 5: Analyze data
t5 = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_stock_data,
    dag=dag,
)

# Define dependencies
t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5
