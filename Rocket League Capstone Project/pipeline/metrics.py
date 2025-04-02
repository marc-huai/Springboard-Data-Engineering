from datetime import datetime

def log_pipeline_metrics(df, stage: str):
    try:
        num_rows = df.count()
        num_columns = len(df.columns)
        columns = ", ".join(df.columns)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} | Stage: {stage}, Rows: {num_rows}, Columns: {num_columns}\n"
        print(log_line)
    except Exception as e:
        print(f"Error logging metrics: {e}")
