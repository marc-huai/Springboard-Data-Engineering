from dotenv import load_dotenv
import os

load_dotenv()

class Configuration:
    storage_account_name = "rocketleague"
    container_name = "rocketleague-data"
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
    file_name = "*.csv"
    file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
    file_location = f"{file_path}{file_name}"
