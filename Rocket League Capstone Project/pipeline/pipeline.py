from pyspark.sql.functions import col, to_timestamp
from pipeline.config import Configuration
from pipeline.metrics import log_pipeline_metrics
import logging

logger = logging.getLogger(__name__)

class DataPipelineSpark:
    def load_data(self):
        try:
            self.df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(Configuration.file_location)
            )
            logger.info(f"Data loaded with {self.df.count()} records")
            log_pipeline_metrics(self.df, "Loading Stage")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def clean_data(self):
        try:
            self.df = self.df.dropDuplicates().dropna()
            if 'event_time' in self.df.columns:
                self.df = self.df.withColumn("event_time", to_timestamp(col("event_time")))
            logger.info("Data cleaned successfully")
            log_pipeline_metrics(self.df, "Cleaning Stage")
            return self.df
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            raise

    def store_as_parquet(self, output_path_base="parqueted_data", partition_column=None):
        try:
            wasbs_path = f"{Configuration.file_path}{output_path_base}"
            writer = self.df.write.mode("overwrite")
            if partition_column:
                writer = writer.partitionBy(partition_column)
            writer.parquet(wasbs_path)
            logger.info(f"Data stored at {wasbs_path}")
        except Exception as e:
            logger.error(f"Error writing parquet: {e}")
            raise
