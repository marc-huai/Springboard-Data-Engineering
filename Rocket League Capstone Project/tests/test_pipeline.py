import unittest
from pyspark.sql import Row, SparkSession
from pipeline.pipeline import DataPipelineSpark
from pipeline.config import Configuration

spark = SparkSession.builder.getOrCreate()

class TestDataPipelineSpark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipeline = DataPipelineSpark()
        cls.pipeline.load_data()

    def test_sample_data_loaded_correctly(self):
        df = self.pipeline.df
        self.assertGreater(df.count(), 0)

    def test_clean_data_removes_nulls(self):
        null_row = Row(**{col: None for col in self.pipeline.df.columns})
        df_with_null = spark.createDataFrame([null_row], schema=self.pipeline.df.schema)
        self.pipeline.df = self.pipeline.df.union(df_with_null)
        cleaned_df = self.pipeline.clean_data()
        self.assertLess(cleaned_df.count(), self.pipeline.df.count())

    def test_store_as_parquet(self):
        self.pipeline.clean_data()
        self.pipeline.store_as_parquet(output_path_base="sample_test_parquet")
        written_df = spark.read.parquet(f"{Configuration.file_path}sample_test_parquet")
        self.assertEqual(written_df.count(), self.pipeline.df.count())
