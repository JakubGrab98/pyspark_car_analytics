from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

class CarsAnalytics:

    TRANSFORMED_DATA = "data/transform"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
        self.base_df = self.load_transformed_data()

    def load_transformed_data(self) -> DataFrame:
        df = (
            self.spark_session.read
            .parquet(self.TRANSFORMED_DATA)
        )
        return df

    def avg_manufacturer_price_by_year(self, manufacturer: str) -> DataFrame:
        df_copy = self.base_df.select("posting_year", "manufacturer", "price")
        avg_price_df = (
            df_copy
            .filter(col("manufacturer").contains(manufacturer))
            .groupby("posting_year", "manufacturer")
            .avg("price")
            .orderBy("posting_year")
        )
        return avg_price_df

    def count_advertises_by_year(self) -> DataFrame:
        df = self.base_df.select("posting_year")
        count_df = (
            df.groupby("posting_year")
            .count()
        )
        return count_df

    def get_model_statistics(self, manufacturer: str, model: str) -> DataFrame:
        df = self.base_df.select("manufacturer", "model", "year", "price")
        max_price_df = (
            df.filter(
                (col("manufacturer") == manufacturer) & (col("model") == model)
            )
            .groupby("manufacturer", "model")
            .agg(
                max("year").alias("max_prod_year"),
                min("year").alias("min_prod_year"),
                avg("price").alias("avg_price"),
                max("price").alias("max_price"),
                min("price").alias("min_price"),
            )
        )
        return max_price_df
