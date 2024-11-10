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
