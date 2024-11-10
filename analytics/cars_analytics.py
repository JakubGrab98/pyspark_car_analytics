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
        """Retrieves car's model statistics
        Args: manufacturer (str): Name of the car's manufacturer.
            model (str): Name of the car's model.

        Returns: DataFrame: Returns dataframe with basic statistics.
        """
        df = self.base_df.select("manufacturer", "model", "year", "price")
        statistics_df = (
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
        return statistics_df

    def filter_advertises(
            self, manufacturer: str, model: str, max_price: int, min_price: int
    ) -> DataFrame:
        """Retrieves car's model advertises based on user parameters.
        Args: manufacturer (str): Name of the car's manufacturer.
            model (str): Name of the car's model.
            max_price (int): Maximum price of the searched model.
            min_price (int): Minimum price of the searched model.

        Returns: DataFrame: Returns dataframe with car's advertises details.
        """
        base_df = self.base_df.select(
            "url", "region", "year", "manufacturer",
            "model", "condition", "fuel", "odometer",
            "VIN", "posting_date", "price",
        )
        final_df = (
            base_df.filter(
                (col("manufacturer") == manufacturer)
                & (col("model") == model)
                & (col("price") <= max_price)
                & (col("price") >= min_price)
            )
        )
        return final_df
