"""Module responsible for raw data transformation."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class CarsTransformation:

    PARTITION_COLUMNS = ["year", "producer"]

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_data(self, csv_file: str):
        """Loads data from csv file.
        Args: csv_file (str): Path to the csv file with car's data.

        Returns: DataFrame: Returns dataframe with raw car's data.
        """
        df = (
            self.spark_session.read
            .option("header", "true")
            .csv(csv_file)
        )
        return df

    def clean_data(self, df: DataFrame) -> DataFrame:
        """Initial transformation and cleaning raw data.
        Args: df (DataFrame): DataFrame with car's data.

        Returns: DataFrame: Transformed DataFrame.
        """
        clean_df = (
            df
            .withColumn("producer", substring_index((col("producer"), " ", 1)))
            .withColumn("price", col("price").cast(FloatType()))
            .withColumn("year", col("year").cast(IntegerType()))
            .withColumn("mileage_unit", substring_index(col("mileage"), " ", -1))
            .withColumn(
                "mileage", regexp_replace(col("mileage"), r"[^0-9]", "").cast(LongType())
            )
            .withColumn("posting_year", year(col("posting_date")))
            .withColumn("posting_month", month(col("posting_date")))
            .withColumn("price_currency", lit("USD"))
            .filter(
                (col("producer").isNotNull())
                & (col("model").isNotNull())
                & (col("price") > 0)
            )
            .fillna("N/A")
        )
        return clean_df

    def get_price_in_pln(self, cars_df: DataFrame, rates_df: DataFrame) -> DataFrame:
        """Assign PLN rate and create column with price in PLN.
        Args: manufacturer (str): Name of the car's manufacturer.
            cars_df (DataFrame): DataFrame with cars data.
            rates_df (DataFrame): DataFrame with rates.

        Returns: DataFrame: Returns dataframe with additional column price in PLN.
        """
        df_with_rates = (
                cars_df.join(rates_df, [cars_df.price_currency == rates_df.code], "left_outer")
                .withColumn(
                    "price_PLN", when(
                        col("price_currency") != "PLN",
                        col("mid") * col("price")
                        .otherwise(col("price_currency") * 1))
                )
        )
        return df_with_rates

    def save_data_to_parquet(self, df: DataFrame, destination_path: str):
        """Saves data to parquet files.

        Args: df (str): DataFrame with data to be saved.
            destination_path (DataFrame): Path to destination directory.
        """
        df.write.partitionBy(self.PARTITION_COLUMNS).mode("overwrite").parquet(destination_path)
