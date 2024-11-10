from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from rates.nbp_rates import Rates


class CarsTransformation:

    COLUMNS_LIST = [
        "id", "url", "region", "region_url", "price", "currency",
        "year", "manufacturer", "model", "condition",
        "fuel", "odometer", "transmission", "VIN",
        "drive", "size", "type", "paint_color",
        "description", "state", "lat", "long", "posting_date",
        "posting_year", "posting_month",
    ]

    PARTITION_COLUMNS = ["posting_year", "posting_month"]

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_data(self, csv_file: str):
        df = (
            self.spark_session.read
            .option("header", "true")
            .csv(csv_file)
        )
        return df

    def clean_data(self, df: DataFrame) -> DataFrame:
        clean_df = (
            df
            .withColumn("price", col("price").cast(IntegerType()))
            .withColumn("odometer", col("odometer").cast(LongType()))
            .withColumn("lat", col("lat").cast(FloatType()))
            .withColumn("long", col("long").cast(FloatType()))
            .withColumn("posting_date", substring(col("posting_date"), 1, 10).cast(DateType()))
            .withColumn("posting_year", year(col("posting_date")))
            .withColumn("posting_month", month(col("posting_date")))
            .withColumn("currency", lit("USD"))
            .filter(
                (col("model").isNotNull())
                & (col("manufacturer").isNotNull())
                & (col("price") > 0)
                & (col("posting_date").rlike(r"^2\d{3}-\d{2}-\d{2}"))
            )
            .select(self.COLUMNS_LIST)
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
                cars_df.join(rates_df, [cars_df.currency == rates_df.code], "left_outer")
                .withColumn("price_PLN", col("mid") * col("price"))
                .select(*self.COLUMNS_LIST, "mid", "price_PLN")
        )
        return df_with_rates

    def save_data_to_parquet(self, df: DataFrame, destination_path: str):
        df.write.partitionBy(self.PARTITION_COLUMNS).mode("overwrite").parquet(destination_path)
