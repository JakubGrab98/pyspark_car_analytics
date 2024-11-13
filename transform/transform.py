"""Module responsible for raw data transformation."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def clean_data(df: DataFrame) -> DataFrame:
    """Initial transformation and cleaning raw data.
    Args: df (DataFrame): DataFrame with car's data.

    Returns: DataFrame: Transformed DataFrame.
    """
    clean_df = (
        df
        .withColumn("producer", substring_index(col("producer"), " ", 1))
        .withColumn("price", regexp_replace(col("price"), r"[^0-9]", "").cast(LongType()))
        .withColumn("year", col("year").cast(IntegerType()))
        .withColumn("mileage_unit", substring_index(col("mileage"), " ", -1))
        .withColumn(
            "mileage", regexp_replace(col("mileage"), r"[^0-9]", "").cast(LongType())
        )
        .filter(
            (col("producer").isNotNull())
        )
        .fillna("N/A")
    )
    return clean_df

def get_price_in_pln(cars_df: DataFrame, rates_df: DataFrame) -> DataFrame:
    """Assign PLN rate and create column with price in PLN.
    Args:
        cars_df (DataFrame): DataFrame with cars data.
        rates_df (DataFrame): DataFrame with rates.

    Returns: DataFrame: Returns dataframe with additional column price in PLN.
    """
    cars_columns = cars_df.columns
    df_with_rates = (
            cars_df.join(rates_df, [cars_df.price_currency == rates_df.code], "left_outer")
            .withColumn(
                "price_PLN", when(
                    col("price_currency") != "PLN",
                    col("mid") * col("price")
                ).otherwise(col("price"))
            )
        .select(*cars_columns, "price_PLN")
    )
    return df_with_rates

def save_data_to_parquet(df: DataFrame, partition_columns: list[str], destination_path: str):
    """Saves data to parquet files.

    Args: df (str): DataFrame with data to be saved.
        destination_path (DataFrame): Path to destination directory.
    """
    df.write.partitionBy(partition_columns).mode("overwrite").parquet(destination_path)
