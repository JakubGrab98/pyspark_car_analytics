"""Module responsible for retrieving NBP Polish currency rates"""
from pyspark.sql import SparkSession, DataFrame
import requests


class Rates:
    def __init__(self, base_url: str, spark_session: SparkSession):
        self.base_url = base_url
        self.spark_session = spark_session

    def get_rates_json(self, table: str, currency: str) -> DataFrame | None:
        """Retrieves dataframe with rates in Polish currency.
        Args: table (str): NBP table type.
            currency (str): Currency 3-digit code.

        Returns: DataFrame: Returns dataframe with daily currency rates.
        """
        params = {
            "table": table,
            "currency": currency.lower(),
        }
        response = requests.get(self.base_url, params=params)
        if response.status_code == 200:
            response_json = response.json()
            rates_df = self.spark_session.read.json(response_json)
            return rates_df
