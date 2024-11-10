"""Module responsible for retrieving NBP Polish currency rates"""
from pyspark.sql import SparkSession, DataFrame
import requests


class Rates:
    def __init__(self, base_url: str, spark_session: SparkSession):
        """Initializes rates class.
        Args: base_url (str): NPB base url API.
            spark_session (SparkSession)
        """
        self.base_url = base_url
        self.spark_session = spark_session

    def get_rates(self, table: str) -> DataFrame | None:
        """Retrieves dataframe with rates in Polish currency.
        Args: table (str): NBP table type.

        Returns: DataFrame: Returns dataframe with effective currency rates.
        """
        url = f"{self.base_url}/{table}/"
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            rates = response_json[0]["rates"]
            rates_df = self.spark_session.createDataFrame(rates)
            return rates_df
