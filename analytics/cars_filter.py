"""Module responsible for filtering DataFrame for reporting purposes."""
from pyspark.sql.functions import *
from const import (
    PRODUCER_COLUMN,
    MODEL_COLUMN,
    YEAR_COLUMN,
    PRICE_COLUMN,
)


class CarsFilter:

    def __init__(
            self, producer: str, model: str, min_prod_year: int = 1900,
            max_prod_year: int = 2999, min_price: int = 0, max_price: int = 0
    ):
        """
        Initializes CarsFilter class.
        :param producer: Producer name.
        :param model: Model name.
        :param min_prod_year: Minimum production year.
        :param max_prod_year: Maximum production year.
        :param min_price: Minimum price amount.
        :param max_price: Maximum price amount.
        """
        self.producer = producer
        self.model = model
        self.min_prod_year = min_prod_year
        self.max_prod_year = max_prod_year
        self.min_price = min_price
        self.max_price = max_price

    def filter_by_producer(self, df: DataFrame) -> DataFrame:
        """
        Filters DataFrame by producer.
        :param df: Car's DataFrame.
        :return: Filtered DataFrame by producer.
        """
        return df.filter(col(PRODUCER_COLUMN) == self.producer)

    def filter_by_model(self, df: DataFrame)-> DataFrame:
        """
        Filters DataFrame by model
        :param df: Car's DataFrame.
        :return: Filtered DataFrame by model.
        """
        return df.filter(
            (col(PRODUCER_COLUMN) == self.producer)
            & (col(MODEL_COLUMN) == self.model)
        )

    def filter_by_price(self, df: DataFrame) -> DataFrame:
        """
        Filters DataFrame by price.
        :param df: Car's DataFrame.
        :return: Filtered DataFrame by minimum and maximum price.
        """
        filtered_min_price = df.filter(col(PRICE_COLUMN) >= self.min_price)
        if self.max_price > 0 and self.max_price > self.min_price:
            filtered_max_price = filtered_min_price.filter(col(PRICE_COLUMN) <= self.min_price)
            return filtered_max_price
        return filtered_min_price

    def filter_by_year(self, df: DataFrame) -> DataFrame:
        """
        Filters DataFrame by production year.
        :param df: Car's DataFrame.
        :return: Filtered DataFrame by minimum and maximum production year.
        """
        filtered_df = df.filter(
            (col(YEAR_COLUMN) >= self.min_prod_year) &
            (col(YEAR_COLUMN) <= self.max_prod_year)
        )
        return filtered_df

    def filter_by_all_parameters(self, df: DataFrame) -> DataFrame:
        """
        Filters DataFrame by all parameters.
        :param df: Car's DataFrame.
        :return: Filtered DataFrame by all parameters.
        """
        filtered_df = (
            df.transform(self.filter_by_model)
            .transform(self.filter_by_price)
            .transform(self.filter_by_year)
        )
        return filtered_df
