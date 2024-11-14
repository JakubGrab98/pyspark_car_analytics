from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class CarsFilter:

    PRODUCER_COLUMN: str = "producer"
    MODEL_COLUMN: str = "model"
    YEAR_COLUMN: str = "year"
    PRICE_COLUMN: str = "price_PLN"


    def __init__(
            self, producer: str, model: str, min_prod_year: int = 1900,
            max_prod_year: int = 2999, min_price: int = 0, max_price: int = 0
    ):
        self.producer = producer
        self.model = model
        self.min_prod_year = min_prod_year
        self.max_prod_year = max_prod_year
        self.min_price = min_price
        self.max_price = max_price

    def filter_by_producer(self, df: DataFrame) -> DataFrame:
        return df.filter(col(self.PRODUCER_COLUMN) == self.producer)

    def filter_by_model(self, df: DataFrame)-> DataFrame:
        return df.filter(
            (col(self.PRODUCER_COLUMN) == self.PRODUCER_COLUMN)
            & (col(self.MODEL_COLUMN) == self.model)
        )

    def filter_by_price(self, df: DataFrame) -> DataFrame:
        filtered_df = df.filter(
            (col(self.PRICE_COLUMN) >= self.min_price) &
            (self.max_price <= 0 | (col(self.PRICE_COLUMN) <= self.max_price))
        )
        return filtered_df

    def filter_by_year(self, df: DataFrame) -> DataFrame:
        filtered_df = df.filter(
            (col(self.YEAR_COLUMN) >= self.min_prod_year) &
            (col(self.YEAR_COLUMN) <= self.min_prod_year)
        )
        return filtered_df
