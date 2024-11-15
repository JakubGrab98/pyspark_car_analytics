from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from analytics.cars_filter import CarsFilter
from const import (
    PRODUCER_COLUMN,
    MODEL_COLUMN,
    YEAR_COLUMN,
    PRICE_COLUMN,
)


class CarReport:
    def __init__(self, df: DataFrame, cars_filter: CarsFilter):
        self.df = df
        self.cars_filter = cars_filter

    def get_number_of_producer_advertises(self) -> int:
        base_df = self.df.select(PRODUCER_COLUMN)
        result = self.cars_filter.filter_by_all_parameters(base_df).count()
        return result

    def get_model_statistics(self) -> DataFrame:
        statistics_df = (
            self.df.select(PRODUCER_COLUMN, MODEL_COLUMN, YEAR_COLUMN, PRICE_COLUMN)
            .transform(self.cars_filter.filter_by_all_parameters)
            .groupby(PRODUCER_COLUMN, MODEL_COLUMN)
            .agg(
                max(YEAR_COLUMN).alias("max_prod_year"),
                min(YEAR_COLUMN).alias("min_prod_year"),
                avg(PRICE_COLUMN).alias("avg_price"),
                max(PRICE_COLUMN).alias("max_price"),
                min(PRICE_COLUMN).alias("min_price"),
            )
        )
        return statistics_df
