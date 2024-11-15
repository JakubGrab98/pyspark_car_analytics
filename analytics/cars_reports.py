from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from analytics.cars_filter import CarsFilter
from const import *


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

    def get_advertises(self) -> DataFrame:
        advertises_df = (
            self.df.select(
                col(PRODUCER_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(PRODUCER_COLUMN)),
                col(MODEL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MODEL_COLUMN)),
                col(VERSION_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(VERSION_COLUMN)),
                col(MILEAGE_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MILEAGE_COLUMN)),
                col(MILEAGE_UNIT_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(MILEAGE_UNIT_COLUMN)),
                col(PRICE_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(PRICE_COLUMN)),
                col(URL_COLUMN).alias(REPORT_COLUMNS_MAPPING.get(URL_COLUMN)),
            ).transform(self.cars_filter.filter_by_all_parameters)
            .withColumn(MILEAGE_COLUMN, col(MILEAGE_COLUMN) + lit(" ") + col(MILEAGE_UNIT_COLUMN))
            .withColumn(PRICE_COLUMN, format_number(col(PRICE_COLUMN), 0) + lit(" PLN"))
            .drop(MILEAGE_UNIT_COLUMN)
        )
        return advertises_df