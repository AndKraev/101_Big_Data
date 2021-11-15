import pytest
from pyspark.sql import SparkSession

from hw_spark.task_1 import build_hotel_dataframe


@pytest.fixture
def train_df():
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
    return build_hotel_dataframe(spark, r"tests\hw_spark\train_100.csv")
