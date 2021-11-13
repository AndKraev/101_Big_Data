from unittest.mock import Mock

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pyspark.sql.types import (
    ByteType,
    DateType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from hw_spark.task_1 import *


@pytest.fixture
def train_df():
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
    return build_hotel_dataframe(spark, r"tests\hw_spark\train_100.csv")


def test_build_hotel_dataframe():
    expected_schema = StructType(
        [
            StructField("date_time", TimestampType()),
            StructField("site_name", IntegerType()),
            StructField("posa_continent", ByteType()),
            StructField("user_location_country", IntegerType()),
            StructField("user_location_region", IntegerType()),
            StructField("user_location_city", IntegerType()),
            StructField("orig_destination_distance", DoubleType()),
            StructField("user_id", IntegerType()),
            StructField("is_mobile", ByteType()),
            StructField("is_package", ByteType()),
            StructField("channel", IntegerType()),
            StructField("srch_ci", DateType()),
            StructField("srch_co", DateType()),
            StructField("srch_adults_cnt", ByteType()),
            StructField("srch_children_cnt", ByteType()),
            StructField("srch_rm_cnt", IntegerType()),
            StructField("srch_destination_id", IntegerType()),
            StructField("srch_destination_type_id", IntegerType()),
            StructField("is_booking", ByteType()),
            StructField("cnt", IntegerType()),
            StructField("hotel_continent", ByteType()),
            StructField("hotel_country", IntegerType()),
            StructField("hotel_market", IntegerType()),
            StructField("hotel_cluster", IntegerType()),
        ]
    )
    mocked_session = Mock()
    filename = "fake_file.csv"
    build_hotel_dataframe(mocked_session, filename)
    assert mocked_session.read.csv.call_args[0][0] == filename
    assert mocked_session.read.csv.call_args[1]["schema"] == expected_schema
    assert mocked_session.read.csv.call_args[1]["sep"] == ","
    assert mocked_session.read.csv.call_args[1]["header"]


def test_show_top_hotels_between_couples(train_df):
    expected_data = {
        "hotel_continent": (3, 2, 2),
        "hotel_country": (151, 50, 50),
        "hotel_market": (69, 680, 191),
        "count": (37, 10, 9),
    }
    expected_pandas = pd.DataFrame(data=expected_data)
    result_spark = show_top_hotels_between_couples(train_df, 3).toPandas()
    assert_frame_equal(result_spark, expected_pandas, check_dtype=False)
