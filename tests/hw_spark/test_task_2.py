import pandas as pd
from pandas.testing import assert_frame_equal

from hw_spark.task_2 import (
    most_popular_search_and_booked_country_with_agg,
    most_popular_search_and_booked_country_with_sort,
)


def test_most_popular_search_and_booked_country_with_agg(train_df):
    expected_data = {
        "user_location_country": (195,),
        "count": (2,),
    }
    expected_pandas = pd.DataFrame(data=expected_data)
    result_spark = most_popular_search_and_booked_country_with_agg(train_df).toPandas()
    assert_frame_equal(result_spark, expected_pandas, check_dtype=False)


def test_most_popular_search_and_booked_country_with_sort(train_df):
    expected_data = {
        "user_location_country": (195,),
        "count": (2,),
    }
    expected_pandas = pd.DataFrame(data=expected_data)
    result_spark = most_popular_search_and_booked_country_with_sort(train_df).toPandas()
    assert_frame_equal(result_spark, expected_pandas, check_dtype=False)
