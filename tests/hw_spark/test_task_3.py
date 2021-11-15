import pandas as pd
from pandas.testing import assert_frame_equal

from hw_spark.task_3 import top_3_hotels_with_children_not_booked


def test_top_3_hotels_with_children_not_booked(train_df):
    expected_data = {
        "hotel_continent": (3, 2, 2),
        "hotel_country": (151, 50, 50),
        "hotel_market": (69, 680, 406),
        "count": (3, 2, 1),
    }
    expected_pandas = pd.DataFrame(data=expected_data)
    result_spark = top_3_hotels_with_children_not_booked(train_df).toPandas()
    assert_frame_equal(expected_pandas, result_spark, check_dtype=False)
