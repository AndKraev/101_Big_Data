from unittest.mock import Mock, call, patch

import pandas as pd

from hw_streaming.Kafka_Spark import *


def test_df_from_col_with_jsons(spark_session):
    d = [{"value": '{"first": "one", "second": "two"}'}]
    df = spark_session.createDataFrame(d)
    schema = StructType(
        [StructField("first", StringType()), StructField("second", StringType())]
    )
    actual = df_from_col_with_jsons(df, schema).toPandas()
    expected = pd.DataFrame({"first": ["one"], "second": ["two"]})
    pd.testing.assert_frame_equal(actual, expected)


def test_get_json_schema(spark_session):
    # Mock SparkSession
    mocked_spark = Mock()
    mocked_spark.read.format.return_value = mocked_spark
    mocked_spark.option.return_value = mocked_spark
    df = spark_session.createDataFrame([{"value": '{"first": "one", "second": "two"}'}])
    mocked_spark.load.return_value = df

    # Call testing function and assert the result
    actual = get_json_schema(mocked_spark, "fake_host", "fake_topic")
    expected = StructType(
        [StructField("first", StringType()), StructField("second", StringType())]
    )
    assert actual == expected

    # Assert Spark session calls
    mocked_spark.read.format.assert_called_once_with("kafka")
    option_calls = [
        call("kafka.bootstrap.servers", "fake_host"),
        call("subscribe", "fake_topic"),
        call("startingOffsets", "earliest"),
        call("endingOffsets", """{"fake_topic":{"0":2}}"""),
    ]
    mocked_spark.option.assert_has_calls(option_calls)


@patch("hw_streaming.Kafka_Spark.col")
@patch("hw_streaming.Kafka_Spark.df_from_col_with_jsons")
def test_batch_read_and_write(mocked_df_from_col_with_jsons, mocked_col):
    # Mock SparkSession
    mocked_spark = Mock()
    mocked_spark.read.format.return_value = mocked_spark
    mocked_spark.option.return_value = mocked_spark
    mocked_spark.load.return_value = mocked_spark

    # Mock select
    mocked_kafka_df = Mock()
    mocked_spark.select.return_value = mocked_kafka_df

    # Mock Spark function col
    mocked_col.return_value = mocked_col
    mocked_col.cast.return_value = mocked_col

    # Mock function df_from_col_with_jsons
    mocked_df = Mock()
    mocked_df_from_col_with_jsons.return_value = mocked_df

    # Call testing function
    batch_read_and_write(
        mocked_spark, "fake_host", "fake_topic", "fake_path", "faked_schema"
    )

    # Assert Spark session calls
    mocked_spark.read.format.assert_called_once_with("kafka")
    option_calls = [
        call("kafka.bootstrap.servers", "fake_host"),
        call("subscribe", "fake_topic"),
        call("startingOffsets", "earliest"),
    ]
    mocked_spark.option.assert_has_calls(option_calls)

    # Assert calls functions col, col.cast and select
    mocked_col.assert_called_once_with("value")
    mocked_col.cast.assert_called_once_with("string")
    mocked_spark.select(mocked_col)

    # Assert call function mocked_df_from_col_with_jsons
    mocked_df_from_col_with_jsons.assert_called_once_with(
        mocked_kafka_df, "faked_schema"
    )

    # Assert json write call was done
    mocked_df.write.json.assert_called_once_with("fake_path")


@patch("hw_streaming.Kafka_Spark.col")
@patch("hw_streaming.Kafka_Spark.df_from_col_with_jsons")
def test_stream_read_and_write(mocked_df_from_col_with_jsons, mocked_col):
    # Mock SparkSession
    mocked_spark = Mock()
    mocked_kafka_df = Mock()
    mocked_spark.readStream.format.return_value = mocked_spark
    mocked_spark.option.return_value = mocked_spark
    mocked_spark.load.return_value = mocked_spark

    # Mock select
    mocked_kafka_df = Mock()
    mocked_spark.select.return_value = mocked_kafka_df

    # Mock coll, cast and select spark functions
    mocked_col.return_value = mocked_col
    mocked_col.cast.return_value = mocked_col

    # Mock function df_from_col_with_jsons
    mocked_df = Mock()
    mocked_df_from_col_with_jsons.return_value = mocked_df

    # Mock writeStream
    mocked_df.writeStream.format.return_value = mocked_df
    mocked_df.option.return_value = mocked_df
    mocked_df.start.return_value = mocked_df

    # Call testing function
    stream_read_and_write(
        mocked_spark, "fake_host", "fake_topic", "fake_path", "faked_schema"
    )

    # Assert call col, coll.cast and select
    mocked_col.assert_called_once_with("value")
    mocked_col.cast.assert_called_once_with("string")
    mocked_spark.select(mocked_col)

    # Assert Spark session calls
    mocked_spark.readStream.format.assert_called_once_with("kafka")
    option_calls = [
        call("kafka.bootstrap.servers", "fake_host"),
        call("subscribe", "fake_topic"),
        call("startingOffsets", "earliest"),
    ]
    mocked_spark.option.assert_has_calls(option_calls)

    # Assert call function mocked_df_from_col_with_jsons
    mocked_df_from_col_with_jsons.assert_called_once_with(
        mocked_kafka_df, "faked_schema"
    )

    # Assert stream writing
    mocked_df.writeStream.format.assert_called_once_with("json")
    option_calls = [
        call("checkpointLocation", "fake_path/checkpointLocation"),
        call("path", "fake_path"),
    ]
    mocked_df.option.assert_has_calls(option_calls)
    mocked_df.start.assert_called_once()
    mocked_df.awaitTermination.assert_called_once()
