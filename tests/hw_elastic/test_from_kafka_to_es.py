from unittest.mock import Mock, call

from hw_elastic.from_kafka_to_es import read_kafka_stream_values, stream_write_to_es


def test_read_kafka_stream_values():
    mocked_spark = Mock()
    mocked_spark.readStream.format.return_value = mocked_spark
    mocked_spark.option.return_value = mocked_spark
    mocked_spark.load.return_value = mocked_spark
    mocked_spark.selectExpr.return_value = "fake dataframe"

    result = read_kafka_stream_values(mocked_spark, "host", "topic")

    mocked_spark.readStream.format.assert_called_once_with("kafka")
    mocked_spark.option.assert_has_calls(
        [
            call("kafka.bootstrap.servers", "host"),
            call("subscribe", "topic"),
            call("startingOffsets", "earliest"),
        ]
    )
    mocked_spark.load.assert_called_once()
    mocked_spark.selectExpr.assert_called_once_with("CAST(value AS STRING)")
    assert result == "fake dataframe"


def test_stream_write_to_es():
    mocked_df = Mock()
    mocked_df.writeStream.format.return_value = mocked_df
    mocked_df.outputMode.return_value = mocked_df
    mocked_df.option.return_value = mocked_df
    mocked_df.start.return_value = mocked_df

    stream_write_to_es(mocked_df, "node", "port", "res", "checkpoint")

    mocked_df.writeStream.format.assert_called_once_with("es")
    mocked_df.outputMode.assert_called_once_with("append")
    mocked_df.option.assert_has_calls(
        [
            call("es.nodes", "node"),
            call("es.port", "port"),
            call("es.index.auto.create", "true"),
            call("es.resource", "res"),
            call("checkpointLocation", "checkpoint"),
        ]
    )
    mocked_df.start.assert_called_once()
    mocked_df.awaitTermination.assert_called_once()
