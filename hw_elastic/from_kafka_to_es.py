import sys

from pyspark.sql import DataFrame, SparkSession


def read_kafka_stream_values(spark: SparkSession, host: str, topic: str) -> DataFrame:
    """Reads Kafka topic with streaming and returns a dataframe

    :param spark: SparkSession object
    :param host: kafka address and port
    :param topic: kafka topic name
    :return: dataframe from kafka with selected value column as string
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
    )


def stream_write_to_es(
    df: DataFrame, nodes: str, port: str, resource: str, checkpoint_path: str
) -> None:
    """Stream writes a dataframe to elasticsearch index

    :param df: a target dataframe to write
    :param nodes: elasticsearch address
    :param port: elasticsearch port
    :param resource: elasticsearch index and type
    :param checkpoint_path: path to a checkpoint directory
    :return: no return
    """
    (
        df.writeStream.format("es")
        .outputMode("append")
        .option("es.nodes", nodes)
        .option("es.port", port)
        .option("es.index.auto.create", "true")
        .option("es.resource", resource)
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    sys.path.append("/mnt/d/PyProjects/101_Big_Data/")
    from hw_streaming.Kafka_Spark import df_from_col_with_jsons, get_json_schema

    kafka_host = "[::1]:9092"
    kafka_topic = "hotels-3m"
    es_host = "localhost"
    es_port = "9200"
    es_index = "hotels-3m"
    checkpoint_path = "/tmp/myelastic"
    spark = SparkSession.builder.master("local[*]").appName("task_1").getOrCreate()

    kafka_df = read_kafka_stream_values(spark, kafka_host, kafka_topic)
    schema = get_json_schema(spark, kafka_host, kafka_topic)
    json_df = df_from_col_with_jsons(kafka_df, schema)
    stream_write_to_es(
        json_df,
        nodes=es_host,
        port=es_port,
        resource=es_index + "/_doc",
        checkpoint_path=checkpoint_path,
    )
