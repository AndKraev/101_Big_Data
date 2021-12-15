from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def read_kafka_stream_values(spark: SparkSession, host: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(col("value").cast("string"))
    )


def stream_write_to_es(
    df: DataFrame, nodes: str, port: str, resource: str, checkpoint_path: str
) -> None:
    (
        df.writeStream.format("es")
        .outputMode("append")
        .option("es.nodes", nodes)
        .option("es.port", port)
        .option("es.resource", resource)
        .option("checkpointLocation", checkpoint_path)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    import sys

    sys.path.append("/mnt/d/PyProjects/101_Big_Data/")
    from hw_streaming.Kafka_Spark import df_from_col_with_jsons, get_json_schema

    spark = SparkSession.builder.master("local[*]").appName("task_1").getOrCreate()
    kafka_host = "[::1]:9092"
    kafka_topic = "hotels-100"

    kafka_df = read_kafka_stream_values(spark, kafka_host, kafka_topic)
    json_df = df_from_col_with_jsons(
        kafka_df, schema=get_json_schema(spark, kafka_host, kafka_topic)
    )
    stream_write_to_es(
        json_df,
        nodes="localhost",
        port="9200",
        resource="hotels/json",
        checkpoint_path="/tmp/",
    )
