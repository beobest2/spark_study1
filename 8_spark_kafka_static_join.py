from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp

"""
Cassandra
distributed NoSQL database
high availability, fault tolerance, scalability
high write and read throughput
"""
spark = (
    SparkSession.builder.appName("SparkStreaming")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .config(
        "spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions"
    )
    .config(
        "spark.sql.catalog.lh",
        "com.datastax.spark.connector.datasource.CassandraCatalog",
    )
    .getOrCreate()
)

schema = StructType(
    [StructField("create_date", StringType()), StructField("login_id", StringType())]
)

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "login_event")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

value_df = events.select(
    col("key"), from_json(col("value").cast("string"), schema).alias("value")
)

timestamp_format = "yyyy-MM-dd HH:mm:ss"
event_df = value_df.select("value.*").withColumn(
    "create_date", to_timestamp("create_date", timestamp_format)
)

user_df = (
    spark.read.format("org.apache.spark.sql.cassandra")
    .option("keyspace", "test_db")
    .option("table", "users")
    .load()
)

join_df = event_df.join(user_df, event_df.login_id == user_df.login_id, "inner").drop(
    user_df.login_id
)

output_df = join_df.select(
    col("login_id"), col("user_name"), col("create_date").alias("last_login")
)


def cassandra_writer(batch_df, batch_id):
    batch_df.write.format("org.apache.spark.sql.cassandra").option(
        "keyspace", "test_db"
    ).option("table", "users").mode("append").save()

    batch_df.show()


query = (
    output_df.writeStream.foreachBatch(cassandra_writer).outputMode("update").start()
)

query.awaitTermination()
