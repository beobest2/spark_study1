from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = (
    SparkSession.builder.appName("SparkStreaming")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "quickstart")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

events.printSchema()

words = events.select(explode(split(events.value, " ")).alias("word"))
words_count = words.groupBy("word").count()

query = (
    words_count.writeStream.option("checkpointLocation", "checkpoint")
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()
