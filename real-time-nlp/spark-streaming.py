from pyspark.sql import SparkSession
import json

if __name__ == '__main__':

    spark = SparkSession.builder. \
        master('local[*]'). \
        appName('Kafka_tweet_sentiment').getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting the read")

    df = spark.readStream.format("kafka") \
        .option('partition.assignment.strategy', 'range').option("auto.offset.reset", 'largest')\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('subscribe', 'sample').option("group.id", "test").option("startingOffsets", 'latest')\
        .load()

    new=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    print(f"Read is streaming: \b{df.isStreaming}")

    new.writeStream.trigger(processingTime='5 seconds').outputMode("append").format("console")\
        .option("truncate", 'false') \
        .start().awaitTermination()


