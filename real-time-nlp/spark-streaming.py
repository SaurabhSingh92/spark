
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import json



if __name__ == '__main__':


    spark = SparkSession.builder. \
        master('local[*]'). \
        appName('Kafka_tweet_sentiment').getOrCreate()

    sc = StreamingContext(spark, 10)

    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream. \
        format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('subscribe', 'twitter')\
        .load()

    df2=df.selectExpr("CAST(value AS STRING)")

    df2.writeStream.format("console").outputMode("append").start()
