from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, IntegerType, StringType, DateType, TimestampType
from nltk.sentiment import SentimentIntensityAnalyzer
import json

def sentance_analyze(sent):
    score = SentimentIntensityAnalyzer()
    points = score.polarity_scores(f"{sent}")
    return points['compound']

if __name__ == '__main__':

    spark = SparkSession.builder. \
        master('local'). \
        appName('Kafka_tweet_sentiment')\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting the read")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('subscribe', 'tweet')\
        .option("partition.assignment.strategy", "range")\
        .option("startingOffsets", 'latest')\
        .load()

    df.printSchema()

    fake_schema = StructType().add("id", IntegerType())\
        .add("name", StringType()).add("city", StringType()).add("country", StringType())

    tweet_schema = StructType().add("id", StringType()).add("tweet", StringType())\
        .add("Creation_date", StringType()).add("UserName", StringType())

    score = udf(lambda v: sentance_analyze(v), StringType())

    df = df.selectExpr("CAST(value AS STRING)")\
        .select(from_json("value", tweet_schema).alias("tweet"))\
        .select("tweet.*").withColumn("Score", score('tweet'))

    query = df\
        .writeStream\
        .format("console") \
        .outputMode("append").trigger(processingTime='5 seconds')\
        .start()

    query.awaitTermination()

    print("Stream Data Processing Application Completed.")
