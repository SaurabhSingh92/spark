from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder. \
        master('local'). \
        appName('Kafka_tweet_sentiment')\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("Starting the read")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option('subscribe', 'twitter')\
        .option("partition.assignment.strategy", "range")\
        .option("startingOffsets", 'earliest')\
        .load()

    df.printSchema()

    query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\
        .writeStream\
        .format("console") \
        .outputMode("append")\
        .trigger(processingTime='5 seconds')\
        .start()

    query.awaitTermination()

    print("Stream Data Processing Application Completed.")
