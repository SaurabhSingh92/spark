from pyspark.sql import SparkSession

if __name__ == '__main__':

    sc = SparkSession.builder.master("local").appName("Test Streaming").getOrCreate()

    df = sc.readStream.format('socket').option("host", 'localhost').option("port",'2222').load()

    df.writeStream.format('console').outputMode('append').start().awaitTermination()