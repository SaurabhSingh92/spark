from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Trying Delta") \
        .master("local") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    from delta.tables import *

    df = spark.read.parquet(r"C:\Users\Saurabh Singh\Downloads\userdata1.parquet")
    df.show()

    df.write.format('delta').save(r"C:\Users\Saurabh Singh\Download")

    spark.stop()
