from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField

schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), False)
    ]
)


def main(args):
    mode = args[0]
    spark = SparkSession.builder.appName("Test_parquet").master("local").getOrCreate()
    df = spark.read.parquet(r"C:\Users\Saurabh Singh\Downloads\userdata1.parquet") \
        .select("id", "first_name", "last_name").rdd

    if len(mode) == 1:
        print("Mode B")
        df2 = spark.read.parquet(r"C:\Users\Saurabh Singh\Downloads\userdata1.parquet")
    else:
        print("Mode A")
        df2 = spark.createDataFrame(df, schema=schema)

    df2.printSchema()
    df2.show()
    spark.stop()


if __name__ == '__main__':
    x=input("Please Enter:")
    main('')
