from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType, FloatType, DataType, TimestampType

typeDict={
    'string':'StringType()',
    'int':'IntegerType()',
    'float':'FloatType()',
    'double':'FloatType()',
    'date': 'DataType()',
    'timestamp': 'TimestampType()'
}

def main():
    spark = SparkSession.builder.appName("Test_parquet").master("local").getOrCreate()
    df = spark.read.parquet(r"C:\Users\Saurabh Singh\Downloads\userdata1.parquet")
    pk = ['id']
    schema = ''
    #main code to start the schema creation
    for name, type in df.dtypes:
        nullchk = True if name in pk else False
        typeChk = typeDict[type]
        schema = f"{schema}"+ f"\n" + f"StructField('{name}',{typeChk},{nullchk}),"

    print(schema)


if __name__ == '__main__':
    main()
