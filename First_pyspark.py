from pyspark.sql import SparkSession

def main():

    sc = SparkSession.builder.appName("FirstApp").master("local").getOrCreate()
    df = sc.read.csv(r"C:\Users\Saurabh Singh\Desktop\Python Scripts\data_folder\bank\bank.csv",
                     sep=";",
                   header=True,
                   inferSchema=True)
    df.show()

#Latest Code Checkin

if __name__ == "__main__":
    main()