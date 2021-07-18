import pandas as pd
import yfinance as yf
from matplotlib import pyplot as plt
from pyspark.sql import SparkSession
from matplotlib.animation import FuncAnimation


def get_data():
    df = yf.download(tickers='UBER', period='1h', interval='1h')
    return df


def main():
    spark = SparkSession.builder.master("local").appName("Stock").getOrCreate()
    data = get_data()
    plt.figure(data.index, data['Open'])
    ani = FuncAnimation(plt, get_data, interval=10)
    plt.show()


if __name__ == '__main__':
    main()
