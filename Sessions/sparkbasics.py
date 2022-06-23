from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("hello class...")

    # sparkconf =SparkConf().setAppName("Spark Context Init").setMaster("local[*]")
    # sc = SparkContext(conf=sparkconf)
    # print(sc)

spark = SparkSession.builder.appName("Spark Context Init").master("local[*]").getOrCreate()
print(spark)