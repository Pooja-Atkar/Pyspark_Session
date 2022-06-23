from pyspark.sql import SparkSession

# Read CSV File
# spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
# print(spark)
# demo = spark.read.csv("inputfile.txt")
# demo.show()

# Gender wise Count

spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
# inputrdd = spark.sparkContext.textFile("inputfile.txt")
# outputrdd = (inputrdd.map(lambda x:x.split(","))
#              .map(lambda x: (x[4],1))
#              .reduceByKey(lambda x,y: x+y))
# print(outputrdd.collect())

print("---------Another Way------------")
# data_split = inputrdd.map(lambda x:x.split(","))
# print(sum([1 for x in data_split.collect() if x=='M']),"Male")
# print(sum([1 for x in data_split.collect() if x=='F']),"Female")

print("---------Another Way------------")
# inputrdd = spark.sparkContext.textFile("inputfile.txt")
# input_split = inputrdd.flatMap(lambda x: x.split(","))
# gender_filterrdd = input_split.filter(lambda x: x in ('M','F'))
# gender_maprdd = gender_filterrdd.map(lambda x:(x,1))
# print(gender_maprdd.reduceByKey(lambda x,y : x+y).collect())

# Sum of Salary per Department

# print(inputrdd.map(lambda x:x.split(","))
#  .map(lambda x:(x[5],int(x[6])))
#  .reduceByKey(lambda x,y:x+y).collect())