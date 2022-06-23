from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from Sessions.sparkbasics import spark

# Read CSV(Comma Separated File) from disk.
spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
csv_file =spark.read.csv("D:\\PythonSparkProject\\Pyspark_Session\\SampleCSVFile.csv")
print(csv_file.show())

# Count Words in the File.
csv_filerdd = spark.sparkContext.textFile("D:\\PythonSparkProject\\Pyspark_Session\\SampleCSVFile.csv")
csv_flatmap = csv_filerdd.flatMap(lambda x:x.split(","))
csv_maprdd = csv_flatmap.map(lambda x:(x,1))
print(csv_maprdd.reduceByKey(lambda x,y:x+y).collect())



# Create RDD on Dict.
# 1. create rdd using parallelize collection [{"a":1},{"b":2},{"c":1},{"d":4}]

# dic = [{"a":1},{"b":2},{"c":1},{"d":4}]
# print(type(dic))
# print(dic)
# dicrdd = spark.sparkContext.parallelize(dic)
# print(dicrdd)
# print(dicrdd.collect())

# 2. retrive element from above rdd, having value as 1 and 2
# dic = [{"a":1},{"b":2},{"c":1},{"d":4}]
# Firstrdd = spark.sparkContext.parallelize(dic)
# print(Firstrdd)
# print(Firstrdd.collect())
# print(Firstrdd.filter(lambda x: "a" in x).collect())
# print(Firstrdd.filter(lambda x: "b" in x).collect())
# print(Firstrdd.filter(lambda x: "c" in x).collect())


# 3.create an a new rdd based on above one by multiplying 25 in value section
# my_list = [1,5,4,6,8,11,3,12]
# inputrdd = spark.sparkContext.parallelize(my_list)
# print(inputrdd)
# print(inputrdd.collect())
# print(inputrdd.map(lambda x: (x * 25)).collect())

# 4.create a new rdd from and retrive even number values only
# my_list = [1,5,4,6,8,11,3,12]
# Nordd = spark.sparkContext.parallelize(my_list)
# EvenNordd = Nordd.filter(lambda x: x % 2 == 0)
# print(EvenNordd.collect())

