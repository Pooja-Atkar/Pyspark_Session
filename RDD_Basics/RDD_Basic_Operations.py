from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Basic Operations").master("local[*]").getOrCreate()

names = spark.sparkContext.parallelize(['Adam','Cray','Shaun','Brain','Mark','Christ','Shail','Satya','Mark','Norby','Frans','Mark','Bill'])
# print(type(names))
# print(names.collect())
# print(names.countByValue())


# foreach is an action,it takes each element and applies a function,but it does not return a value.
# This is particularly useful in you have to call perform some calculations on an RDD and log the result somewhere else.
# for example a database or call a REST API with each element in the RDD.

# def f(x): print(x)
# a = spark.sparkContext.parallelize([1,2,3,4,5]).foreach(lambda x:print(x))
# print(type(a))
# print(a.collect())

# Create RDD using File
# emprdd = spark.sparkContext.textFile(r"D:\PythonSparkProject\Pyspark_Session\RDD_Basics\emp_data")
# print(emprdd.collect())


num = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# print(num.collect())
# print(type(num))
# print(num.first())
# print(num.count())
# print(num.distinct().count())
# print(num.glom().collect())
# print(num.countByValue())

num1 = spark.sparkContext.parallelize([5,5,4,3,2,9,2],2)
# print(num1.collect())
# print(num1.glom().collect())
# print(num1.take(4))
# print(num1.glom().collect()[1:])
# print(num1.max())
# print(num1.min())
# print(num1.mean())
# print(num1.reduce(lambda a,b:a+b))
# print(num1.reduce(lambda a,b:a*b))
# print(num1.reduce(lambda x,y:x if x > y else y))

# def myfun(a,b):
#     return a*2 + b*2
# print(num1.reduce(myfun))

# print(num1.takeOrdered(3))

# Fold: The initial value for the accumulated result of each partition for the op operator,
# and also the initial value for the combine results from different partitions.

# print(num1.fold(2,lambda a,b : a*b))
# print(num1.fold(1,lambda a,b : a+b))

# Create RDD using Operator
from operator import add,mul
# b = spark.sparkContext.parallelize([1,2,3,4,5])
# print(b.fold(1,add))

# num3 = spark.sparkContext.parallelize([5,5,4,3,2,9,2]).fold(10,mul)
# print(num3)

# Create RDD using range()
# nordd = spark.sparkContext.parallelize(range(1,10))
# print(nordd.collect())

# Narrow Transformation
# .map Function
no = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# print(no.collect())
# print(no.map(lambda a: a*2).collect())
# print(no.map(lambda a: pow(a,2)).collect())

namesrdd = spark.sparkContext.parallelize(["Bills","Mark","Brain","Mick"])
# print(namesrdd.map(lambda a: "Mr."+a).collect())

# .flatMap Function
flatmaprdd = spark.sparkContext.parallelize([2,3,4])
# print(flatmaprdd.collect())

# a = range(1,3)
# for i in a:
#     print(i)

# print(flatmaprdd.flatMap(lambda x: range(1,x)).collect())

# flatmaprdd1 =spark.sparkContext.parallelize([1,2,3])
# b = flatmaprdd1.flatMap(lambda x: x*10.57)
# print(flatmaprdd1.collect())

# .filter
filterrdd = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# print(filterrdd.collect())
# print(filterrdd.filter(lambda x: x%2 == 0).collect())

# namesrdd = spark.sparkContext.parallelize(["Bills","Mark","Brain","Mick"])
# print(namesrdd.collect())
# print(namesrdd.filter(lambda x : "B" in x).collect())

# .union
# no = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# no1 = spark.sparkContext.parallelize([1,7,9,4,10,15])
# print(no.collect())
# print(no1.collect())
# print(no.union(no1).collect())

# x = spark.sparkContext.parallelize([1,2,3],2)
# print(x.collect())
# y = spark.sparkContext.parallelize([3,4],1)
# print(y.collect())
# z = x.union(y)
# print(z.collect())

# sample
# sample: Return a random sample
# subset RDD of the input RDD.
# API: (withReplacement:Boolean,fraction:Double,seed:Long = utils.random.nextLong):RDD[T]
# Note This is not guaranteed to provide exactly the fraction specified of the total count of the given.

parallelrdd = spark.sparkContext.parallelize(range(1,10))
# print(parallelrdd.collect())
# print(parallelrdd.sample(True,.2,seed = 19).collect())
# print(parallelrdd.sample(True,.2).collect())

# wide Transformation
# GroupBy
# namesrdd = spark.sparkContext.parallelize(["Bills","Mark","Brain","Mick"])
# print(namesrdd.collect())
# groupbyrdd = namesrdd.groupBy(lambda x:x[0])
# print(groupbyrdd.collect())
# for (k,v) in groupbyrdd.collect():
#     print(k,list(v))

# groupbyrdd1 = spark.sparkContext.parallelize([1,1,2,3,5,8])
# result = groupbyrdd1.groupBy(lambda x: x % 2)
# print(result.collect())
# for (k,v) in result.collect():
#     print(k,list(v))

# intersection
no = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# print(no.collect())
no1 = spark.sparkContext.parallelize([1,7,9,4,10,15])
# print(no1.collect())
# print(no.intersection(no1).collect())
# print(no1.intersection(no).collect())
# print(no.subtract(no1).collect())
# print(no1.subtract(no).collect())

# Distinct
# no = spark.sparkContext.parallelize([5,5,4,3,2,9,2])
# print(no.collect())
# print(no.distinct().collect())