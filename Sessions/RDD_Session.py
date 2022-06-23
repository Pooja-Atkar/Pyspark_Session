from Sessions.sparkbasics import spark

# Parellelized Collection(RDD)
# Create RDD on List Collection.

# lst = [1,2,3,4,5]
# # print(lst)
# # [1, 2, 3, 4, 5]
# rdd1 = spark.sparkContext.parallelize(lst)
# print(rdd1)
# # ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
# print(rdd1.collect())
# # [1, 2, 3, 4, 5]
#
# firstadd = rdd1
# print(firstadd.collect())
# # [1, 2, 3, 4, 5]

# Create RDD on Tuple Collection.

# input1 = (('red',1),('green',2),('blue',3))
# print(type(input1))
# # <class 'tuple'>
# tuplerdd = spark.sparkContext.parallelize(input1)
# print(tuplerdd)
# # ParallelCollectionRDD[1] at readRDDFromFile at PythonRDD.scala:274
# print(tuplerdd.collect())
# # [('red', 1), ('green', 2), ('blue', 3)]
#

# From Other RDDs
# lst = [1,2,3,4,5]
# print(lst)
# # [1, 2, 3, 4, 5]
# rdd1 = spark.sparkContext.parallelize(lst)
# print(rdd1)
# # ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
# print(rdd1.collect())
# # [1, 2, 3, 4, 5]
# # firstadd = rdd1
# # print(firstadd.collect())
# # [1, 2, 3, 4, 5]
# # print(rdd1.collect())
# # [1, 2, 3, 4, 5]
# rdd2 = rdd1.map(lambda x:(x,1))
# print(rdd2.collect())
# [(1, 1), (2, 1), (3, 1), (4, 1), (5, 1)


# From Other RDDs

# lst1 = ["red","green","blue","yellow"]
# print(lst1)
# # # ['red', 'green', 'blue', 'yellow']
# inputadd = spark.sparkContext.parallelize(lst1)
# print(inputadd.collect())
# # # ['red', 'green', 'blue', 'yellow']
# # Fetch Specific Key
# print(inputadd.filter(lambda x: 'red' in x).collect())
# ['red']

# maprdd = inputadd.map(lambda x:(x,1))
# print(maprdd.collect())
# # [('red', 1), ('green', 1), ('blue', 1), ('yellow', 1)]



# # Using External File

# inputfilerdd = spark.sparkContext.textFile("D:\\PythonSparkProject\\Pyspark_Session\\SampleCSVFile.csv")
# print(inputfilerdd)
# print(inputfilerdd.collect())
# print("------------------")

# filemaprdd = inputfilerdd.map(lambda y:(y,1))
# print(filemaprdd.collect())
# print("------------------")

# fileflatmap = inputfilerdd.flatMap(lambda x:x.split(" "))
# print(fileflatmap.collect())
# print("------------------")

# filemaprdd = fileflatmap.map(lambda z:(z,1))
# print(filemaprdd.collect())
# print("------------------")

#  Count Word
# wordcoutrdd = filemaprdd.reduceByKey(lambda x,y : x + y)
# print(wordcoutrdd.collect())
# print("------------------")

# Find 'storage' Word in 'ampleCSVFile File'
# print(wordcoutrdd.filter(lambda x: 'storage' in x[0].lower()).collect())


# # Empty RDD
# emptyrdd1 = spark.sparkContext.emptyRDD
# print(emptyrdd1)
# # <bound method SparkContext.emptyRDD of <SparkContext master=local[*] appName=PySparkShell>>
# print(emptyrdd1.collect())





