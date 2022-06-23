
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Session Init")\
        .master("local[*]").getOrCreate()

# 1. average salary per department.

inputrdd = spark.sparkContext.textFile(r"D:\PythonSparkProject\Pyspark_Session\RDD_Assignment3\employee_detail.txt")
print(inputrdd.collect())
# deptsalrdd = inputrdd.map(lambda x: x.split(","))\
#               .map(lambda x: (x[5],int(x[6])))\
#               .groupByKey().mapValues(lambda x:sum(x)/len(x))
#
# print(deptsalrdd.collect())


# 2. provide employee details who has second highest salary.

# inputrdd = spark.sparkContext.textFile(r"D:\PythonSparkProject\Pyspark_Session\RDD_Assignment3\employee_detail.txt")
# secondHighestSal = min((inputrdd.map(lambda x :x.split(","))
#                         .map(lambda x:int(x[6]))).distinct()
#                        .takeOrdered(2, key=lambda x:-x))
# empDetailsRdd = (inputrdd.map(lambda x:x.split(","))
#                  .filter(lambda x:int(x[6]) == secondHighestSal))
# print(empDetailsRdd.collect())

# 3. retrieve employee_details with unique records from employee_detail.
# txt and employee_details1.txt and store them in different file.

# empDetailsrdd = spark.sparkContext.textFile("employee_detail.txt")
# empDetailsrdd1 = spark.sparkContext.textFile("emp_details1.txt")
# unionrdd = empDetailsrdd.union(empDetailsrdd1).distinct()
# print(unionrdd.collect())
# unionrdd.saveAsTextFile('Union_rdd.txt')

# 4. retrieve employee_details with Common records from employee_detail.
# txt and employee_details1.txt and store them in different file.

# empDetailsrdd = spark.sparkContext.textFile("employee_detail.txt")
# empDetailsrdd1 = spark.sparkContext.textFile("emp_details1.txt")
# emp = empDetailsrdd.map(lambda x: x.split(","))
# emp1 = empDetailsrdd1.map(lambda x: x.split(","))
# a = emp1.collect()
# res =emp.filter(lambda x :x in a)
# print(res.collect())