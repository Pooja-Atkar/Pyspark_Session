from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == '__main__':

    spark = SparkSession.builder.appName("Pyspark DataFrame").master("local[*]").getOrCreate()

# Read "employee_detail.csv" File.

    # csv_Df = spark.read.csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\employee_detail.csv")
    # csv_Df.show()

# Read "employee_detail.csv" File

    # Df1 = spark.read.option("inferSchema","true").csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\employee_detail.csv")\
    #    .toDF("Id","Fname","Lname","Age","Gender","Deptno","Salary")
    # Df1.show()

# Read "employee_detail1.csv" File

    # Df2 = spark.read.option("inferSchema","true").csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\employee_details1.csv")\
    #    .toDF("Id","Fname","Lname","Age","Gender","Deptno","Salary")
    # Df2.show()

# Read "dept.csv" File.

    # Df3 = spark.read.option("header",True).csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\dept.csv")
    # Df3.show()

# 1. average salary per department
# DeptWiseSal = Df1.groupBy('Deptno').avg('Salary')
# DeptWiseSal.show()

# 4. Create a dataframe from existing rdd/existing collection using various ways
# 1.Using Parallelize
# data = [("ram","20000"),("Radha","50000"),("Ajay","30000")]
# file2 = spark.sparkContext.parallelize(data)
# print(file2.collect())

# 2.Using toDF
# rdd1 = file2.toDF()
# rdd1.printSchema()
# print(rdd1.collect())

# 3.Using Spark CreateDataFrame
# DataFrame1 = spark.createDataFrame(data)
# print(DataFrame1.collect())

# 5. Create a dataframe from csv file using various ways
# 1.Using Read CSV
# csv_Df = spark.read.csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\employee_detail.csv")
# csv_Df.show()

# csv_Df = spark.read.csv('employee_detail.csv')
# csv_Df.show()

# 2.Using Read Format
# csv_Df1 = spark.read.format("csv").load('employee_detail.csv')
# csv_Df1.show()

