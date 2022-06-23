from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("DataFrame Assignment").master("local[*]").getOrCreate()
# Q1. Create a Dataframe  and write a dataframe values using  csv,json,orc,parquet file format and try this with various ways.

# Create Dataframe Using Data
data = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
df = spark.createDataFrame(data=data,schema=schema)
# df.printSchema()
# df.show()

# Create Dataframe using File
# schemadf = spark.read.csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\EmpData.csv",schema=schema)
# schemadf.printSchema()
# schemadf.show()

# write dataframe in CSV file
# schemadf.write.csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\csvfile",sep="\\t")

# Read or Create Dataframe from csv file
# csvdf = spark.read.csv("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\csvfile\*")
# csvdf.printSchema()
# csvdf.show()

# write dataframe in Json file
# schemadf.write.json(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\jsonfile",mode="overwrite")

# Read or Create Dataframe from json file
# jsondf = spark.read.json("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\jsonfile\*")
# jsondf.printSchema()
# jsondf.show()

# write dataframe in orc file
# schemadf.write.orc(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\orcfile")

# Read or Create Dataframe from orc file
# orcdf = spark.read.orc("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\orcfile\*")
# orcdf.printSchema()
# orcdf.show()

# write dataframe in parquet file
# schemadf.write.parquet(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\parquetfile")

# Read or Create Dataframe from parquet file
# parquetdf = spark.read.parquet("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\OutputFiles\parquetfile\*")
# parquetdf.printSchema()
# parquetdf.show()



# Q2. Apply select() Transformation on dataframe in/using various ways.
# 1. Select Single & Multiple Columns From PySpark
df.show()
# df.select("firstname","lastname").show()
# df.select(df.firstname,df.lastname).show()
# df.select(df["firstname"],df["lastname"]).show()

# By using col function
from pyspark.sql.functions import *
# df.select(col("firstname"),col("lastname")).show()

# Select columns by regular expression
# df.select(df.colRegex("`^.*name*`")).show()

# 2. Select All Columns From List
# Select All columns
# df.select([col for col in df.columns]).show()
# df.select("*").show()

# 3. Select Columns by Index
# Selects first 3 columns and top 3 rows
# df.select(df.columns[:3]).show(3)

# Selects columns 2 to 4  and top 3 rows
# df.select(df.columns[2:4]).show(3)

# Q3. Apply withColumn() Transformation on dataframe in/using various ways.
# 1. Change DataType using PySpark withColumn()
# df.withColumn("salary",col("salary").cast("Integer")).show()

# 2. Update The Value of an Existing Column
# df.withColumn("salary",col("salary")*100).show()

# 3. Create a Column from an Existing
# df.withColumn("CopiedColumn",col("salary")* -1).show()
