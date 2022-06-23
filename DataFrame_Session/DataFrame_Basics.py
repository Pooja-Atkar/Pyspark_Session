
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Pyspark Basics").master("local[*]").getOrCreate()
    # spark = SparkSession.builder.appName("Pyspark Basics")\
    #              .master("local[*]")\
    #              .config("spark.driver.bindAddress","localhost")\
    #              .config("spark.ui.port","4050").getOrCreate()

    # print (input("Enter a value"))

    dataschema1 = StructType([StructField(name = "Id",dataType = IntegerType()),
                              StructField("Fname",StringType()),
                              StructField("Lname",StringType()),
                              StructField("Age",IntegerType()),
                              StructField("Gender",StringType()),
                              StructField("Deptno",IntegerType()),
                              StructField("Salary",LongType())
                              ])
    csvwithschemadf = spark.read.csv(path=r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\employee_detail.csv",schema=dataschema1)
    # csvwithschemadf.printSchema()
    # csvwithschemadf.show()

    # Write DataFrame in CSV File
    # csvwithschemadf.write.csv(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\csvoutput",sep="\\t")

    # Write DataFrame in Json File
    # csvwithschemadf.write.json(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\jsonoutput",lineSep="\\t")
    # csvwithschemadf.write.json(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\jsonoutput",mode="overwrite")

    # Write DataFrame in orc File
    # rc File Format(store data row and column Format).In orc file data Store in Serialize file format.
    # rc file Optimize version orc file. orc file format  used for faster read.

    # csvwithschemadf.write.orc(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\orcoutput")

    # Write DataFrame in Parquet File
    # csvwithschemadf.write.parquet(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\parquetoutput")

    # Create a DataFrame from CSV
    csvdf = spark.read.csv("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\csvoutput\*")
    csvdf.printSchema()
    csvdf.show()

    # Create a DataFrame from json
    # jsondf = spark.read.json("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\jsonoutput\*")
    # jsondf.printSchema()
    # jsondf.show()

    # Create a DataFrame from orc
    # orcdf = spark.read.orc("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\orcoutput\*")
    # orcdf.printSchema()
    # orcdf.show()

    # Create a DataFrame from parquet
    # parquetdf = spark.read.parquet("D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\output\parquetoutput\*")
    # parquetdf.printSchema()
    # parquetdf.show()

    # Empty RDD
    # rdd1 = spark.sparkContext.parallelize([])
    # print(rdd1.collect())

    # Empty DataFrame
    # spark.createDataFrame(rdd1, schema=dataschema1).show()

    # Select Function
    # csvwithschemadf.printSchema()
    # csvwithschemadf.printSchema()
    # csvwithschemadf.cache()
    # csvwithschemadf.select(csvwithschemadf.Fname).show()   # Select Single Column
    # csvwithschemadf.select(csvwithschemadf.Fname.alias("Firstname")).show()  # Select Single Column With alise Name
    # csvwithschemadf.select(col("Fname"),col("Lname")).show() # Select Multiple Column
    # csvwithschemadf.select(["Fname","Lname"]).show()  # Select Multiple Column

    # Dealing with nested Data
    # jsonnesteddf = spark.read.format("json").load(r"D:\PythonSparkProject\Pyspark_Session\DataFrame_Session\nestedjson.json")
    # jsonnesteddf.printSchema()
    # jsonnesteddf.show()
    # jsonnesteddf.select(jsonnesteddf.Address.City).show()
    # jsonnesteddf.select(["Address.City","Address.State"]).show()
    # jsonnesteddf.select("*").show()

    # print(csvwithschemadf.columns) # DataFrame column Name

    # withColumns()
    # csvwithschemadf.show()

    # existing column value change
    # csvwithschemadf.withColumn("Salary",col("Salary") * 10).show()

    # Change datatype of existing column
    # csvwithschemadf.withColumn("Deptno",col("Deptno").cast("Long")).printSchema()

    # Adding new column
    # csvwithschemadf.withColumn("State",lit("MH")).show()

    # withColumnRenamed
    # csvwithschemadf.withColumnRenamed("Fname","Firstname").show()

    # filter
    # csvwithschemadf.filter(col("Gender")=="M").show()   # filter for Single condition

    # csvwithschemadf.filter((col("Gender")=="M") & (col("Salary") > 25000)).show()    # filter for more than one condition.

    # drop(), dropDuplicate() distinct
    from pyspark.sql import Row

    data11 =[Row(name='Ajay',age=20),
             Row(name='Nikhil',age=25),
             Row(name='Ajay',age=20),
             Row(name='Ajay',age=30)]

    # df11 = spark.sparkContext.parallelize(data11).toDF()
    # df11.printSchema()
    # df11.show()


    # distinct()
    # df11.distinct().show()

    # dropDuplicates()
    # df11.dropDuplicates(['name']).show()

    # drop
    # df11.drop('age').show()

    # groupby()-------Aggregate Function
    # csvwithschemadf.groupby('Deptno').avg('Salary').show()

    # Join
    datajoin = [Row(Deptno=11,Deptname='HR'),
                Row(Deptno=12,Deptname='IT')]

    # datajoindf = spark.sparkContext.parallelize(datajoin).toDF()
    # datajoindf.printSchema()
    # datajoindf.show()

    # csvwithschemadf.printSchema()

    # inner join
    # csvwithschemadf.join(datajoindf,
    #                      on=csvwithschemadf.Deptno == datajoindf.Deptno,
    #                      how ='inner').select(["Id","Fname","Lname",
    #                                     csvwithschemadf.Deptno,"Deptname"]).show()

    # Left Join
    # csvwithschemadf.join(datajoindf,
    #                      on=csvwithschemadf.Deptno == datajoindf.Deptno,
    #                      how='left').show()

    # antijoin
    # csvwithschemadf.join(datajoindf,
    #                      on=csvwithschemadf.Deptno == datajoindf.Deptno,
    #                      how='left_anti').show()

    # Union
    datajoin1 = [Row(Deptno=11, Deptname='HR'),
                 Row(Deptno=13, Deptname='IT'),
                 Row(Deptno=14, Deptname='IT')]

    # datajoindf1 = spark.sparkContext.parallelize(datajoin1).toDF()
    # datajoindf1.printSchema()

    # datajoindf.union(datajoindf1).show()
    # datajoindf.unionAll(datajoindf1).distinct().show()