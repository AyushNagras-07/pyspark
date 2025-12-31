from pyspark.sql import SparkSession
from datetime import datetime 
from pyspark.sql.types import *


spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

# reading using inferSchema
df = spark.read.option("header", True).option("inferSchema", True).csv("/home/ayush/pyspark-workspace/data/data.csv")

df.show()
df.printSchema()

#reading using explicit structtype
schema = StructType([StructField("PRN",IntegerType(),nullable=False),
                     StructField("Date DT",DateType(),nullable=False)])
edf=spark.read.option("header",True).schema(schema).csv("/home/ayush/pyspark-workspace/data/data.csv")

edf.show()
edf.printSchema()

#Default schema = all columns as STRING
sdf = spark.read.csv("/home/ayush/pyspark-workspace/data/data.csv")

sdf.show()
sdf.printSchema()
