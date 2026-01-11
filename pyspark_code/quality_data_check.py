# how to identify whether the data is good or bad
# by using data quality dimensions

#dimensions are 

#Uniqueness : no duplicates
#Accuracy : represents the real world entities accurately nothing noise 
#Completeness : no null values
#Validity : expected format, range, or structure
#Consistency : data is uniform and coherent across various sources no bias
#Timeliness : data being up-to-date and relevant for its intended use.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,sum,current_date


spark = SparkSession.builder.master("local[*]").appName("AyushNagras").getOrCreate()

df = spark.read.option("header",True).csv("data/data2.csv")

df.show()
#uniquesness
#displaying rows with duplicates data in all column
df.exceptAll(df.dropDuplicates()).show()
#you can see the rows with  duplicates value in all column

#completeness
#handle missing/null values 
df.select(*(sum(col(c).isNull().cast("int")).alias(c) for c in df.columns)).show()
#will show number of null values

#accuracy
#data is valid and correct 
df_cleaned = df.withColumn(
    "Age",
    when((col("Age").cast("int").isNull()) | (col("Age") <= 0), None).otherwise(col("Age"))
)
df_cleaned.show()
#you can see the data will correct age and the age is incorrect the value will be shown null

#validity
#various checks and constraints
valid_occupations = ["Engineer", "Teacher", "Software Developer", "Accountant", "Marketing Manager"]

df_valid = df.withColumn(
    "Occupation",
    when(col("Occupation").isin(valid_occupations), col("Occupation")).otherwise(None)
).filter(col("Occupation").isNotNull()).show()
#wil show the data for only occuption that are present in valid_occupations

#consistency
#consistent structure and format, especially when you’re working with multiple data sources 
#lets consider you have two different csv with column name a little different
#you can easily configure that in pyspark


df1 = spark.read.csv("data/data_1.csv", header=True)
df2 = spark.read.csv("data/data_2.csv", header=True)

df1 = df1.withColumnRenamed("emp_id", "employee_id").withColumnRenamed("emp_name", "employee_name")
df2 = df2.withColumnRenamed("employeeID", "employee_id").withColumnRenamed("employeeName", "employee_name")

df1.union(df2).show()

#timeliness
#examine timestamps and ensure that the data is up-to-date
df_day = spark.read.csv("data/day_wise_data.csv",header=True)
#The code below will check whether the events occurred within the last 7 days
days_threshold = 7
df_timely = df_day.filter((current_date() - col("EventDate")).cast("int") <= days_threshold)

df_timely.show()

# can visit for in details knowledge https://medium.com/towards-data-engineering/pyspark-data-quality-3bbeb5e17887