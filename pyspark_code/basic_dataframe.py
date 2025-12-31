from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()


data = [[70001,"Ayush"],[70005,"Siddhesh"]]

column = ["PRN","Name"]

df = spark.createDataFrame(data=data,schema=column)

print(df.explain())

print(df.count())