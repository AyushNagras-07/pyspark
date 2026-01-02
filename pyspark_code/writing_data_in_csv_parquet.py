from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()


data = [[70001,"Ayush"],[70005,"Siddhesh"]]
col=["PRN","Name"]
df = spark.createDataFrame(data=data,schema=col)

df.show()
#writing data in csv format
df.write.csv("data_1.csv",header=True,mode="overwrite")

#writing data in parquet format
df.write.parquet('data.parquet',mode="overwrite")

#reading from both format

#csv
spark.read.csv("data_1.csv",header=True).show()

#parquet
spark.read.parquet("data.parquet").show()