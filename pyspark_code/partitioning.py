from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

data = [
    ("US", "Ayush", 100),
    ("US", "Gire", 200),
    ("US", "Athrav", 300),
    ("IN", "Siddhesh", 400),
    ("IN", "Om", 500),
    ("IN", "Vaibhav", 600),
    ("UK", "Kadu", 700),
    ("UK", "Kishore", 800),
    ("UK", "Nilesh", 900),
    ("CA", "Sajjan", 1000),
    ("US", "Shubham", 400),
    ("US", "Govind", 152),
    ("US", "Shrikant", 700),

]

df = spark.createDataFrame(data, schema=["country", "name", "sales"])

df.show()

#Spark/Pyspark partitioning is a way to split the data into multiple partitions so 
# that you can execute transformations on multiple partitions in parallel which allows 
# completing the job faster. 

#You can partition or repartition the Dataframe by calling repartition() or coalesce().

#using repartitioning 
#checking how much partition by default generated
print(df.rdd.getNumPartitions())

#partitioning in group of 6 randomly
df1 = df.repartition(6)
print(df1.rdd.getNumPartitions())

#partitioning in group of 3 with column country
df_2 = df.repartition(3,"country")
print(df_2.rdd.getNumPartitions())

#using partitionby to write data into different csv according to country 
df.write.option("header",True).partitionBy("country").mode("overwrite").csv("country_partition")

#same using repartionby
#here we use partition by inside repartitionby it will create all country folder 
# and inside it will repartition by 3 count per csv 
df.repartition(3).write.option("header",True).partitionBy("country").mode("overwrite").csv("country_repartition")