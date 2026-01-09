from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

data = [("Ayush", 1, 55000,70001), ("Vaibhav", 4, 75000,70004), ("Siddhesh", 0, 60000,70005)]
columns = ["name", "job_changed", "salary","id"]

df = spark.createDataFrame(data=data,schema= columns)
df.show()
#cache()
# Persists the DataFrame with the default storage level (MEMORY_AND_DISK_DESER).

#df.cache() is used to store a DataFrame's intermediate results in memory and on disk for
# faster access in subsequent operations

#before cache 

df2 = df.where(col("salary")>=60000)
count = df2.count()

df3 = df2.where(col("job_changed")<3)
count = df3.count()

# Since action triggers the transformations, in the above example df2.count() is the 
# first action hence it triggers the execution of reading a CSV file, and df.where().

#We also have another action df3.count(), this again triggers execution of reading a file,
#  df.where() and df2.where(). as df2.where will execute df.where again 


#after cache 

df2 = df.where(col("salary")>=60000).cache()
count = df2.count()

df3 = df2.where(col("job_changed")<3)
count = df3.count()

#now everything will be executed just once df2 will store the cache value of the action 
# df.where which will reduce excution time

#after all this you need to free the memory and free memory and disk storage
#for that we use Unpersist

df2.unpersist()

#this will free up the space 
# use this when you no longer need the dataset to manage memory 
# and prevent potential overflow errors.