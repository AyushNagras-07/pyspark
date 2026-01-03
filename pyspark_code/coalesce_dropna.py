from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

data = [("Ayush", 1, 55000,70001), ("Vaibhav", 4, 75000,70004), ("Siddhesh", 0, 60000,70005)]
columns = ["name", "job_changed", "salary","id"]

df = spark.createDataFrame(data=data,schema= columns)

data2 = [(70001,8.5,'ABC'),(70002,9.6,None),(70005,8.67,'XYZ'),(70016,9.5,'Cisco'),(None,9.69,'Colgate')]
columns2 = ["id","cgpa","company"]

df2 = spark.createDataFrame(data=data2 , schema = columns2)

# merging two diffferent datasets
merged_dataset = df.join(df2,on = df.id == df2.id,how="full")

merged_dataset.show()

#as we can see there is null in every column we cant figure out the data nicely
# even the primary key id contains null and
# the main problem there are two id columns which one to choose right or left side column
# coalesce comes in here where it consider one of the id from both id , if one is null choose
# other id which is present and vice versa 

#using coalesce 

merged_dataset = merged_dataset.withColumn("new_id",coalesce(df.id,df2.id))
merged_dataset.show()

#using dropna 
# we use dropna to drop if the row if there is any null values in it 

df2.na.drop().show()