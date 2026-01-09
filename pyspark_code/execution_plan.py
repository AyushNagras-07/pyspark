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

#An execution plan is the set of operations executed to translate a query language statement
#  (SQL, Spark SQL, Dataframe operations)to a set of optimized logical and physical operations.

# explain is used Prints the (logical and physical) plans to the console for debugging purposes.
# for printing both logical and physical plans we need to use extended=true as parameter

#this might give simple logical and physical plan
df.explain(extended=True)

df_agg = df.groupBy("country").agg({"sales": "sum"})
df_agg.show()
#check for this you will get to know what actually it shows
df_agg.explain(extended=True)

#The logical plans describe the intent of your code,
# evolving from a raw request to an optimized definition of the required data flow

# The physical plan is where the distributed computing happens. 
# It is much more complex because calculating a global sum per country requires moving 
# data around your cluster


#logical plan (what to do) and the physical plan (how to do it on the cluster)