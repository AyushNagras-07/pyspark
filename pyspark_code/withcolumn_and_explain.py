from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

data = [
    (101, "Rohan", 28, 5200),
    (102, "Ananya", 32, 6800),
    (103, "Vikram", 41, 7500),
    (104, "Neha", 26, 4800),
    (105, "Amit", 35, 6100)
]

columns = ["emp_id", "full_name", "age", "salary"]

df = spark.createDataFrame(data, columns)
df.show()

# we use withcolumn to
# add a new column to a DataFrame 
# replace the values of an existing column with a new expression or value

# df1 = df.withColumn("salary_after_hike",df.salary+0.80*df.salary)

# df1.show()

#Explain parameter: Prints the (logical and physical) plans to the console for debugging purposes.


df.explain(mode="formatted")  
