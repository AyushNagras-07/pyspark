from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()
data = [
    (101, "Sales", "2024-01-01", 500),
    (101, "Sales", "2024-01-05", 700),
    (101, "Sales", "2024-01-10", 300),
    (102, "Sales", "2024-01-02", 400),
    (102, "Sales", "2024-01-06", 600),
    (201, "HR", "2024-01-03", 200),
    (201, "HR", "2024-01-07", 300),
    (301, "IT", "2024-01-01", 1000),
    (301, "IT", "2024-01-08", 1200),
    (302, "IT", "2024-01-04", 900)
]

df = spark.createDataFrame(data, schema= ["employee_id", "department", "sale_date", "sales_amount"])

df.show()


# partiting using department different into parquet format
df.write.option("header",True).partitionBy("department").mode("overwrite").parquet("department_partition")

#reading only specific partitioning file
#reading where department is HR
df2 = spark.read.parquet("/home/ayush/pyspark-workspace/department_partition/department=HR")

df2.show()

# this is known as partitioning pruning

#engines decide to only read the partitions that are needed for the processing and 
# eliminate the processing of all the other partitions. 