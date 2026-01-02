from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col , count , when

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("PRN", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("CGPA", DoubleType(), True),
    StructField("All clear", StringType(), True),
    StructField("Dead Backlogs", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Internship", StringType(), True),
    StructField("Scholarship", StringType(), True)
])

data = [
    [70001, "Ayush",     8.55, "Y", "N", "Computer", 4, "Y", "Y"],
    [70005, "Siddhesh",  8.00, "Y", "N", "IT",       4, "N", "N"],
    [70002, "Gire",      7.27, "Y", "Y", "Computer", 4, "N", "N"],
    [70006, "Govind",    8.20, "Y", "N", "Mechanical", 4, "Y", "N"],
    [70007, "Vaibhav",   8.56, "Y", "N", "Computer", 4, "Y", "Y"],
    [70008, "Om",        8.30, "Y", "N", "IT",       3, "N", "N"],
    [70009, "Nilesh",    8.80, "Y", "N", "Computer", 4, "Y", "Y"],
    [70010, "Shubham",   8.00, "Y", "N", "Mechanical", 3, "N", "N"],
    [70011, "Shrikant",  8.00, "Y", "N", "IT",       4, "N", "N"]
]

df = spark.createDataFrame(data, schema=schema)

df.show()


#chain select means where we apply transformations to create 
# a sequence of operations in a single, fluent expression.

# for the above example 
#Count students with Internship = 'Y' per Year (no dead backlogs)
df_chained = (
    df.filter(col("Dead Backlogs") == "N")
    .groupBy("Year")
    .agg(count(when(col("Internship") == "Y", True)).alias("Internship_Count"))
    .orderBy(col("Internship_Count").desc())
)

df_chained.show()


#Select student names with no dead backlogs, sorted by CGPA
df_chained_1 = (
    df.filter(col("Dead Backlogs") == "N")
    .orderBy(col("CGPA").desc())
    .select(col("Name"))
)

df_chained_1.show()