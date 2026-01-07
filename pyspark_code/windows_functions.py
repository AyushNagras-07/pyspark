from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import rank
from pyspark.sql.functions import dense_rank
from pyspark.sql.functions import lag


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

# windows functions allow you to perform calculations across a set of rows
# that are related to the current row. 

#Window functions are particularly useful in scenarios where you need to calculate 
# aggregates or other calculations that depend on a set of related rows, such as 
# computing running totals, rank, or percentiles

#1. row number 


window_spec = Window.partitionBy("employee_id").orderBy("sale_date")

df.withColumn("row_num", row_number().over(window_spec)).show()

#2. rank 

window_spec = Window.partitionBy("department").orderBy(df.sales_amount.desc())

df.withColumn("rank_in_dept", rank().over(window_spec)).show()

#3. dense rank

window_spec = Window.partitionBy("department").orderBy(df.sales_amount.desc())

df_dense_rank = df.withColumn("dense_rank_in_dept",dense_rank().over(window_spec))

df_dense_rank.show()


# lag
window_spec = Window.partitionBy("employee_id").orderBy("sale_date")

df.withColumn("prev_sales", lag("sales_amount", 1).over(window_spec)).show()


