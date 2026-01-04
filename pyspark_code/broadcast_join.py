from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()


#As you know PySpark splits the data into different nodes for parallel processing, 
# when you have two DataFrames, the data from both are distributed across multiple nodes 
# in cluster so, when you perform traditional join, PySpark is required to shuffle the data. 
# Shuffle is needed as the data for each joining key may not colocate on the same node 
# and to perform join the data for each key should be brought together on the same node. 
# Hence, the traditional PySpark Join is a very expensive operation.

#so we use broadcast join
# PySpark Broadcast Join is an important part of the SQL execution engine, With broadcast 
# join, PySpark broadcast the smaller DataFrame to all executors and the executor keeps this
# DataFrame in memory and the larger DataFrame is split and distributed across all 
# executors so that PySpark can perform a join without shuffling any 
# data from the larger DataFrame as the data required for join colocated on every executor. 



spark = SparkSession.builder.getOrCreate()

large_df = (
    spark.range(0, 10_000_000)
         .withColumn("customer_id", (expr("id % 100000") + 1))
         .withColumn("amount", expr("round(rand() * 1000, 2)"))
         .withColumn("transaction_type", expr("CASE WHEN id % 2 = 0 THEN 'ONLINE' ELSE 'STORE' END"))
         .drop("id")
)

large_df.printSchema()


small_data = [
    Row(customer_id=i, customer_name=f"Customer_{i}", country="US")
    for i in range(1, 100001)
]

small_df = spark.createDataFrame(small_data)

small_df.printSchema()

regular_join = large_df.join(small_df , on= large_df.customer_id == small_df.customer_id)
broadcast_join = large_df.join(broadcast(small_df),on = large_df.customer_id == small_df.customer_id)


regular_join.explain(extended=False)
broadcast_join.explain(extended=False)

