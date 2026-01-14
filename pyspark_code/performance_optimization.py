# we can optimize the performance by many methods 
# column reduction is one of them 
# Reducing columns early means selecting only the columns you actually need as
# soon as possible in your pipeline.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

transactions_df = spark.read.csv("data/transactions.csv",header=True)
customers_df = spark.read.csv("data/customers.csv",header=True)

transactions_df.show()
customers_df.show()

#what we actual need from transactions df is cust_id amount status 
#and from customer df we need cust_id segment 

#all other column with excessive data we do not need them so they should be drop early

#this is small dataset so the change would ne hard to notice but with large column
# this is the best practice

transactions_df = transactions_df.select("cust_id","amount","status")
customers_df = customers_df.select("cust_id","segment")

join_df = transactions_df.join(customers_df,on = transactions_df.cust_id == customers_df.cust_id,how="inner")

join_df.show()

#now lets do another technique for performace optimization

