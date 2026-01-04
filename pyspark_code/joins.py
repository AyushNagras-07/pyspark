from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

df = spark.createDataFrame([[70001,"Ayush","N","Y",8.0],[70016,"Govind","Y","Y",8.2],[70008,"Vaibhav","N","Y",8.6],[70013,"Sajjan","Y","N",7.5]],schema = ["PRN","Name","Dead Backlogs","All clear","CGPA"])

df2 = spark.createDataFrame([[70001,80,"Azure",77000,"Office"],[70005,169,"Rboconics",95000,"Office"],[70016,25,"Windows",125000,"Hybrid"],[70008,369,"Colgate",115000,"Remote"]],schema =["PRN","id","Comapny","Salary","Working_Type"])

df.show()
df2.show()

#join is used to join two or more than two tables using a unique id present in both table 
# there are differnet type of join the following is inner joins
# where only matching column from both table will appear 
df_joined = df.join(df2,on = df.PRN == df2.PRN ,how ="inner" )

df_joined.show()

#left join where every value from left table will appear and matching from right table

df_left_join = df.join(df2,on = df.PRN == df2.PRN ,how ="left")
df_left_join.show()

#same goes for right join

df_right_join = df.join(df2,on = df.PRN == df2.PRN ,how ="right")
df_right_join.show()

#outer join where all the values from both the will be present every and each value

df_outer_join = df.join(df2,on = df.PRN == df2.PRN ,how ="outer")
df_outer_join.show()