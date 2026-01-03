from pyspark.sql import SparkSession
from pyspark.sql import functions as sparkfunc 
from pyspark.sql.functions import col
from pyspark.sql.functions import when


spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()

df = spark.createDataFrame([[70001,"Ayush","Y","N",3],[70005,"Siddhesh","Y","N",2],[70008,"Vaibhav","Y","N",3]],schema=["PRN","Name","All_clear","Dead BackLogs","Year"])

df.show()

#Evaluates a list of conditions and returns one of multiple possible result expressions. 
#If Column.otherwise() is not invoked, None is returned for unmatched conditions.

result = df.select(df.Name,sparkfunc.when(df.Year>2,1).when(df.Year<3,0).alias("In 3rd or 4th year"),df.PRN)
result.show()

#Using otherwise gives us the else condition for remaining non condition passing column

result_1 = df.select(df.Name,sparkfunc.when(df.Year>2,1).otherwise(0).alias("In 3rd or 4th year"),df.PRN)
result_1.show()

#using it for an application

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]

columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

#selecting those who are in third year and averaging there cgpa

result_df= df.select(col("*"),when(df.gender == "M","Male")
                  .when(df.gender == "F","Female")
                  .when(df.gender.isNull() ,"")
                  .otherwise(df.gender).alias("new_gender"))

result_df.show()
