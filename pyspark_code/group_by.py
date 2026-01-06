from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SparkByExamples.com").getOrCreate()


data =[["ENTC","Ayush",70001,80000,"N"],
       ["ENTC","Govind",70016,120000,"Y"],
       ["ENTC","Vaibhav",70008,100000,"N"],
       ["ENTC","Vedant",70134,100000,"N"],
       ["CS","Shivam",40063,110000,"Y"],
       ["CS","Sujal",40131,110000,"N"],
       ["EC","Abhay",60015,75000,"Y"],
       ["ENTC","Rohit",70021,90000,"Y"],
       ["ENTC","Kunal",70045,85000,"N"],
       ["ENTC","Sneha",70102,95000,"N"],
       ["CS","Amit",40011,130000,"Y"],
       ["CS","Neha",40105,125000,"N"],
       ["CS","Rahul",40188,115000,"N"],
       ["EC","Pooja",60022,78000,"N"],
       ["EC","Nikhil",60039,82000,"Y"],
       ["EC","Anjali",60110,80000,"N"],
       ["ME","Saurabh",50001,70000,"Y"],
       ["ME","Pratik",50017,72000,"N"],
       ["ME","Komal",50044,75000,"N"]
]
df = spark.createDataFrame(data,schema=["Dept","Name","PRN","Salary","Ever_backlog"])

df.show()

#Group by:Groups the DataFrame by the specified columns so that aggregation can be performed 
# on them.

#example in this case we group by department and see there total salary
df.groupBy(df.Dept).sum("Salary").show()

#in this we see department wise average salary
df.groupBy(df.Dept).avg("Salary").show()

#total number of students per department 

df.groupBy(df.Dept).count().show()

# using join here for every column after group by
# for getting eevry details of student with max salary per department 
df_1 = df.groupBy(df.Dept).max("Salary")

result = df.join(df_1,on = (df_1.Dept == df.Dept) & (df_1["max(Salary)"] == df.Salary))

result.show()