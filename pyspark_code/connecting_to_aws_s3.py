from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VSCode S3 Stable")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (1, "Ayush", "Data"),
    (2, "Spark", "S3"),
]

df = spark.createDataFrame(data, ["id", "name", "source"])
df.show()

s3_path = "s3a://ayush-pyspark-data-bucket-2026/raw/vscode_stable/"

df.write.mode("overwrite").parquet(s3_path)

spark.stop()
