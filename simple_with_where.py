from pyspark.sql import SparkSession
import time
#initialize SparkSession
spark = SparkSession.builder.appName("performance").getOrCreate()

weather_df = spark.read.csv("home/data.csv", header=True, inferSchema=True)
weather_df.createOrReplaceTempView("mydb")
time_start = time.time()
spark.sql("select * from mydb where NAME = 'PRESTWICK, UK'").count()
time_end = time.time()
print("thoi gian chay:", time_end - time_start)