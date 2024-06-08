from pyspark.sql import SparkSession

#initialize SparkSession
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

weather_df = spark.read.csv("home/data.csv", header=True, inferSchema=True)
weather_df.createOrReplaceTempView("mydb")

spark.sql("select * from mydb where HourlyDewPointTemperature >40").show()