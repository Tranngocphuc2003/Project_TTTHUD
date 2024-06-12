from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
#initialize SparkSession
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()
weather_df = spark.read.csv("home/data.csv", header = True, inferSchema = True)
weather_df.createOrReplaceTempView("mydb")

#Create
new_weather = [(55.50911, "HANOI,VN", 43, 100,10)]
new_weather_df = spark.createDataFrame(new_weather, schema = weather_df.schema)
update_weather_df = weather_df.union(new_weather_df)
update_weather_df_with_id = update_weather_df.withColumn("id", monotonically_increasing_id())
update_weather_df_with_id.createOrReplaceTempView("mydb")
#Select
result = spark.sql("SELECT * FROM mydb ORDER BY id DESC ")
result.show()