from pyspark.sql import SparkSession

#initialize SparkSession
spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()


#Create
new_weather = [(55.50911, "HANOI,VN", 43, 100,10)]
new_weather_df = spark.createDataFrame(new_weather, schema = weather_df.schema)
update_weather_df = weather_df.union(new_weather_df)
update_weather_df.createOrReplaceTempView("mydb")
#Select
result = spark.sql("SELECT * FROM mydb")
result.show() 