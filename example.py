from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("example-app").getOrCreate()

# Read data from database
df = spark.read.format("jdbc").option("url", f"jdbc:sqlite:/home/test.db").option("dbtable","mydb").load()

# Run a simple SQL query
df.createOrReplaceTempView("mydb")
result = spark.sql("SELECT COUNT(*) FROM mydb")

# Show the result
result.show()

# Stop SparkSession
spark.stop()