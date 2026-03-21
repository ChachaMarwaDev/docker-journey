# test_spark.py
# Run this to confirm PySpark is working inside the container.
# Command: python test_spark.py

from pyspark.sql import SparkSession

# Start a local Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestSpark") \
    .getOrCreate()

# Reduce noisy logs
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark started successfully!")
print(f"   Spark version: {spark.version}")

# Create a tiny test DataFrame
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
df = spark.createDataFrame(data, ["name", "value"])

print("\n📊 Test DataFrame:")
df.show()

print("✅ Everything works! Spark is ready for your homework.")

spark.stop()
