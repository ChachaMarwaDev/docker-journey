# ==============================
# 1. IMPORTS
# ==============================
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, max

# ==============================
# 2. CREATE SPARK SESSION
# ==============================
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Taxi Data Analysis") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")

# ==============================
# 3. LOAD DATA
# ==============================
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

print(f"Total records: {df.count()}")
df.printSchema()

# ==============================
# 4. FILTER DATA (Question 3)
# ==============================
df_filtered = df.filter(
    (col("tpep_pickup_datetime") >= "2025-11-15") &
    (col("tpep_pickup_datetime") < "2025-11-16")
)

print(f"November 15 records: {df_filtered.count()}")

# ==============================
# 5. COMPUTE DURATION (Question 4)
# ==============================
df_duration = df.withColumn(
    "duration_hours",
    (unix_timestamp("tpep_dropoff_datetime") - 
     unix_timestamp("tpep_pickup_datetime")) / 3600
)

df_duration.select(max("duration_hours")).show()

# ==============================
# 6. GROUPING + JOIN (Question 6)
# ==============================
zones = spark.read.option("header", "true") \
    .csv("taxi_zone_lookup.csv")

result = df.groupBy("PULocationID") \
    .count() \
    .join(zones, df["PULocationID"] == zones["LocationID"]) \
    .orderBy("count") \
    .select("Zone", "count")

result.show(5)

# ==============================
# 7. NOTES
# ==============================
# Spark UI runs on port 4040 by default
# !wget url for the data on zones and yellow_tripdata_2025-11
# !wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
# !wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv