from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Load data
df = spark.read.csv("user_data.csv", header=True, inferSchema=True)

# Transformations
df_cleaned = df.filter((col("age") > 0) & (col("income") > 0))

# Add derived metric
df_transformed = df_cleaned.withColumn("CTR", col("clicks") / (col("purchases") + 1))

# Save as Parquet (efficient format)
df_transformed.write.mode("overwrite").parquet("cleaned_data")

spark.stop()
