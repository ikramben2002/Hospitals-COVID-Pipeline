from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count

spark = SparkSession.builder \
    .appName("Analyze Missing Data") \
    .getOrCreate()

input_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/final_data"

df = spark.read.parquet(input_path)

total_rows = df.count()
print(f"Total rows: {total_rows}")

missing_stats = df.select(
    *[sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_missing") for c in df.columns],
    *[sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(f"{c}_present") for c in df.columns]
)

print("Missing and Present Counts:")
missing_stats.show(truncate=False)

missing_percentages = missing_stats.select(
    *[(col(f"{c}_missing") / total_rows * 100).alias(f"{c}_missing_percentage") for c in df.columns]
)

print("Percentage of Missing Values:")
missing_percentages.show(truncate=False)
