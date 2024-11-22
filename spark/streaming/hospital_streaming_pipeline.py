from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, trim, lower, to_date, split

spark = SparkSession.builder \
    .appName("Hospitals-Covid-Integration") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .config("spark.network.timeout", "300s") \
    .getOrCreate()

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hospital-utilization") \
    .option("startingOffsets", "earliest") \
    .load()

df_hospital_utilization = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(split(col("value"), ",").alias("fields")) \
    .select(
        col("fields")[0].alias("Setting"),
        col("fields")[1].alias("System"),
        col("fields")[2].alias("Facility"),
        col("fields")[3].alias("Date"),
        col("fields")[4].cast(IntegerType()).alias("Utilization_Count")
    )

df_hospital_utilization = df_hospital_utilization \
    .withColumn("Date", to_date("Date", "yyyy-MM-dd")) \
    .withColumn("Setting", trim(lower(col("Setting"))))

path_diagnosis = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/data/cleaned/in-hospital-mortality-trends-by-diagnosis-type-cleaned.csv"
path_health = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/data/cleaned/in-hospital-mortality-trends-by-health-category-cleaned.csv"

df_mortality_diagnosis = spark.read.csv(path_diagnosis, header=True, inferSchema=True) \
    .withColumnRenamed("Count", "Diagnosis_Count") \
    .withColumnRenamed("Category", "Diagnosis_Category") \
    .withColumn("Date", to_date("Date", "yyyy-MM-dd")) \
    .withColumn("Setting", trim(lower(col("Setting"))))

df_mortality_health = spark.read.csv(path_health, header=True, inferSchema=True) \
    .withColumnRenamed("Count", "Health_Count") \
    .withColumnRenamed("Category", "Health_Category") \
    .withColumn("Date", to_date("Date", "yyyy-MM-dd")) \
    .withColumn("Setting", trim(lower(col("Setting"))))

df_enriched_with_diagnosis = df_hospital_utilization \
    .join(df_mortality_diagnosis, on=["Date", "Setting"], how="left")

df_fully_enriched = df_enriched_with_diagnosis \
    .join(df_mortality_health, on=["Date", "Setting"], how="left")

output_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/final_data"
checkpoint_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/checkpoints/final_data"

query = df_fully_enriched.writeStream \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", checkpoint_path) \
    .outputMode("append") \
    .start()

query.awaitTermination()