from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, sum, count, to_date
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Metrics Calculation")

spark = SparkSession.builder \
    .appName("Metrics Calculation with Dates") \
    .getOrCreate()

input_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/cleaned_data_processed"
output_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/metrics"

if not os.path.exists(input_path):
    logger.error(f"Input path {input_path} does not exist. Please check the path.")
    exit(1)

logger.info(f"Reading cleaned data from {input_path}")
df = spark.read.parquet(input_path)

df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))

logger.info("Calculating metrics grouped by Date...")

metrics_by_date = df.groupBy("Date").agg(
    avg("Utilization_Count").alias("Average_Utilization"),
    max("Utilization_Count").alias("Max_Utilization"),
    min("Utilization_Count").alias("Min_Utilization"),
    sum("Utilization_Count").alias("Total_Utilization"),
    avg("Diagnosis_Count").alias("Average_Diagnosis_Count"),
    max("Diagnosis_Count").alias("Max_Diagnosis_Count"),
    min("Diagnosis_Count").alias("Min_Diagnosis_Count"),
    sum("Diagnosis_Count").alias("Total_Diagnosis_Count"),
    avg("Health_Count").alias("Average_Health_Count"),
    max("Health_Count").alias("Max_Health_Count"),
    min("Health_Count").alias("Min_Health_Count"),
    sum("Health_Count").alias("Total_Health_Count")
)

metrics_by_date.show(truncate=False)

logger.info("Calculating overall metrics...")
overall_metrics = df.agg(
    avg("Utilization_Count").alias("Average_Utilization"),
    max("Utilization_Count").alias("Max_Utilization"),
    min("Utilization_Count").alias("Min_Utilization"),
    sum("Utilization_Count").alias("Total_Utilization"),
    avg("Diagnosis_Count").alias("Average_Diagnosis_Count"),
    max("Diagnosis_Count").alias("Max_Diagnosis_Count"),
    min("Diagnosis_Count").alias("Min_Diagnosis_Count"),
    sum("Diagnosis_Count").alias("Total_Diagnosis_Count"),
    avg("Health_Count").alias("Average_Health_Count"),
    max("Health_Count").alias("Max_Health_Count"),
    min("Health_Count").alias("Min_Health_Count"),
    sum("Health_Count").alias("Total_Health_Count")
)

overall_metrics.show(truncate=False)

logger.info(f"Saving metrics grouped by Date to {output_path}/by_date")
metrics_by_date.write.mode("overwrite").parquet(os.path.join(output_path, "by_date"))

logger.info(f"Saving overall metrics to {output_path}/overall")
overall_metrics.write.mode("overwrite").parquet(os.path.join(output_path, "overall"))

logger.info("Metrics calculation completed successfully.")
