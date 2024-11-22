from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Handle Missing Data")

spark = SparkSession.builder \
    .appName("Handle Missing Data") \
    .getOrCreate()


input_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/final_data"
output_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/cleaned_data_processed"


if not os.path.exists(input_path):
    logger.error(f"Input path {input_path} does not exist. Please check the path.")
    exit(1)

logger.info(f"Reading data from {input_path}")
df = spark.read.parquet(input_path)

logger.info("Calculating missing values before cleaning...")
missing_values = df.select(
    *[sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)
missing_values.show()

logger.info("Filling missing values...")
df_cleaned = df.fillna({
    "Diagnosis_Category": "Unknown",
    "Diagnosis": "Not Specified",
    "Diagnosis_Count": 0,
    "Health_Category": "Unknown",
    "Health_Count": 0
})

logger.info("Filtering invalid rows...")
df_cleaned = df_cleaned.filter(col("Utilization_Count").isNotNull())

logger.info("Calculating missing values after cleaning...")
missing_values_cleaned = df_cleaned.select(
    *[sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df_cleaned.columns]
)
missing_values_cleaned.show()

if df_cleaned.count() > 0:
    logger.info(f"Writing cleaned data to {output_path}")
    df_cleaned.write.mode("overwrite").parquet(output_path)
    logger.info("Data cleaning process completed successfully.")
else:
    logger.warning("No data to write after cleaning. Skipping write step.")
