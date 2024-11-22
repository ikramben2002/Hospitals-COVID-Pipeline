from pyspark.sql import SparkSession

# Initialisez une session Spark
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()

# Chemin vers le dossier contenant les fichiers Parquet
parquet_path = "/Users/benabbas/Hospitals-Covid-Data-Pipeline/output/final_data"

# Lisez les fichiers Parquet
print("Reading Parquet files...")
df = spark.read.parquet(parquet_path)

# Affichez les données
print("Displaying data from Parquet files:")
df.show(20, truncate=False)

print("Displaying schema of the data:")
df.printSchema()
