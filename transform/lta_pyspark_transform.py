from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Initialize Spark session
spark = SparkSession.builder.appName("ltaTransform").getOrCreate()

# Load JSON file into DataFrame
json_path = "lta_avail_2025-03-03_22-08-50_bc7eb704-a3fc-40d0-873f-223e75da5d1c.json"
df = spark.read.option("multiline", "true").json(json_path)

# Extract 'value' array and explode it
carpark_df = df.select(explode(col("value")).alias("carpark"))

# Select required fields
final_df = carpark_df.select(
    col("carpark.CarParkID").alias("carpark_id"),
    col("carpark.Area").alias("area"),
    col("carpark.Development").alias("development"),
    col("carpark.Location").alias("location"),
    col("carpark.AvailableLots").cast("int").alias("available_lots"),
    col("carpark.LotType").alias("lot_type"),
    col("carpark.Agency").alias("agency")
)

# Show the final DataFrame
final_df.show(30)

final_df.toPandas().to_csv('lta_pyspark_out.csv')

# Stop Spark session
spark.stop()