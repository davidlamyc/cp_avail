from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName('hdbTransform').getOrCreate()
df = spark.read.option("multiLine",True).json('hdb_avail_2025-03-02_02-30-00_d819472b-fd20-43b1-9acb-3adcad5ca2b3.json')

# Extract 'items' array and explode it
items_df = df.select(explode(col("items")).alias("item"))

# Extract 'carpark_data' array and explode it
carpark_data_df = items_df.select(explode(col("item.carpark_data")).alias("carpark"))

# Extract 'carpark_info' array and explode it
carpark_info_df = carpark_data_df.select(
    col("carpark.carpark_number"),
    col("carpark.update_datetime"),
    explode(col("carpark.carpark_info")).alias("carpark_info")
)

# Select the required fields
final_df = carpark_info_df.select(
    col("carpark_number"),
    col("update_datetime"),
    col("carpark_info.total_lots").cast("int").alias('total_lots'),
    col("carpark_info.lot_type"),
    col("carpark_info.lots_available").cast("int").alias('lots_available')
)

# Show the final DataFrame
# final_df.show(30)

final_df.toPandas().to_csv('hdb_pyspark_out.csv')

# Stop Spark session
spark.stop()