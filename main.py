# src/main.py
from pyspark.sql import SparkSession
from process_history import create_wallet_history
from calc_interest import calculate_daily_interest

spark = SparkSession.builder \
    .appName("CDI Bonus Calculation") \
    .getOrCreate()


rates_path = "data/daily_rates.parquet" # Interest rates also in Parquet format
parquet_files = [
    "data/part-00000-tid-8402580976294597621-c55615b7-f61b-424b-ae71-3a1118e366b9-21003-1-c000.zstd.parquet",
    "data/part-00001-tid-8402580976294597621-c55615b7-f61b-424b-ae71-3a1118e366b9-21004-1-c000.zstd.parquet",
    "data/part-00002-tid-8402580976294597621-c55615b7-f61b-424b-ae71-3a1118e366b9-21005-1-c000.zstd.parquet",
    "data/part-00003-tid-8402580976294597621-c55615b7-f61b-424b-ae71-3a1118e366b9-21006-1-c000.zstd.parquet",
    "data/part-00004-tid-8402580976294597621-c55615b7-f61b-424b-ae71-3a1118e366b9-21007-1-c000.zstd.parquet"
]

# Read and stack all files
df_total = spark.read.parquet(parquet_files[0])
for arquivo in parquet_files[1:]:
    df = spark.read.parquet(arquivo)
    df_total = df_total.unionByName(df)

# Create balance history
wallet_history = create_wallet_history(spark, df_total)

# Calculate interest
payouts = calculate_daily_interest(spark, wallet_history, rates_path)


# Save result
payouts.write.mode("overwrite").parquet("data/output_daily_payouts")




