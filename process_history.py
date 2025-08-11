# src/process_history.py
from pyspark.sql import functions as F, Window

def create_wallet_history(spark, df_total):
    cdc_df = df_total

    # Filter only relevant operations (e.g., exclude deletes)
    cdc_filtered = cdc_df.filter(F.col("cdc_operation") != "delete")

    # Validate required columns
    required_cols = ["event_time", "user_id", "account_id", "amount", "cdc_sequence_num"]
    missing = [c for c in required_cols if c not in cdc_filtered.columns]
    if missing:
        raise ValueError(f"Missing columns in input data: {missing}")

    # Create window to order transactions by user, account, and timestamp, using CDC sequence for tie-breaking
    window_spec = Window.partitionBy("user_id", "account_id") \
                        .orderBy(F.col("event_time").cast("timestamp"), F.col("cdc_sequence_num")) \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Calculate cumulative balance
    wallet_with_balance = cdc_filtered \
        .withColumn("balance", F.sum("amount").over(window_spec)) \
        .withColumn("date", F.to_date("event_time"))

    # Create daily balance history per user and wallet by capturing end-of-day balance
    wallet_history = wallet_with_balance.groupBy("user_id", "account_id", "date") \
        .agg(F.last("balance").alias("end_of_day_balance"))

    return wallet_history
