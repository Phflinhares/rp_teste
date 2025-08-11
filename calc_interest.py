# src/calc_interest.py
from pyspark.sql import functions as F

def calculate_daily_interest(spark, wallet_history, rates_path):
    
    rates_df = spark.read.parquet(rates_path) \
    .withColumn("rate_date", F.to_date(F.col("date"), "yyyy-MM-dd")) \
    .drop("date")

    joined_df = wallet_history.join(
        rates_df,
        wallet_history.date == rates_df.rate_date,
        "left"
    )

    # Calculate interest only for balances greater than 100
    qualified_df = joined_df.filter(F.col("end_of_day_balance") > 100)

    payouts = qualified_df.withColumn(
        "interest_amount",
        F.col("end_of_day_balance") * (F.col("daily_rate") / 100)
    ).select("user_id", "account_id", "date", "end_of_day_balance", "daily_rate", "interest_amount")

    return payouts
