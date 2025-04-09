# %%
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, broadcast
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("AnalyticalETL").enableHiveSupport().getOrCreate()

# Azure Storage Account Setup
storage_account_name = "trial25"
container = "equity-data"
storage_account_key = ""

# Set Spark Configuration for Azure Blob Storage
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",storage_account_key)

trade_file_location = "wasbs://equity-data@trial25.blob.core.windows.net/output_dir/partition=T/"
quote_file_location = "wasbs://equity-data@trial25.blob.core.windows.net/output_dir/partition=Q/"

# Read Parquet Files From Azure Blob Storage Partition
# Read Trade Parquet
trade_df = spark.read.parquet("wasbs://equity-data@trial25.blob.core.windows.net/trade")
#"wasbs://equity-data@trial25.blob.core.windows.net/trade/trade_dt={}".format("2020-08-06")

# Read Quote Parquet
quote_df = spark.read.parquet("wasbs://equity-data@trial25.blob.core.windows.net/output_dir/partition=Q/")

# %%
# Create Trade Staging Table
# Use Spark To Read The Trade Table With Date Partition “2020-08-06”
trade_df = spark.sql("""
                     SELECT    trade_dt,
                               symbol,
                               exchange,
                               event_tm,
                               event_seq_nb,
                               trade_pr 
                        FROM trades 
                        WHERE trade_dt = '2020-08-06'
                        ORDER BY symbol,exchange,trade_dt,event_tm
                     """)
display(trade_df)

# %%
# Create A Spark Temporary View
trade_df.createOrReplaceTempView("tmp_trade_moving_avg")

# %%
# Calculate The 30-min Moving Average Using The Spark Temp View
mov_avg_df = spark.sql("""
    SELECT 
        trade_dt,
        symbol, 
        exchange, 
        event_tm, 
        event_seq_nb, 
        trade_pr,
        -- Compute 30-minute moving average based on time
        AVG(trade_pr) OVER (
            PARTITION BY symbol, exchange 
            ORDER BY event_tm 
            RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW
        ) AS mov_avg_pr
    FROM tmp_trade_moving_avg
""")

display(mov_avg_df.take(mov_avg_df.count()))

# %%
# Save The Temporary View Into Hive Table For Staging
mov_avg_df.write.mode("overwrite").saveAsTable("temp_trade_moving_avg")

# %%
# Create Staging Table For The Prior Day’s Last Trade
# Get The Previous Date Value
trade_date = date(2020, 8, 6)#datetime.strptime('2020-08-06', '%Y-%m-%d')
#[use datetime utility to calculate previous date]
prev_date_str = (trade_date - timedelta(days=1)).isoformat()#.strftime("%Y-%m-%d")

print("Trade_date:",trade_date)
print("Prev_Date_Str:",prev_date_str)

# %%
# Use Spark To Read The Trade Table With Date Partition “2020-08-05”
prev_trade_df = spark.sql(f"""
    SELECT symbol, exchange, event_tm, event_seq_nb, trade_pr 
    FROM trades 
    WHERE trade_dt = '{prev_date_str}'
""")
prev_trade_df.show()

# %%
# Create Spark Temporary View
prev_trade_df.createOrReplaceTempView("tmp_last_trade")

# %%
#Calculate Last Trade Price Using The Spark Temp View

# Need to MODIFY this query for calculating last trade price
last_pr_df = spark.sql("""
    SELECT 
        symbol, 
        exchange, 
        last_pr 
    FROM (
        SELECT 
            symbol, 
            exchange, 
            event_tm, 
            event_seq_nb, 
            trade_pr, 
            LAST(trade_pr) OVER (
                PARTITION BY symbol, exchange 
                ORDER BY event_tm
            ) AS last_pr
        FROM temp_trade_moving_avg
    ) a
""")


last_pr_df.show()

# %%
#Calculate Last Trade Price Using The Spark Temp View

# Need to MODIFY this query for caculating last trade price
last_pr_df = spark.sql("""
    SELECT 
        symbol, 
        exchange, 
        last_pr 
    FROM (
        SELECT 
            symbol, 
            exchange, 
            event_tm, 
            event_seq_nb, 
            trade_pr, 
            FIRST_VALUE(trade_pr) OVER (
                PARTITION BY symbol, exchange 
                ORDER BY event_tm DESC
            ) AS last_pr
        FROM tmp_last_trade
    ) a
""")

last_pr_df.show()

# %%
# Save The Temporary View Into Hive Table For Staging
last_pr_df.write.mode("overwrite").saveAsTable("temp_last_trade")

# %%
'''
4.4 Populate The Latest Trade and Latest Moving Average Trade Price To The Quote
Records
Now that you’ve produced both staging tables, join them with the main table “quotes” to
populate trade related information.
'''

# %%
'''4.4.1 Join With Table temp_trade_moving_avg
You need to join “quotes” and “temp_trade_moving_avg” to populate trade_pr and mov_avg_pr
into quotes. However, you cannot use equality join in this case; trade events don’t happen at the
same quote time. You want the latest in time sequence. This is a typical time sequence
analytical use case. A good method for this problem is to merge both tables in a common time sequence.'''

'''4.4.1.1 Define A Common Schema Holding “quotes” and “temp_trade_moving_avg”
Records
This is a necessary step before the union of two datasets which have a different schema
(denormalization). The schema needs to include all the fields of quotes and
temp_trade_mov_avg so that no information gets lost.'''
display(spark.sql("DESCRIBE quotes"))
display(spark.sql("DESCRIBE temp_trade_moving_avg"))

from pyspark.sql.types import *

unified_schema = StructType([
    StructField("trade_dt", DateType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("bid_pr", DecimalType(10, 2), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(10, 2), True),
    StructField("ask_size", IntegerType(), True),
    StructField("arrival_tm", TimestampType(), True),
    StructField("trade_pr", DecimalType(10, 2), True),
    StructField("mov_avg_pr", DecimalType(14, 6), True),
    StructField("rec_type", StringType(), True)
    
])

quote_schema = StructType([
    StructField("trade_dt", DateType(), True),
    StructField("rec_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("arrival_tm", TimestampType(), True),
    StructField("trade_pr", DecimalType(10, 2), True),
    StructField("bid_pr", DecimalType(10, 2), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(10, 2), True),
    StructField("ask_size", IntegerType(), True),
    #StructField("rec_type", StringType(), True),=
    # StructField("trade_pr", DecimalType(10, 2), True),
    # StructField("mov_avg_pr", DecimalType(14, 6), True)    
])

temp_trade_moving_avg_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("trade_pr", DecimalType(10, 2), True),
    StructField("mov_avg_pr", DecimalType(14, 6), True),
    StructField("trade_dt", DateType(), True)
    #StructField("rec_type", StringType(), True)
])


display(spark.sql("DESCRIBE quotes"))

quotes_df = spark.sql("SELECT * FROM quotes")
quotes_casted = spark.createDataFrame(quotes_df.collect(), schema=quote_schema)
quotes_casted.createOrReplaceTempView("quotes_casted")

trades_df = spark.sql("SELECT * FROM temp_trade_moving_avg")
trades_casted = spark.createDataFrame(trades_df.collect(), schema=temp_trade_moving_avg_schema)
trades_casted.createOrReplaceTempView("trades_casted")


# %%
# 4.4.1.2 Create Spark Temp View To Union Both Tables
from pyspark.sql.functions import last
# Step 1: Create a unified time-ordered table with all quote and trade fields
quote_union = spark.sql("""
    SELECT * FROM (
        SELECT
            trade_dt,
            symbol,
            exchange,
            event_tm,
            event_seq_nb,
            bid_pr,
            bid_size,
            ask_pr,
            ask_size,
            arrival_tm,
            NULL AS trade_pr,
            NULL AS mov_avg_pr,
            'Q' AS rec_type
        FROM quotes_casted
        WHERE trade_dt IN (SELECT DISTINCT(trade_dt) FROM temp_trade_moving_avg)

        UNION ALL

        SELECT
            trade_dt,
            symbol,
            exchange,
            event_tm,
            NULL AS event_seq_nb,
            NULL AS bid_pr,
            NULL AS bid_size,
            NULL AS ask_pr,
            NULL AS ask_size,
            NULL AS arrival_tm,
            trade_pr,
            mov_avg_pr,
            'T' AS rec_type
        FROM trades_casted
    )
    ORDER BY trade_dt, event_tm
    
""")

quote_union.createOrReplaceTempView("quote_union")
#display(quote_union)

# Step 2: Apply window function to fill forward trade_pr and mov_avg_pr
window_spec = Window.partitionBy("symbol", "exchange") \
                    .orderBy("trade_dt", "event_tm") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 4.4.1.3 Populate The Latest trade_pr and mov_avg_pr
quote_union_update = quote_union \
    .withColumn("latest_trade_pr", last("trade_pr", ignorenulls=True).over(window_spec)) \
    .withColumn("latest_mov_avg_pr", last("mov_avg_pr", ignorenulls=True).over(window_spec)) 

quote_union_update.createOrReplaceTempView("quote_union_update")

#4.4.1.4 Filter For Quote Records
quote_update = quote_union_update.filter("rec_type = 'Q'")  # keep only quotes

quote_update.createOrReplaceTempView("quote_update")

# Step 3: Show results or use as needed
display(quote_update.select(
    "trade_dt","symbol", "exchange", "event_tm", "bid_pr", "ask_pr", "arrival_tm",
    "latest_trade_pr", "latest_mov_avg_pr"))

# %%
#4.4.2 Join With Table temp_last_trade To Get The Prior Day Close Price
from pyspark.sql.functions import broadcast, expr

# Load the prior day close price table
last_trade_df = spark.sql("SELECT * FROM temp_last_trade")

# Broadcast join with corrected column name
quote_final = quote_update.alias("q").join(
    broadcast(last_trade_df).alias("lt"),
    on=["symbol", "exchange"],
    how="left"
).select(
    "q.trade_dt",
    "q.symbol",
    "q.event_tm",
    "q.event_seq_nb",
    "q.exchange",
    "q.bid_pr",
    "q.bid_size",
    "q.ask_pr",
    "q.ask_size",
    "q.latest_trade_pr",
    "q.latest_mov_avg_pr",
    expr("q.bid_pr - lt.last_pr").alias("bid_pr_mv"),
    expr("q.ask_pr - lt.last_pr").alias("ask_pr_mv")
)

quote_final.createOrReplaceTempView("quote_final")

# Preview the final result
display(quote_final)


# %%
# Write The Final Dataframe Into Azure Blob Storage At Corresponding Partition
#trade_date_str = trade_date.strftime("%Y-%m-%d")
output_path = f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/quote-trade-analytical/date={trade_date}"
quote_final.write.mode("overwrite").parquet(output_path)


