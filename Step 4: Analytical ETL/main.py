from pyspark.sql import SparkSession
from pyspark.sql.types import *
import config

#supplying the jar files necessary to connect to Azure blob storage
AZURE_STORAGE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/azure-storage-8.6.5.jar'
HADOOP_AZURE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/hadoop-azure-3.3.0.jar'
JSON_CON = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-ajax-9.4.0.v20180619.jar'
UTIL = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-9.4.0.v20180619.jar'

#Azure blob storage info
storage_account_name =config.storage_account_name
storage_account_access_key = config.storage_account_access_key
container_name = config.container_name

# Load the spark session with the jars
spark = SparkSession.builder.config('spark.jars', f"{AZURE_STORAGE_JAR_PATH},{HADOOP_AZURE_JAR_PATH},{JSON_CON},{UTIL}").master('local').appName('Analytical_ETL_spark').getOrCreate()
spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name),"{}".format( storage_account_access_key))
# Read parqut files from local storage
df_trade = spark.read.parquet(("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'output/trade/trade_dt={}/*.parquet'.format("2020-08-06"))))

# Creating staging table for current day's trades
df_trade.createOrReplaceTempView("tmp_trade_moving_avg")

# Calculating the moving average for last 30 minutes
mov_avg_df = spark.sql("""select symbol, exchange, trade_dt,event_tm, event_seq_nb, trade_pr,
                        AVG(trade_pr) OVER(PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) 
                        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr
                        from tmp_trade_moving_avg""")
# Drop the Hive table if it already exists
# spark.sql("DROP TABLE IF EXISTS tmp_trade_moving_avg")
# Save the data frame as table in hive .mode('overwrite') doesn't work. Why?
# mov_avg_df.write.mode('overwrite').saveAsTable("tmp_trade_moving_avg")

mov_avg_df.createOrReplaceTempView("tmp_trade_moving_avg")


# mov_avg_df.write.csv('mov_avg.csv')
mov_avg_df.filter(mov_avg_df.symbol=='SYMA').show()
# print(spark.sql( ''' select * from tmp_trade_moving_avg'''))

# Create staging table for the prior day's last trade
df_last_trade = spark.read.parquet(("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'output/trade/trade_dt={}/*.parquet'.format("2020-08-05"))))
df_last_trade.createOrReplaceTempView("temp_last_trade")
# Calculating the moving average for last 30 minutes
last_pr_df = spark.sql("""select symbol, trade_dt,exchange, event_tm, event_seq_nb, trade_pr,
                        AVG(trade_pr) OVER(PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) 
                        RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as last_pr
                        from temp_last_trade""")
# # DROP HIVE TABLE IF EXISTS
# spark.sql("DROP TABLE IF EXISTS temp_last_trade")
# Save the data frame as table in hive

last_pr_df.createOrReplaceTempView("temp_last_trade")
last_pr_df.filter(last_pr_df.symbol=='SYMA').show()

# # Populating the latest trade and latest moving average tade price to the quote records
quotes = spark.read.parquet(("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'output/quote/trade_dt=*/*.parquet')))
quotes.createOrReplaceTempView("quotes")


# # Union quotes and trades into a single union view
quote_union = spark.sql("""
SELECT trade_dt,rec_type,symbol,event_tm,event_seq_nb,exchange,bid_pr,bid_size,ask_pr,
ask_size,null as trade_pr,null as mov_avg_pr FROM quotes
UNION
SELECT trade_dt,"T" as rec_type,symbol,event_tm,event_seq_nb,exchange,null as bid_pr,null as bid_size,null as ask_pr,
null as ask_size,trade_pr,mov_avg_pr FROM tmp_trade_moving_avg
""")

quote_union.createOrReplaceTempView("quote_union")

# Populate the latest trade_pr and move_avg_pr
quote_union_update = spark.sql("""
select *,
last_value(trade_pr,true) OVER(PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) DESC) as last_trade_pr,
last_value(mov_avg_pr,true) OVER(PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) DESC) as last_mov_avg_pr
from quote_union
""")
quote_union_update.createOrReplaceTempView("quote_union_update")
# quote_union_update.filter(quote_union_update.symbol=='SYMA').show()

# Filter for quote records
quote_update = spark.sql("""
select trade_dt, rec_type, symbol, event_tm, event_seq_nb, exchange, bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr
from quote_union_update
where rec_type = 'Q'
""")
quote_update.createOrReplaceTempView("quote_update")

# quote_update.filter(quote_update.symbol=='SYMA').show()

quote_final = spark.sql("""
    select trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_pr, bid_size, ask_pr, ask_size, last_trade_pr, last_mov_avg_pr,
    bid_pr - close_pr as bid_pr_mv,
    ask_pr - close_pr as ask_pr_mv
    from (
    select /* + BROADCAST(t) */
    q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange, q.bid_pr, q.bid_size, q.ask_pr, q.ask_size, q.last_trade_pr,q.last_mov_avg_pr,
    t.last_pr as close_pr
    from quote_update q
    left outer join temp_last_trade t on
    (q.symbol = t.symbol and q.exchange = t.exchange))
""")

# quote_final.filter(quote_update.symbol=='SYMA').show()


# Write to Azure Blob Storage.
quote_final.write.parquet( ("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'step_4_output/')) )