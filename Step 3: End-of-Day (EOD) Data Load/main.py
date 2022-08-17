#%%
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from azure.storage.blob import BlockBlobService
import config
import os
spark = SparkSession.builder.getOrCreate()

dir = 'data/input/'

def applyLatest(df, type):
    #trades
    if type == "T":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("arrival_tm").alias("latest_trade"))

        #Renaming the column names of the tables to avoid errors.
        df_grouped_r = df_grouped.select(*(col(x).alias(x + '_df_grouped') for x in df_grouped.columns))
        df_r = df.select(*(col(x).alias(x + '_df') for x in df.columns))

        final = df_grouped_r.join(df_r, (df_r.event_seq_nb_df ==df_grouped_r.event_seq_nb_df_grouped)  & (df_r.symbol_df ==df_grouped_r.symbol_df_grouped)  & (df_r.trade_dt_df ==df_grouped_r.trade_dt_df_grouped) &(df_r.arrival_tm_df == df_grouped_r.latest_trade_df_grouped)       )

        return final
    #quotes
    elif type == "Q":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "event_seq_nb").agg(max("arrival_tm").alias("latest_quote"))

        #Renaming the column names of the tables to avoid errors.
        df_grouped_r = df_grouped.select(*(col(x).alias(x + '_df_grouped') for x in df_grouped.columns))
        df_r = df.select(*(col(x).alias(x + '_df') for x in df.columns))
        
        final = df_grouped_r.join(df_r, (df_r.event_seq_nb_df ==df_grouped_r.event_seq_nb_df_grouped)  & (df_r.symbol_df ==df_grouped_r.symbol_df_grouped)  & (df_r.trade_dt_df ==df_grouped_r.trade_dt_df_grouped) &(df_r.arrival_tm_df == df_grouped_r.latest_quote_df_grouped)       )
        
        return final

#Read and select data
trade_common = spark.read.option('header',True).parquet('data/input/partition=T/*')
trade = trade_common.select('rec_type', 'trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
quote_common = spark.read.option('header',True).parquet("data/input/partition=Q/*")
quote = quote_common.select ("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

trade_corrected = applyLatest(trade, 'T')
quote_corrected = applyLatest(quote, 'Q')

trades_080520 = trade_corrected.where(trade_corrected.trade_dt_df_grouped == "2020-08-05")
trades_080620 = trade_corrected.where(trade_corrected.trade_dt_df_grouped == "2020-08-06")
quote_080520 = quote_corrected.where(quote_corrected.trade_dt_df_grouped == "2020-08-05")
quote_080620 = quote_corrected.where(quote_corrected.trade_dt_df_grouped == "2020-08-06")

#  Write The Trade Dataset 

trades_080520.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-05'))
trades_080620.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-06'))
quote_080520.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-05'))
quote_080620.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-06'))

# Upload data to Azure Blob Storage
blob_service = BlockBlobService(config.storage_account_name, config.storage_account_access_key)

path_remove = "/Users/daniel/Desktop/Data_Engineering/Guided Capstone/Step 3 End-of-Day (EOD) Data Load/data/"
local_path = "/Users/daniel/Desktop/Data_Engineering/Guided Capstone/Step 3 End-of-Day (EOD) Data Load/data/output"

for r,d,f in os.walk(local_path):        
    if f:
        for file in f:
            file_path_on_azure = os.path.join(r,file).replace(path_remove,"")
            file_path_on_local = os.path.join(r,file)
            blob_service.create_blob_from_path(config.container_name,file_path_on_azure,file_path_on_local)            

