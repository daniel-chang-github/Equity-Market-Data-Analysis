#%%
from pyspark.sql.types import *
# import pyspark.sql.functions as f 
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

dir = 'data/output/'

def applyLatest(df, type):
    #trades
    if type == "T":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "arrival_tm", "event_seq_nb").agg(max("arrival_tm").alias("latest_trade"))
        # df_joined = df_grouped.join(df.select("event_tm", "exchange", "trade_pr","event_seq_nb", "symbol", "trade_dt" ), ["event_seq_nb", "symbol", "trade_dt" ] , "inner")
        # df_joined = df_grouped.join(df.select("event_tm", "exchange", "trade_pr","event_seq_nb", "symbol", "trade_dt" ), ["event_seq_nb", "symbol", "trade_dt" ] , "inner")
        df_joined = df_grouped.join(df.select("event_tm", "exchange", "trade_pr"), [ (df.arrival_tm == df_grouped.latest_trade) & (df.event_seq_nb == df_grouped.event_seq_nb) & (df.symbol == df_grouped.symbol)  & (df.trade_dt == df_grouped.trade_dt) ], 'inner')
        df_final = df_joined.select("trade_dt", "rec_type","event_tm", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_trade", "event_seq_nb", "arrival_tm", "trade_pr").orderBy("trade_dt", "symbol", "event_seq_nb")


        print('df')
        df.filter((df.event_seq_nb == 10) & (df.trade_dt == '2020-08-05') & (df.symbol == 'SYMA') ).show()
        print('df grouped') 
        df_grouped.filter((df_grouped.event_seq_nb == 10) & (df_grouped.trade_dt == '2020-08-05') & (df_grouped.symbol == 'SYMA') ).show()
        print('df_filtered')
        df.filter((df.event_seq_nb == 10) & (df.trade_dt == '2020-08-05') & (df.symbol == 'SYMA')).show()

        print('df_joined')
        df_joined.filter((df_joined.event_seq_nb == 10) & (df_joined.trade_dt == '2020-08-05') & (df_joined.symbol == 'SYMA') ).show()
        print('df_final')
        df_final.filter((df_final.event_seq_nb == 10) & (df_final.trade_dt == '2020-08-05') & (df_final.stock_symbol == 'SYMA') ).show()


        # return df_final
    #quotes
    elif type == "Q":
        df_grouped = df.groupBy("trade_dt", "rec_type", "symbol", "event_seq_nb").agg(max("arrival_tm").alias("latest_quote"))

        df_grouped_r = df_grouped.select(*(col(x).alias(x + '_df_grouped') for x in df_grouped.columns))
        df_r = df.select(*(col(x).alias(x + '_df') for x in df.columns))
        
        # df_joined = df_grouped.join(df.select("event_tm", "exchange", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size"),
        # [(df['arrival_tm'] == df_grouped['latest_quote']) & (df['trade_dt'] == df_grouped['trade_dt']) & (df['symbol'] == df_grouped['symbol'])& 
        # (df['event_seq_nb'] == df_grouped['event_seq_nb'])   & (df['rec_type'] == df_grouped['rec_type'])   ], 'inner').select('df_grouped.*')

        # df_joined = df_grouped.join(df.select("event_tm", "exchange", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size",'event_seq_nb','trade_dt','symbol'), ['event_seq_nb', 'trade_dt', 'symbol' ], "inner")
        # df_final = df_joined.select("trade_dt", "rec_type", col("symbol").alias("stock_symbol"), col("exchange").alias("stock_exchange"), "latest_quote", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size").orderBy("trade_dt", "symbol", "event_seq_nb")

        print('df')
        df.filter((df.event_seq_nb == 1) & (df.trade_dt == '2020-08-05') & (df.symbol == 'SYMC') ).show()
        print('df grouped') 
        df_grouped.filter((df_grouped.event_seq_nb == 1) & (df_grouped.trade_dt == '2020-08-05') & (df_grouped.symbol == 'SYMC') ).show()
        # print('df_joined')
        # print(df_joined.filter((df_joined.event_seq_nb == 2) & (df_joined.trade_dt == '2020-08-05') & (df_joined.symbol == 'SYMC') ).count())
        
        # print("df_grouped_r")
        # df_grouped_r.filter((df_grouped_r.event_seq_nb_df_grouped==1)  ).show()

        final = df_grouped_r.join(df_r, (df_r.event_seq_nb_df ==df_grouped_r.event_seq_nb_df_grouped)  & (df_r.symbol_df ==df_grouped_r.symbol_df_grouped)  & (df_r.trade_dt_df ==df_grouped_r.trade_dt_df_grouped) &(df_r.arrival_tm_df == df_grouped_r.latest_quote_df_grouped)       )
        final.filter( (final.symbol_df == 'SYMA') & (final.event_seq_nb_df == 1)  &  (final.trade_dt_df == '2020-08-05')).show()  

        # print('df_final')
        # df_final.filter((df_final.event_seq_nb == 1) & (df_final.trade_dt == '2020-08-05') & (df_final.stock_symbol == 'SYMB') ).show()


        # return df_joined.where(df_joined.symbol == "SYMA").orderBy('event_seq_nb')


#Read and select data
trade_common = spark.read.option('header',True).parquet('data/output/partition=T/*')
trade = trade_common.select('rec_type', 'trade_dt', 'symbol', 'exchange', 'event_tm', 'event_seq_nb', 'arrival_tm', 'trade_pr')
quote_common = spark.read.option('header',True).parquet("data/output/partition=Q/*")
quote = quote_common.select ("trade_dt", "rec_type", "symbol", "exchange", "event_tm", "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")

# trade_corrected = applyLatest(trade, 'T')
quote_corrected = applyLatest(quote, 'Q')
# trade_corrected.show()
# quote_corrected.show()

# trades_080520 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-05")
# trades_080620 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-06")
# quote_080520 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-05")
# quote_080620 = trade_corrected.where(trade_corrected.trade_dt == "2020-08-06")

#  Write The Trade Dataset 

# trades_080520.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-05'))
# trades_080620.write.mode("overwrite").parquet('data/output/trade/trade_dt={}'.format('2020-08-06'))
# quote_080520.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-05'))
# quote_080620.write.mode("overwrite").parquet('data/output/quote/trade_dt={}'.format('2020-08-06'))
