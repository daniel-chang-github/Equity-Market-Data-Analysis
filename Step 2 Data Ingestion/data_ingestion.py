#%%
from distutils.command.config import config
from typing import List
import decimal
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DecimalType
import config

#%%
#Parse csv fuction
def parse_csv(line:str):
    try:    
        record_type_pos = 2
        record = line.split(",")
        
        # [logic to parse records]

        if record[record_type_pos] == "T":            
            event = [record[0],\
                     record[1],\
                     record[2],\
                    #  record[2],\
                     record[3],\
                     record[4],\
                     int(record[5]),\
                     record[6],\
                     decimal.Decimal(record[7]),\
                     int(record[8]),\
                     decimal.Decimal(record[9]),\
                     int(record[10]),\
                     "T"]
            return event
        elif record[record_type_pos] == "Q":
            event = [record[0],\
                     record[1],\
                     record[2],\
                     record[3],\
                     record[4],\
                     int(record[5]),\
                     record[6],\
                     decimal.Decimal(record[7]),\
                     int(record[8]),\
                     decimal.Decimal(record[9]),\
                     int(record[10]),\
                     "Q"]
            
            return event
    except Exception as e:
        event = [None,None,None,None,None,None,None,None,None,None,None,"B"]
        return event

#%%
#Parse json fuction
def parse_json(line:str):
    try:    
        record = json.loads(line)
        record_type = record['event_type']
                                            
        # [logic to parse records]
        if record_type == "T":            
            event = [record['trade_dt'],\
                     record['file_tm'],\
                     record['event_type'],\
                     record['symbol'],\
                     record['event_tm'],\
                     int(record['event_seq_nb']),\
                     record['exchange'],\
                     decimal.Decimal(record['bid_pr']),\
                     int(record['bid_size']),\
                     decimal.Decimal(record['ask_pr']),\
                     int(record['ask_size']),\
                     "T"]
            return event
        elif record_type == "Q":
            event = [record['trade_dt'],\
                     record['file_tm'],\
                     record['event_type'],\
                     record['symbol'],\
                     record['event_tm'],\
                     int(record['event_seq_nb']),\
                     record['exchange'],\
                     decimal.Decimal(record['bid_pr']),\
                     int(record['bid_size']),\
                     decimal.Decimal(record['ask_pr']),\
                     int(record['ask_size']),\
                     "Q"]
            return event
    except Exception as e:
        event = [None,None,None,None,None,None,None,None,None,None,None,"B"]
        return event

#%%
#data from two different sources will be put into this schema.
common_event = StructType([\
                    StructField("trade_dt", StringType(),True),\
                    StructField("arrival_tm", StringType(),True),\
                    StructField("rec_type", StringType(),True),\
                    StructField("symbol", StringType(),True),\
                    StructField("event_tm", StringType(),True),\
                    StructField("event_seq_nb", IntegerType(),True),\
                    StructField("trade_pr", StringType(),True),\
                    StructField("bid_pr", DecimalType(),True),\
                    StructField("bid_size", IntegerType(),True),\
                    StructField("ask_pr", DecimalType(),True),\
                    StructField("ask_size", IntegerType(),True),\
                    StructField("partition", StringType(),True)\
                          ])

#%%
storage_account_name =config.storage_account_access_key
storage_account_access_key = config.storage_account_access_key
container_name = config.container_name

#supplying the jar files necessary to connect to Azure blob storage
AZURE_STORAGE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/azure-storage-8.6.5.jar'
HADOOP_AZURE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/hadoop-azure-3.3.0.jar'
JSON_CON = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-ajax-9.4.0.v20180619.jar'
UTIL = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-9.4.0.v20180619.jar'

#%%
from pyspark.sql import SparkSession
# spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark = SparkSession.builder.config('spark.jars', f"{AZURE_STORAGE_JAR_PATH},{HADOOP_AZURE_JAR_PATH},{JSON_CON},{UTIL}").getOrCreate()
spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name),"{}".format( storage_account_access_key))

dates = ['2020-08-05','2020-08-06']
csvlist = []
jsonlist = []

for dt in dates:
    rawcsv = spark.sparkContext.textFile("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'data/csv/{}/NYSE/*.txt'.format(dt)))
    rawjson = spark.sparkContext.textFile("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'data/json/{}/NASDAQ/*.txt'.format(dt)))
    
    parsed_csv = rawcsv.map(lambda line: parse_csv(line))
    parsed_json = rawjson.map(lambda line: parse_json(line))

    datacsv = spark.createDataFrame(parsed_csv, common_event)
    datajson = spark.createDataFrame(parsed_json, common_event)

    csvlist.append(datacsv)
    jsonlist.append(parsed_json)



#%%
print(csvlist)


#%% 
dates = ['2020-08-05','2020-08-06']
csvlist = []
jsonlist = []

spark = SparkSession.builder.getOrCreate()
for dt in dates:
    rawcsv = spark.sparkContext.textFile("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'data/csv/{}/NYSE/*.txt'.format(dt)))
    rawjson = spark.sparkContext.textFile("wasbs://{}@{}.blob.core.windows.net/{}".format(container_name,storage_account_name, 'data/json/{}/NASDAQ/*.txt'.format(dt)))

    parsedcsv = rawcsv.map(lambda line: parse_csv(line))
    parsedjson = rawjson.map(lambda line: parse_json(line))

    
    datacsv = spark.createDataFrame(parsedcsv, common_event)
    datajson = spark.createDataFrame(parsedjson, common_event)

    csvlist.append(datacsv)
    jsonlist.append(datajson)

csv_data = csvlist[0].union(csvlist[1])
json_data= jsonlist[0].union(jsonlist[1])
all_data = csv_data.union(json_data)
all_data.show(800)

all_data.write.partitionBy("partition").mode("overwrite").parquet("/Users/daniel/Desktop/Data_Engineering/Guided Capstone/data/output")


