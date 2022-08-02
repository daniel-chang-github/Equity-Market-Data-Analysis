#%%
from distutils.command.config import config
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import config
from datetime import datetime

#%%
#Parse csv fuction
def parse_csv(line:str):
    record_type_pos = 2
    record = line.split(",")
    try:    
        # [logic to parse records]
        if record[record_type_pos] == 'T':
            event = (datetime.strptime(record[0], '%Y-%m-%d').date(),
                      record[2],
                      record[3],
                      record[6],
                      datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                      int(record[5]),
                      datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                      float(record[7]),
                      None,
                      None,
                      None,
                      None,
                      record[2])
            return event
        elif record[record_type_pos] == 'Q':
            event = (datetime.strptime(record[0], '%Y-%m-%d').date(),
                    record[2],
                    record[3],
                    record[6],
                    datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'),
                    int(record[5]),
                    datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f'),
                    None,
                    float(record[7]),
                    int(record[8]),
                    float(record[9]),
                    int(record[10]),
                    record[2])
            
            return event
    except Exception as e:
        event = [None,None,None,None,None,None,None,None,None,None,None,None,"B"]
        return event    

#%%
#Parse json fuction
def parse_json(line:str):
    try:    
        record = json.loads(line)
        record_type = record['event_type']
                                            
        # [logic to parse records]
        if record_type == "T":            
            event = (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(),
                    record_type,
                    line['symbol'],
                    line['exchange'],
                    datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                    line['event_seq_nb'],
                    datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                    line['price'],
                    None,
                    None,
                    None,
                    None,
                    record_type)
            return event
        elif record_type == "Q":
            event = (datetime.strptime(line['trade_dt'], '%Y-%m-%d').date(),
                    record_type,
                    line['symbol'],
                    line['exchange'],
                    datetime.strptime(line['event_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                    line['event_seq_nb'],
                    datetime.strptime(line['file_tm'], '%Y-%m-%d %H:%M:%S.%f'),
                    None,
                    line['bid_pr'],
                    line['bid_size'],
                    line['ask_pr'],
                    line['ask_size'],
                    record_type)
            return event
    except Exception as e:

        event = [None,None,None,None,None,None,None,None,None,None,None,None,"B"]
        return event

#%%
#data from two different sources will be put into this schema.
common_event = StructType([StructField('trade_dt', DateType(), True),
                           StructField('rec_type', StringType(), True),
                           StructField('symbol', StringType(), True),
                           StructField('exchange', StringType(), True),
                           StructField('event_tm', TimestampType(), True),
                           StructField('event_seq_nb', IntegerType(), True),
                           StructField('arrival_tm', TimestampType(), True),
                           StructField('trade_pr', FloatType(), True),
                           StructField('bid_pr', FloatType(), True),
                           StructField('bid_size', IntegerType(), True),
                           StructField('ask_pr', FloatType(), True),
                           StructField('ask_size', IntegerType(), True),
                           StructField('partition', StringType(), True)])
#%%
storage_account_name =config.storage_account_name
storage_account_access_key = config.storage_account_access_key
container_name = config.container_name

#supplying the jar files necessary to connect to Azure blob storage
AZURE_STORAGE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/azure-storage-8.6.5.jar'
HADOOP_AZURE_JAR_PATH = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/hadoop-azure-3.3.0.jar'
JSON_CON = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-ajax-9.4.0.v20180619.jar'
UTIL = '/Users/daniel/Desktop/Data_Engineering/Guided Capstone/jars/jetty-util-9.4.0.v20180619.jar'

#%%
from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.jars', f"{AZURE_STORAGE_JAR_PATH},{HADOOP_AZURE_JAR_PATH},{JSON_CON},{UTIL}").getOrCreate()
spark.conf.set("fs.azure.account.key.{}.blob.core.windows.net".format(storage_account_name),"{}".format( storage_account_access_key))


#%% 
dates = ['2020-08-05','2020-08-06']
csvlist = []
jsonlist = []

# spark = SparkSession.builder.getOrCreate()
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
all_data.show()

all_data.write.partitionBy("partition").mode("overwrite").parquet("/Users/daniel/Desktop/Data_Engineering/Guided Capstone/data/output")


