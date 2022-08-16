# Step 1 Table Design


![Guided Capstone Project (1)](https://user-images.githubusercontent.com/81652137/180630254-c06f23f3-fd7c-409f-bee9-90a5f026c676.png)


# Step 2 Data Ingestion
### Output result

![image](https://user-images.githubusercontent.com/81652137/182288372-56fdeab4-cb0c-4331-9b81-da429438e1ce.png)

### Parquet output

![image](https://user-images.githubusercontent.com/81652137/182288303-3aa33505-26d5-4583-86c1-a2a6e619fc22.png)

# Step 3 End-of-Day (EOD) Data Load

## Apply Data Correction

In the exchange dataset, you can uniquely identify a record by the combination of trade_dt, symbol, exchange, event_tm, event_seq_nb. However, the exchange may correct an error in any submitted record by sending a new record with the same uniqueID. Such records will come with later arrival_tm. You must ensure you only accept the one with the most recent arrival_tm.

## Result

![Screen Shot 2022-08-15 at 4 14 29 PM](https://user-images.githubusercontent.com/81652137/184787337-2780d809-6639-4a3c-883b-37068de9067c.png)

