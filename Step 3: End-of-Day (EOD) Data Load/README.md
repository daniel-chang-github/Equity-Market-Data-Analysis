## Goal
Practice the data normalization process and using cloud storage with the Spark output.

## Apply Data Correction

In the exchange dataset, you can uniquely identify a record by the combination of trade_dt, symbol, exchange, event_tm, event_seq_nb. However, the exchange may correct an error in any submitted record by sending a new record with the same uniqueID. Such records will come with later arrival_tm. You must ensure you only accept the one with the most recent arrival_tm.

## Result

![Screen Shot 2022-08-15 at 4 14 29 PM](https://user-images.githubusercontent.com/81652137/184787337-2780d809-6639-4a3c-883b-37068de9067c.png)
