# Interview: DE at Samolet

## 1. [Alfa.kz](alfa.kz) parser  
- stored at `src/parser`
- called with `python src/parser -n #`, where # = number of pages to parse  
- category is hardcoded to phones and smartphones (`phones/telefony-i-smartfony`)  

## 2. Hadoop ETL operation
- stored at `src/hadoop`
- extracts file from `FILEPATH`
- transforms data (title → brand + model, avg price over model)
- loads to HDFS as `smartphones.parquet` and to PostgreSQL `smartphones` table

## 3 Binary data parser
- stored at `src/kafka/parser.py`
- extracts data from `data/data.bin`
- implemented as generator (yields instances of User object)
- loads data to `data/users_*timestamp*.csv`

## 4. Kafka scripts

### 4.1 Kafka producer
- stored at `src/kafka/producer.py`
- extracts data from `data/users_*timestamp*.csv`
- sends messages to Kafka `blacklist` topic, message key is `id`, value is a UTF-8 encoded JSON string
### 4.2 Kafka consumer
- stored at `src/kafka/consumer.py`
- gets data from Kafka `blacklist` topic (no offset, gets all messages)
- writes data to PostgreSQL `blacklist` table

## 5. REST API
- stored at `src/api`
- built with `docker-compose up`
- PostgreSQL connection is set in `docker-compose.yml` environment variables
- result is logged into `logs` table at PostgreSQL
- gets JSON with `ctn`, `iin`, `smartphoneID`, `income` and `creationDate` params
- returns JSON with `result` and optional `reason` params