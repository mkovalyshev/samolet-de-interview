# Interview: DE at Samolet

All environment variables (connections mostly) can be set by running `env.sh` script.

## 1. [Alfa.kz](alfa.kz) parser  
- stored at `src/parser`
- requires CLI argument `-n` defining number of pages to parse
- writes products in `phones/telefony-i-smartfony` category to `src/parser/data/output_*unixtime*.csv` file, skips product with no RAM or memory data

## 2. Hadoop ETL operation
- stored at `src/hadoop`
- requires CLI argument `-f` defining filename of input from `src/parser/data` folder
- transforms data (title → brand + model, average price over model)
- loads data to HDFS as `smartphones.parquet` and to PostgreSQL `smartphones` table

## 3 Binary data parser
- stored at `src/kafka/parser.py`
- extracts data from `data/data.bin`
- loads data to `data/users_*unixtime*.csv`

## 4. Kafka scripts

### 4.1 Kafka producer
- stored at `src/kafka/producer.py`
- requires CLI argument `-f` defining filename of input from `src/kafka/data` folder
- sends messages to Kafka `blacklist` topic, message key is `id`, value is a UTF-8 encoded JSON string
### 4.2 Kafka consumer
- stored at `src/kafka/consumer.py`
- gets data from Kafka `blacklist` topic (no offset, gets all messages)
- writes data to PostgreSQL `blacklist` table

## 5. REST API
- stored at `src/api`, has its own venv and requirements.txt
- built and run with `docker-compose up`
- PostgreSQL connection is set in `docker-compose.yml` environment variables
- each result is logged into `logs` table at PostgreSQL
- gets JSON with `ctn`, `iin`, `smartphoneID`, `income` and `creationDate` params
- returns JSON with `result` and optional `reason` params
