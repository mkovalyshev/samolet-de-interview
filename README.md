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
