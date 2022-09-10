from kafka import KafkaConsumer, TopicPartition
import json
import sys
import os
import psycopg2

sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))

from producer import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC

POSTGRES_USER = "samolet"
POSTGRES_PASSWORD = "1234"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "postgres"

BLACKLIST_DDL = """
create table if not exists public.blacklist (
    id int,
    phone varchar(10),
    datetime timestamp
);
"""

BLACKLIST_INDEX = """
create index if not exists blacklist_id on blacklist(id);
"""

KAFKA_COMMIT_THRESHOLD = 5

if __name__ == "__main__":

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
        key_deserializer=lambda x: int.from_bytes(x, byteorder="big"),
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )

    with psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
    ) as connection:
        cursor = connection.cursor()

        cursor.execute(BLACKLIST_DDL)
        cursor.execute(BLACKLIST_INDEX)

        connection.commit()

        cnt = 0

        for msg in consumer:
            cursor.execute(
                f"""
            insert into blacklist (
                id,
                phone,
                datetime
            ) values (
                {msg.value.get("id")},
                {msg.value.get("phone")},
                '{msg.value.get("datetime")}'
            )
            """
            )

            connection.commit()

        consumer.close()
