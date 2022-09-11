import os
import json
import psycopg2
from kafka import KafkaConsumer
from producer import KAFKA_HOST, KAFKA_PORT, KAFKA_TOPIC

POSTGRES_USER = "samolet"
POSTGRES_PASSWORD = "1234"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "postgres"

KAFKA_COMMIT_THRESHOLD = 5

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "SQL/blacklist_DDL.sql"),
    "r",
) as f:
    BLACKLIST_DDL = f.read()

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "SQL/blacklist_index.sql"),
    "r",
) as f:
    BLACKLIST_INDEX = f.read()

with open(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "SQL/blacklist_insert.sql"
    ),
    "r",
) as f:
    BLACKLIST_INSERT = f.read()


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
                BLACKLIST_INSERT.format(
                    id=msg.value.get("id"),
                    phone=msg.value.get("phone"),
                    datetime=msg.value.get("datetime"),
                )
            )

            connection.commit()

        consumer.close()
