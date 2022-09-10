import json
import csv
import sys
import os
import datetime
from kafka import KafkaProducer

sys.path.append(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))

from parser import User, DATETIME_FORMAT

KAFKA_HOST = "localhost"
KAFKA_PORT = 9092
KAFKA_TOPIC = "blacklist"

PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data/users_1662847835.csv"
)

if __name__ == "__main__":

    producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
    )

    with open(PATH, newline="\n") as f:
        users = list(csv.DictReader(f, delimiter=",", quotechar='"'))

    for user in users:
        user["id"] = int(user["id"])  # standart csv reader is not that powerful :(
        user["datetime"] = datetime.datetime.strptime(user["datetime"], DATETIME_FORMAT)
        producer.send(topic=KAFKA_TOPIC, **User(**user).serialize())

    producer.flush()
    producer.close()
