import csv
import os
import datetime
import argparse
from kafka import KafkaProducer
from parser import User, DATETIME_FORMAT

KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_PORT = os.getenv("KAFKA_PORT")
KAFKA_TOPIC = "blacklist"


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", dest="f", type=str, required=True)
    cli_args = arg_parser.parse_args()
    
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), f"data/{cli_args.f}"
    )

    producer = KafkaProducer(
        bootstrap_servers=f"{KAFKA_HOST}:{KAFKA_PORT}",
    )

    with open(path, newline="\n") as f:
        users = list(csv.DictReader(f, delimiter=",", quotechar='"'))

    for user in users:
        user["id"] = int(user["id"])  # standart csv reader is not that powerful :(
        user["datetime"] = datetime.datetime.strptime(user["datetime"], DATETIME_FORMAT)
        producer.send(topic=KAFKA_TOPIC, **User(**user).serialize())

    producer.flush()
    producer.close()
