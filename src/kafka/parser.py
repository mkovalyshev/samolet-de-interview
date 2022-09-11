import os
import csv
import json
import time
import datetime
from dataclasses import dataclass


USER_LENGTH = 21
BIN_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/data.bin")

CSV_OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), f"data/users_{int(time.time())}.csv"
)

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@dataclass
class User:
    id: str
    phone: str
    datetime: datetime.datetime

    @staticmethod
    def deserialize(msg: bytes) -> tuple:
        """
        returns tuple of objects from bytes
        """

        total = len(msg) // USER_LENGTH
        cnt = 0

        while cnt < total:
            user_message = msg[cnt * USER_LENGTH : cnt * USER_LENGTH + USER_LENGTH]
            cnt += 1

            yield User(
                id=int.from_bytes(user_message[0:4], byteorder="big"),
                phone=user_message[4:14].decode("utf-8"),
                datetime=datetime.datetime(
                    int.from_bytes(user_message[14:16], byteorder="little"),
                    int(user_message[16]),
                    int(user_message[17]),
                    int(user_message[18]),
                    int(user_message[19]),
                    int(user_message[20]),
                ),
            )

    def serialize(self) -> dict:
        """
        returns dict with bytes representing key and value for Kafka
        """

        key = self.id.to_bytes(2, byteorder="big")

        value = json.dumps(
            {
                "id": self.id,
                "phone": self.phone,
                "datetime": self.datetime.strftime(DATETIME_FORMAT),
            }
        ).encode("utf-8")

        return {"key": key, "value": value}


if __name__ == "__main__":

    with open(BIN_PATH, "rb") as f:
        data = f.read()

    users = [user.__dict__ for user in User.deserialize(data)]

    with open(CSV_OUTPUT_PATH, "w", newline="\n") as f:

        title = [field for field in User.__dataclass_fields__]

        writer = csv.DictWriter(
            f, title, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )

        writer.writeheader()
        writer.writerows(users)
