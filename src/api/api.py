from dataclasses import dataclass
import datetime
from enum import Enum
import json
from typing import Optional
from unittest import result
from urllib import response
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import os

MONTHLY_PAYMENT_THRESHOLD = 0.2
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

LOG_DDL = """
create table if not exists logs (
    id serial primary key,
    url varchar(255),
    ctn varchar(10),
    result varchar(64),
    log_timestamp timestamp
);
"""

postgres_credentials = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "database": os.getenv("PG_DATABASE"),
}

app = Flask(__name__)


class ResponseResolution(Enum):
    approve = "Одобрение"
    reject = "Отказ"


class RejectReason(Enum):
    blacklist = "Клиент находится в черном списке"
    income = "Клиент не имеет достаточный уровень дохода"


@dataclass
class ResultRequest:
    ctn: str
    iin: str
    smartphoneID: int
    income: int
    creationDate: str


@dataclass
class ResultResponse:
    result: ResponseResolution
    reason: Optional[RejectReason]

    def json(self):
        return {key: value for key, value in self.__dict__.items() if value is not None}


@dataclass
class Log:
    url: str
    ctn: str
    result: str
    log_timestamp: str


def log(func):
    global postgres_credentials

    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)

        log = Log(
            url=request.url,
            ctn=request.get_json().get("ctn"),
            result=json.loads(response.get_data().decode("utf-8")).get("result"),
            log_timestamp=datetime.datetime.now().strftime(DATETIME_FORMAT),
        )

        with psycopg2.connect(**postgres_credentials) as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                insert into logs (
                    url,
                    ctn,
                    result,
                    log_timestamp
                ) values (
                    '{log.url}',
                    '{log.ctn}',
                    '{log.result}',
                    '{log.log_timestamp}'
                )
                """
            )

            connection.commit()

        print(log.__dict__)

        return response

    return wrapper


@app.route("/")
def ping():
    return "This is home, better go to endpoints"


@app.route("/get_result", methods=["POST"])
@log
def get_result():
    """
    docstring
    """

    global postgres_credentials

    result_request = ResultRequest(**request.get_json())

    with psycopg2.connect(**postgres_credentials) as connection:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            f"""
            select count(*) as blacklist
            from public.blacklist
            where True
                and phone = '{result_request.ctn}'
            """
        )

        blacklist_cnt = cursor.fetchone().get("blacklist") > 0

        if blacklist_cnt:
            response = ResultResponse(
                ResponseResolution.reject.value, RejectReason.blacklist.value
            )

            return jsonify(response.json())

        cursor.execute(
            f"""
            select smartphone_price
            from public.smartphones
            where True
                and smartphone_id = {result_request.smartphoneID}
            """
        )

        smartphone_price = cursor.fetchone().get("smartphone_price")

        if smartphone_price / 12 >= result_request.income * MONTHLY_PAYMENT_THRESHOLD:
            response = ResultResponse(
                ResponseResolution.reject.value, RejectReason.income.value
            )

            return jsonify(response.json())

        response = ResultResponse(ResponseResolution.approve.value, None)

        return jsonify(response.json())


if __name__ == "__main__":

    with psycopg2.connect(**postgres_credentials) as connection:
        cursor = connection.cursor()
        cursor.execute(LOG_DDL)
        connection.commit()

    app.run(host="0.0.0.0", port=5000, debug=True)
