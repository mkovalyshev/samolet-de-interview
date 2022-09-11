import os
from models import Log
from flask import request
import datetime
import psycopg2
import json

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

postgres_credentials = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "database": os.getenv("PG_DATABASE"),
}


def log(func):
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
