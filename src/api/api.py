import os
import psycopg2
from flask import Flask, request, jsonify
from psycopg2.extras import RealDictCursor
from models import ResponseResolution, RejectReason, ResultResponse, ResultRequest
from functions import log

MONTHLY_PAYMENT_THRESHOLD = 0.2


with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "SQL/log_DDL.sql"),
    "r",
) as f:
    LOG_DDL = f.read()

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "SQL/blacklist.sql"),
    "r",
) as f:
    BLACKLIST = f.read()

with open(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "SQL/price.sql"),
    "r",
) as f:
    PRICE = f.read()

postgres_credentials = {
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASS"),
    "database": os.getenv("PG_DATABASE"),
}

app = Flask(__name__)


@app.route("/get_result", methods=["POST"])
@log
def get_result():
    """
    returns payment resolution
    """

    global postgres_credentials

    result_request = ResultRequest(**request.get_json())

    with psycopg2.connect(**postgres_credentials) as connection:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(BLACKLIST.format(result_request.ctn))

        blacklist_cnt = cursor.fetchone().get("blacklist") > 0

        if blacklist_cnt:
            response = ResultResponse(
                ResponseResolution.reject.value, RejectReason.blacklist.value
            )

            return jsonify(response.json())

        cursor.execute(PRICE.format(result_request.smartphoneID))

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
