from dataclasses import dataclass
import os
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

HADOOP_HOST = "localhost"
HADOOP_PORT = "9000"

HADOOP_PATH = "user/kovalyshev/samolet"
HADOOP_TABLE = "smartphones.parquet"

RELATIVE_FILEPATH = "parser/data"

POSTGRES_USER = "samolet"
POSTGRES_PASSWORD = "1111"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DATABASE = "postgres"
POSTGRES_DRIVER = "/Users/kovalyshev/postgresql-42.5.0.jar"

POSTGRES_TABLE = "smartphones"

# did not set env variables for reproducibility


def extract(spark: object, path: str, filename: str) -> object:
    """
    reads local .csv from path/filename, returns instance of pyspark DataFrame
    """

    path = os.path.join(path, filename)

    data = spark.read.format("csv").option("header", "true").load(f"file://{path}")

    return data


def transform(raw_data: object) -> object:
    """
    transforms pyspark DataFrame (avg price over model + autoinc id)
    """

    data = (
        raw_data.select(
            F.split("title", " ", 0)[0].alias("smartphone_brand"),
            F.array_join(
                F.slice(F.split("title", " ", 0), 2, F.length("title") - 1), " "
            ).alias("smartphone_model"),
            "price",
        )
        .groupBy("smartphone_brand", "smartphone_model")
        .agg(F.avg("price").cast("int").alias("smartphone_price"))
        .select(
            F.monotonically_increasing_id().alias("smartphone_id"),
            "smartphone_brand",
            "smartphone_model",
            F.col("smartphone_price").alias("smartphone_price"),
        )
    )

    return data


def to_hadoop(
    data: object,
    path: str,
    table: str,
    host: str = HADOOP_HOST,
    port: int = HADOOP_PORT,
) -> None:
    """
    loads pyspark DataFrame to HDFS (located at host:port) as .parquet
    """

    data.write.mode("overwrite").option("header", "true").parquet(
        f"hdfs://{host}:{port}/{path}/{table}"
    )


def to_database(
    data: object,
    table: str,
    host: str = POSTGRES_HOST,  # too many arguments, would change to DBConnection class
    port: int = POSTGRES_PORT,
    database: str = POSTGRES_DATABASE,
    user: str = POSTGRES_USER,
    password: str = POSTGRES_PASSWORD,
) -> None:
    """
    writes data to RDB
    """

    data.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{host}:{port}/{database}"
    ).option("driver", "org.postgresql.Driver").option("dbtable", table).option(
        "user", user
    ).option(
        "password", password
    ).mode(
        "overwrite"
    ).save()


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-f", dest="f", type=str, required=True)
    cli_args = arg_parser.parse_args()

    spark = (
        SparkSession.builder.master("local")
        .appName("samolet-hadoop")
        .config("spark.jars", POSTGRES_DRIVER)
        .getOrCreate()
    )

    filename = cli_args.f

    filepath = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), RELATIVE_FILEPATH
    )

    raw_data = extract(spark, filepath, filename)

    data = transform(raw_data)

    to_hadoop(data, path=HADOOP_PATH, table=HADOOP_TABLE)

    to_database(data, POSTGRES_TABLE)

    spark.stop()
