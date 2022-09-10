import os
import pyspark
from pyspark.sql import SparkSession

HADOOP_HOST = "localhost"
HADOOP_PORT = "9000"

RELATIVE_FILEPATH = "parser/data"
FILENAME = "output_1662809896.csv"

FILEPATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), RELATIVE_FILEPATH
)


def extract(spark: object, path: str = FILEPATH, filename: str = FILENAME) -> object:
    """
    reads local .csv from path/filename, returns instance of pyspark DataFrame
    """

    path = os.path.join(path, filename)

    data = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load(f"file://{path}")

    return data


def load(
    data: object,
    host: str = HADOOP_HOST,
    port: int = HADOOP_PORT,
    path: str = FILEPATH,
    filename: str = FILENAME,
) -> None:
    """
    loads pyspark DataFrame to HDFS (located at host:port) as .csv
    """

    data.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"hdfs://{host}:{port}/{path}/{filename}")


if __name__ == "__main__":

    spark = SparkSession.builder \
        .master("local") \
        .appName("samolet-hadoop") \
        .getOrCreate()

    data = extract(spark, FILEPATH, FILENAME)

    load(data, path="user/kovalyshev/samolet")
