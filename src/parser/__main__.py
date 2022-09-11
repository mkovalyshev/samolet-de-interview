import re
import os
import csv  # because pandas seems to much for this
import sys
import time
import logging
from typing import Union
from dataclasses import dataclass
import argparse
import requests
from bs4 import BeautifulSoup


CALL_MESSAGE = "{} returned {}"

OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f"data/output_{int(time.time())}.csv",
)

logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter("[%(asctime)s: %(levelname)s] %(message)s"))
logger.addHandler(handler)


@dataclass
class Log:
    level: int
    msg: str


def log(func: callable):
    def wrapper(*args, **kwargs):
        try:
            level = logging.INFO
            func_return = func(*args, **kwargs)
        except BaseException as e:
            level = logging.WARNING
            func_return = e

        log = Log(level, CALL_MESSAGE.format(func.__qualname__, func_return))
        logger.log(**log.__dict__)

        if not isinstance(func_return, BaseException):
            return func_return

    return wrapper


@dataclass
class Product:
    title: str
    price: float  # would add currency in production version
    seller: str
    ram: int
    memory: int

    _rom_pattern = re.compile(", Встроенная память: (\d+)(\D+),")
    _ram_pattern = re.compile(", Оперативная память: (\d+ \D+),")
    _itemtype = "http://schema.org/Product"

    @staticmethod
    @log
    def from_link(url: str) -> object:
        """
        returns instance of Product from link
        """

        response = requests.get(url)

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"status_code = {response.status_code}, url = {url}"
            )

        soup = BeautifulSoup(response.text, features=Parser.PARSER)

        title = soup.find("h1", {"itemprop": "name"}).text
        seller = soup.find("div", {"class": "card-body"}).find("a").text.strip()

        price = soup.find("meta", {"itemprop": "price"}).get("content")
        price = int(price.split(".")[0])

        ram = Product._ram_pattern.findall(soup.find("div", {"class": "excerpt"}).text)[0]

        memory = " ".join(
            Product._rom_pattern.findall(soup.find("div", {"class": "excerpt"}).text)[0]
        )

        # ram and memory sometimes throw "out of range" error 
        # for phones that have no data about ram and memory

        return Product(title=title, price=price, seller=seller, ram=ram, memory=memory)


class Parser:
    """
    alfa.kz website parser
    """

    PARSER = "html.parser"

    SCHEMA = "https"  # not in __init__ because it's not likely to be redefined
    HOST = "alfa.kz"

    PHONES_PATH = "phones/telefony-i-smartfony"  
    # for this task it can be hardcoded, for production version it should be an element of category tree

    def __init__(self, parser: str = PARSER):
        self.parser = parser

    @log
    def get_products(self, page: int) -> Union[Product, Exception]:
        """
        yields instances of Product class from page
        """

        url = (
            f"{Parser.SCHEMA}://{Parser.HOST}/{Parser.PHONES_PATH}/page{page}#products"
        )

        response = requests.get(url)

        if response.status_code != 200:
            raise requests.exceptions.HTTPError(
                f"status_code = {response.status_code}, url = {url}"
            )

        soup = BeautifulSoup(  
            response.text, features=Parser.PARSER
        ) 
        # regex parser would probably increase performance, BS decreases dev time

        products = soup.find_all("div", {"itemtype": Product._itemtype})

        links = [
            product.find("div", {"class": "title"}).find("a").get("href")
            for product in products
        ]

        for link in links:
            try:
                yield Product.from_link(link)
            except Exception as e:
                yield e


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-n", dest="n", type=int, required=True)
    cli_args = arg_parser.parse_args()

    parser = Parser()

    logger.setLevel(logging.WARNING)

    products = []

    for page in range(cli_args.n):

        products += [
            product.__dict__
            for product in parser.get_products(page + 1)
            if product is not None
        ]

    with open(OUTPUT_PATH, "w", newline="\n") as f:

        title = list(Product.__dataclass_fields__)

        writer = csv.DictWriter(
            f, title, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )

        writer.writeheader()
        writer.writerows(products)
