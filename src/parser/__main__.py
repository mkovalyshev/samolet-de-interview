from dataclasses import dataclass
import argparse
import time
import requests
import re
import os
import csv  # because pandas seems to much for this
from bs4 import BeautifulSoup

SCHEMA = "https"
HOST = "alfa.kz"
SMARTPHONES_PATH = "phones/telefony-i-smartfony/2153-smartfon"
# for this task it can be hardcoded, for production version it should be an element of category tree


class Parser:
    def __init__(
        self, schema: str = SCHEMA, host: str = HOST, path: str = SMARTPHONES_PATH
    ):
        self.schema = schema
        self.host = host
        self.path = path

    def get_products_from_page(self, page: int):  # name too verbose?
        """
        returns all product URLs from page defined in parameter page
        """

        response = requests.get(
            f"{self.schema}://{self.host}/{self.path}/page{page}#products"
        )

        if response.status_code != 200:
            raise Exception("request error on Parser.get_products_from_page()")

        soup = BeautifulSoup(
            response.text, features="html.parser"
        )  # full regex parser would probably increase perf, bs decreases dev time

        products = soup.find_all("div", {"itemtype": "http://schema.org/Product"})

        links = [
            product.find("div", {"class": "title"}).find("a").get("href")
            for product in products
        ]

        return links


@dataclass
class Product:
    name: str
    price: float  # would add currency in production version
    seller: str
    ram: int
    rom: int

    rom_pattern = re.compile(", Встроенная память: (\d+)(\D+),")
    ram_pattern = re.compile(", Оперативная память: (\d+)(\D+),")
    # group is needed for memory unit, would add in production version

    @staticmethod
    def from_link(link: str) -> object:
        """
        returns instance of Product from link
        """

        response = requests.get(link)

        if response.status_code != 200:
            raise Exception("request error on Product.from_link()")

        soup = BeautifulSoup(response.text, features="html.parser")

        name = soup.find("h1", {"itemprop": "name"}).text
        seller = soup.find("div", {"class": "card-body"}).find("a").text.strip()

        price = soup.find("meta", {"itemprop": "price"}).get("content")
        price = float(price)

        ram_tuple = Product.ram_pattern.findall(
            soup.find("div", {"class": "excerpt"}).text.strip()
        )

        if ram_tuple is not None and len(ram_tuple) > 0:
            ram = int(ram_tuple[0][0])
        else:
            ram = None

        rom_tuple = Product.rom_pattern.findall(
            soup.find("div", {"class": "excerpt"}).text.strip()
        )

        if rom_tuple is not None and len(rom_tuple) > 0:
            rom = int(rom_tuple[0][0])
        else:
            rom = None

        return Product(name=name, price=price, seller=seller, ram=ram, rom=rom)


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-n", dest="n", type=int, required=True)
    args = arg_parser.parse_args()

    parser = Parser()

    links = []

    for i in range(args.n):
        links += parser.get_products_from_page(i + 1)

    products = [Product.from_link(link).__dict__ for link in links]

    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f"data/output_{int(time.time())}.csv",
    )

    with open(output_path, "w", newline="\n") as f:
        title = [field for field in Product.__dataclass_fields__]
        writer = csv.DictWriter(
            f, title, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )

        writer.writeheader()
        writer.writerows(products)