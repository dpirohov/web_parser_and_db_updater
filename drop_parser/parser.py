from bs4 import BeautifulSoup
import aiohttp
from utils.logger import logger


class Parser:
    def __init__(self, url, headers):
        self.url = url
        self.headers = headers
        self.results = {}

    async def parse(self) -> dict:
        """
        :return: result = {'AM-13/2': {'name': 'Зимовий комбінезон на хутрі DB синій AM-13',
                'sku': 'AM-13/2', 'price': '455.00 грн', 'stock_quantity': '4'}
        """
        async with aiohttp.ClientSession() as session:
            logger.debug("Starting parser....")
            response = await session.get(self.url, headers=self.headers)
            response_text = await response.text()
            logger.debug("Web code retrieved, starting Bs4")
        await self.parse_text(response_text)
        return self.results

    async def parse_text(self, text: str):
        bs4 = BeautifulSoup(text, "html.parser")
        goods = bs4.find("div", {"class": "product-list"}).find_all("div", {"class": "product-card product-card-line"})
        for good in goods:
            product_name = good.find("div", {"data-search": "title"}).text
            grid = good.find_all("div", {"class": "offer"})
            for single in grid:
                article = single.find_all("div", {"class": "offers-grid-cell"})[0].text
                price = single.find_all("div", {"class": "offers-grid-cell"})[-2].text.strip()
                quantity = single.find("div", {"class": "store-quantity"}).text
                self.results[article] = {
                    "name": product_name,
                    "sku": article,
                    "price": price,
                    "stock_quantity": quantity,
                }
                logger.debug(
                    f"Found product name: {product_name}, sku: {article}, price: {price}, stock_quantity: {quantity}"
                )
