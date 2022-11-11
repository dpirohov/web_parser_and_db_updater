import asyncio
import pandas as pd
import requests

from config.config import OUTPUT_FILE
from utils.logger import logger
from database.db import DataBase
from drop_parser import Parser


class Worker:
    def __init__(self, database: DataBase, parser: Parser, queue: asyncio.Queue):
        self.database = database
        self.parser = parser
        self.queue = queue
        self.parse_results = None
        self.products = None

    async def parse(self):
        self.parse_results = await self.parser.parse()

    async def get_products(self):
        self.products = await self.database.get_products()

    async def gather_api_tasks(self):
        logger.debug("Starting api queries")
        tasks = []
        for product in self.products:
            task = asyncio.create_task(self.form_api_queue(product))
            tasks.append(task)
        await asyncio.gather(*tasks)

    async def form_api_queue(self, product):
        """
        product = {'product_id': 7530, 'sku': 'M-74', 'virtual': 0, 'downloadable': 0,
        'min_price': Decimal('499.0000'), 'max_price': Decimal('499.0000'), 'onsale': 0, 'stock_quantity': None,
        'stock_status': 'outofstock', 'rating_count': 1, 'average_rating': Decimal('5.00'), 'total_sales': 0,
        'tax_status': 'taxable', 'tax_class': ''}
        variation = {'ID': 7739, 'post_title': 'Дощовик світловідбиваючий Міккі - 2 розмір', 'sku': 'M-57/2'}
        """
        product_variations = await self.database.get_product_variations(product.get("product_id"))
        if (len(product_variations.get(product.get("product_id")))) != 0:
            data = {"update": []}
            for variation in product_variations.get(product.get("product_id")):
                new_data = {"id": variation.get("ID")}
                article = variation.get("sku")
                if article in self.parse_results:
                    parsed_product = self.parse_results.pop(article)
                    new_data["stock_quantity"] = parsed_product.get("stock_quantity")
                else:
                    new_data["stock_quantity"] = 0
                data["update"].append(new_data)
            return await self.queue.put({"parent_id": product.get("product_id"), "data": data})

    async def start_db_connection(self):
        await self.database.connect()

    async def close_all_connections(self):
        await self.database.close_all_connections()

    async def update_products(self):
        while not self.queue.empty():
            update = await self.queue.get()
            try:
                logger.info(f"Starting API query: {update}")
                await self.database.api_batch_update(update)
            except requests.exceptions.ReadTimeout as err:
                logger.critical(f"API failed, {err}")
                logger.critical(f"Returning API query back to queue")
                await self.queue.put(update)

    async def create_new_products(self):
        logger.debug("Collecting new products")
        new_products = [product for product in self.parse_results.values()]
        df = pd.DataFrame(data=new_products)
        df.to_excel(OUTPUT_FILE, index=False)
        logger.debug("Uploading new products to new_products.xlsx")
