import aiomysql
from woocommerce import API
from utils.logger import logger


class DataBase:
    def __init__(self, host: str, port: int, user: str, password: str, db_name: str, wcapi: API):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.wcapi = wcapi
        self.pool = None

    async def get_products(self):
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = f"SELECT * FROM gg_wc_product_meta_lookup WHERE tax_class='' AND sku != ''"
                logger.debug(f"Starting database query: {query}")
                await cursor.execute(query)
                result = await cursor.fetchall()
                logger.debug(f"DB received data parent products")
        return result

    async def get_product_variations(self, parent_id: int) -> dict:
        async with self.pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = (
                    f"SELECT gg_posts.ID, gg_posts.post_title, gg_wc_product_meta_lookup.sku FROM gg_posts JOIN"
                    f" gg_wc_product_meta_lookup ON gg_posts.ID=gg_wc_product_meta_lookup.product_id"
                    f" AND gg_posts.post_parent = {parent_id}"
                )
                logger.debug(f"Starting database query: {query}")
                await cursor.execute(query)
                result = await cursor.fetchall()
                logger.debug(f"DB received data {result}")
                return {parent_id: result}

    async def api_batch_update(self, data: dict):
        """
        :param data: {'parent_id': 8192, 'data': {'update': [{'id': 8199, 'stock_quantity': '5'}]}}
        :return: None
        """
        parent_id, data_variations = map(lambda x: data.get(x), data)
        self.wcapi.post(f"products/{parent_id}/variations/batch", data_variations).json()
        logger.info(f"API query sent: products/{parent_id}/variations/batch, {data_variations}")
        logger.info(f"API query on {data} successful")

    async def connect(self, loop=None):
        logger.debug("Connecting to database...")
        pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db_name,
            loop=loop,
            cursorclass=aiomysql.DictCursor,
        )
        self.pool = pool
        logger.debug("Connected to database, connection pools created")

    async def close_all_connections(self):
        self.pool.close()
        await self.pool.wait_closed()
        logger.debug("Disconnected from database, all connection pools are closed")
