import pymysql.cursors
from config.config import wcapi, HOST, USER, PASSWORD, DB_NAME
from utils.logger import *


def get_all_parent_products(cursor) -> list:
    query = f"SELECT * FROM gg_wc_product_meta_lookup WHERE tax_class=''"
    logger.info(f"<DB> new query to database {query}")
    cursor.execute(query)
    result = cursor.fetchall()
    logger.info(f"<DB> response from database {result}")
    return result


def get_child_products_from_parent(cursor, parent_product: dict):
    query = f'SELECT * FROM `gg_posts` WHERE post_parent = {parent_product.get("product_id")} ' \
            f'AND post_type = "product_variation"'
    logger.info(f"<DB> new query {query}")
    cursor.execute(query)
    product_variations = cursor.fetchall()
    logger.info(f"<DB> response from database: {product_variations}")
    for child_product in product_variations:
        size = get_product_size(child_product)
        child_product["sku"] = f"{parent_product.get('sku')}/{size}"
    update_product_variations(parent_product, product_variations)


def get_product_size(product: dict):
    logger.info(f"<SIZE> GETTING SIZE OF {product}")
    size_raw = product.get("post_title")
    if size_raw is None:
        raise ValueError(f"{product} size is NONE")
    if size_raw[:26] == "Дощовик із шлеєю 2в1 квіти":
        size_raw = size_raw[-8:]
    size = "".join([v for v in size_raw if v.isdigit()])
    if size == "":
        logger.critical(f'{product} size is ""')
    return size


def update_product(parent_products: list):
    """data = {"update": [{"id": parent_product.get("product_id"), "manage_stock": True, "stock_quantity": 0}]}"""
    data = {"update": []}
    for product in parent_products:
        if len(data.get("update")) >= 50:
            logger.info(f"<WCAPI> data limit reached, doing WCAPI request")
            wcapi_send_update_product_request(data)
            data = {"update": []}
            logger.info(f"<WCAPI> flushed data, new data: {data}")
        data["update"].append({"id": product.get("product_id"), "manage_stock": False, "stock_quantity": 0})
    if data.get("update"):
        logger.info(f"<WCAPI> data left, doing WCAPI request")
        wcapi_send_update_product_request(data)


def wcapi_send_update_product_request(data: dict):
    logger.warning(f"<WCAPI> product request, data: {data}")
    try:
        wcapi.post("products/batch", data).json()
        logger.warning(f"<WCAPI> product request done")
    except Exception as error:
        logger.critical(f"<WCAPI ERROR> {error}, retrying")
        wcapi_send_update_product_request(data)


def update_product_variations(parent_product: dict, product_variations: list):
    data = {"update": []}
    for product_variation in product_variations:
        data["update"].append(
            {
                "id": product_variation.get("ID"),
                "manage_stock": True,
                "stock_quantity": 0,
                "sku": product_variation.get("sku"),
            }
        )
    wcapi_send_update_variations_request(parent_product, data)


def wcapi_send_update_variations_request(parent_product: dict, data: dict):
    logger.warning(f"<WCAPI> product variation request, data: {data}")
    try:
        wcapi.post(f"products/{parent_product.get('product_id')}/variations/batch", data).json()
        logger.warning(f"<WCAPI> product variation request done")
    except Exception as error:
        logger.critical(f"<WCAPI ERROR> {error}, retrying")
        wcapi_send_update_variations_request(parent_product, data)


def main():
    try:
        connection = pymysql.connect(
            host=HOST, port=3306, user=USER, password=PASSWORD, database=DB_NAME, cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("<DB> Connected to database")
        cursor = connection.cursor()
        logger.info("GATHERING PARENT PRODUCTS")
        parent_products = get_all_parent_products(cursor)
        logger.info("ALL PARENT PRODUCTS RETRIEVED, STARTING UPDATING PARENT PRODUCTS")
        update_product(parent_products)
        logger.info("ALL PARENT PRODUCTS UPDATED, STARTING UPDATING VARIATIONS")
        for parent_product in parent_products:
            logger.info(f"UPDATING variations of {parent_product}")
            get_child_products_from_parent(cursor, parent_product)
            logger.info(f"UPDATING SUCCESSFUL {parent_product}")
        logger.info(f"ALL TASKS ARE DONE!")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
