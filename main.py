import asyncio
from config.config import URL, HEADERS, HOST, PORT, USER, PASSWORD, DB_NAME, wcapi
from database.db import DataBase
from drop_parser import Parser
from utils.logger import logger
from worker.worker import Worker


async def main():
    logger.debug("Starting")
    api_queue = asyncio.Queue()
    parser = Parser(URL, HEADERS)
    database = DataBase(HOST, PORT, USER, PASSWORD, DB_NAME, wcapi)
    worker = Worker(database, parser, api_queue)

    try:
        await worker.start_db_connection()
        task1 = asyncio.create_task(worker.parse())
        task2 = asyncio.create_task(worker.get_products())
        await asyncio.gather(task1, task2)
        await worker.gather_api_tasks()
    finally:
        await worker.close_all_connections()

    await worker.update_products()
    await worker.create_new_products()
    logger.debug("All tasks are done")


if __name__ == "__main__":
    asyncio.run(main())
