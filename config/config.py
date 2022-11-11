import os
from woocommerce import API
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
key_path = os.path.join(BASE_DIR, ".env")

load_dotenv(key_path)


URL = os.getenv("URL")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/105.0.0.0 Safari/537.36"
}
OUTPUT_FILE = "new_products.xlsx"

HOST = os.getenv("HOST")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DB_NAME = os.getenv("DB_NAME")
PORT = 3306

wcapi = API(
    url=os.getenv("API_URL"),
    consumer_key=os.getenv("CONSUMER_KEY"),
    consumer_secret=os.getenv("SECRET_KEY"),
    wp_api=True,
    version=os.getenv("VERSION"),
    timeout=15,
)
