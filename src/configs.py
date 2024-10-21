from dotenv import load_dotenv
import os

HOST = os.environ.get("MYSQL_HOST")
USER = os.environ.get("MYSQL_USER")
PASSWORD = os.environ.get("MYSQL_PASSWORD")
DATABASE =os.environ.get("MYSQL_DATABASE")
REDIS_URL=os.environ.get("REDIS_URL")
APP_KEY=os.getenv("APP_KEY")
APP_SECRET=os.getenv("APP_SECRET")