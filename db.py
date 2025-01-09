import psycopg2
from pymongo import MongoClient


def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="dashboard",
            user="postgres",
            password="ale12345678",
            host="192.168.1.5",
            port="5432"
        )
        cursor = conn.cursor()
        return conn, cursor
    except psycopg2.OperationalError as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        raise
def get_mongo_connection():
    try:
        mongo_client = MongoClient("mongodb://192.168.1.5:27017/sastreria")
        db = mongo_client["sastreria"]
        return db
    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        raise    