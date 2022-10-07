import psycopg2
from psycopg2.extensions import connection
import os


def get_connection() -> connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        password=os.getenv("DB_PASSWORD", "password"),
        user=os.getenv("DB_USER", "postgres"),
        database=os.getenv("DB_DATABASE", "postgres"),
    )
