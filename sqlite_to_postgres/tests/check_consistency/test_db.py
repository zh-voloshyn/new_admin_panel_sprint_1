# Сразу хочу извениться за невыполненое задание. Хочу получить, комментарии по моему коду. Понимаю, что моих знаний не хватает для продолжения учебы на данном курсе. ((

import sqlite3

import psycopg2
import pytest

postgres_dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

sqlite_db_path = 'db.sqlite'


@pytest.fixture
def sqlite_conn_cursor():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    yield conn, cursor
    conn.close()


@pytest.fixture
def postgres_conn_cursor():
    conn = psycopg2.connect(**postgres_dsn)

    cursor = conn.cursor()
    yield conn, cursor
    conn.close()


def test_records_in_postgres_exist_in_sqlite(postgres_conn_cursor, sqlite_conn_cursor):
    postgres_conn, postgres_cursor = postgres_conn_cursor
    sqlite_conn, sqlite_cursor = sqlite_conn_cursor

    tables_to_check = ['genre', 'film_work', 'person', 'genre_film_work', 'person_film_work']

    for table in tables_to_check:
        postgres_cursor.execute(f"SELECT * FROM {table}")
        postgres_data = postgres_cursor.fetchall()

        sqlite_cursor.execute(f"SELECT * FROM {table}")
        sqlite_data = sqlite_cursor.fetchall()

        differing_records = [record for record in postgres_data if record not in sqlite_data]
        assert not differing_records, f"Records mismatch in table {table}: {differing_records}"


def test_table_row_count(postgres_conn_cursor, sqlite_conn_cursor):
    postgres_conn, postgres_cursor = postgres_conn_cursor
    sqlite_conn, sqlite_cursor = sqlite_conn_cursor

    tables_to_check = ['genre', 'film_work', 'person']

    for table in tables_to_check:
        sqlite_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        sqlite_count = sqlite_cursor.fetchone()[0]

        postgres_cursor.execute(f"SELECT COUNT(*) FROM {table}")
        postgres_count = postgres_cursor.fetchone()[0]

        assert sqlite_count == postgres_count, f"Row count mismatch in table {table}"
