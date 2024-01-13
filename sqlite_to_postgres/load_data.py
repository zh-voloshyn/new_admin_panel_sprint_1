import datetime
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from uuid import uuid4

import psycopg2
import psycopg2.extras
# from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection

# db_path = os.environ.get('DB_SQL')

# dsn = {
#     'dbname': os.environ.get('DB_NAME'),
#     'user': os.environ.get('DB_USER'),
#     'password': os.environ.get('DB_PASSWORD'),
#     'host': os.environ.get('DB_HOST', '127.0.0.1'),
#     'port': os.environ.get('DB_PORT', 5432),
#     'options': '-c search_path=content',
# }

db_path = 'db.sqlite'

dsn = {
    'dbname': 'movies_database',
    'user': 'app',
    'password': '123qwe',
    'host': 'localhost',
    'port': 5432,
    'options': '-c search_path=content',
}

@dataclass
class Genre:
    id: uuid4
    name: str
    description: str
    created_at: datetime.datetime = field(default=datetime.datetime.now())
    updated_at: datetime.datetime = field(default=datetime.datetime.now())

    # def __post_init__(self):
    #     self.created_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")
    #     self.updated_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")


@dataclass
class Person:
    id: uuid4
    full_name: str
    created_at: datetime.datetime = field(default=datetime.datetime.now())
    updated_at: datetime.datetime = field(default=datetime.datetime.now())

    # def __post_init__(self):
    #     self.created_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")
    #     self.updated_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")


@dataclass
class Film_Work:
    id: uuid4
    title: str
    description: str
    creation_date: datetime.datetime
    rating: float = field(default=0.0)
    type: str = field(default="movie")
    created_at: datetime.datetime = field(default=datetime.datetime.now())
    updated_at: datetime.datetime = field(default=datetime.datetime.now())

    # def __post_init__(self):
    #     self.created_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")
    #     self.updated_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")


@dataclass
class Genre_Film_Work:
    id: uuid4
    film_work_id: uuid4
    genre_id: uuid4
    created_at: datetime.datetime = field(default=datetime.datetime.now())

    # def __post_init__(self):
    #     self.created_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")


@dataclass
class Person_Film_Work:
    id: uuid4
    person_id: uuid4
    film_work_id: uuid4
    role: str
    created_at: datetime.datetime = field(default=datetime.datetime.now())

    # def __post_init__(self):
    #     self.created_at = datetime.strptime(self.created_at, "%Y-%m-%d %H:%M:%S.%f")


@contextmanager
def conn_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def postgres_cursor(psql_db):
    connection = psycopg2.connect(**psql_db)
    try:
        yield connection
    finally:
        connection.close()


class PostgresSaver:
    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.pg_cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def save_model_data(self, data, model_class):
        column_name_mapping = {
            'created_at': 'created',
            'updated_at': 'modified',
        }
        column_names = [field.name for field in fields(model_class)]
        column_names = [column_name_mapping.get(name, name) for name in column_names]
        column_names_str = ','.join(column_names)

        col_count = ', '.join(['%s'] * len(column_names))

        table_name = model_class.__name__.lower()

        query = f"INSERT INTO {table_name} ({column_names_str}) VALUES ({col_count}) ON CONFLICT (id) DO NOTHING"

        for record in data:
            try:
                values = tuple(record[field.name] for field in fields(model_class))
                self.pg_cursor.execute(query, values)
            except Exception as e:
                print(f"Error while saving data to PostgreSQL: {e}")
            finally:
                self.pg_conn.commit()

    def save_all_data(self, data):
        person_data, genre_data, film_work_data, genre_film_work_data, person_film_work_data = data
        self.save_model_data(genre_data, Genre)
        self.save_model_data(person_data, Person)
        self.save_model_data(film_work_data, Film_Work)
        self.save_model_data(genre_film_work_data, Genre_Film_Work)
        self.save_model_data(person_film_work_data, Person_Film_Work)


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.conn = connection
        self.cur = connection.cursor()

    def load_data(self, model_class, limit=None, offset=None):
        table_name = model_class.__name__.lower()
        query = f"SELECT {', '.join(field.name for field in fields(model_class))} FROM {table_name}"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"

        self.cur.execute(query)

    def extract_movies(self, batch_size=None):
        person_data, genre_data, film_work_data, genre_film_work_data, person_film_work_data = [], [], [], [], []
        offset = 0

        try:
            while True:

                self.load_data(Person, limit=batch_size, offset=offset)
                batch_person_data = self.cur.fetchall()
                if not batch_person_data:
                    break
                person_data.extend(batch_person_data)

                self.load_data(Genre, limit=batch_size, offset=offset)
                batch_genre_data = self.cur.fetchall()
                genre_data.extend(batch_genre_data)

                self.load_data(Film_Work, limit=batch_size, offset=offset)
                batch_film_work_data = self.cur.fetchall()
                film_work_data.extend(batch_film_work_data)

                self.load_data(Genre_Film_Work, limit=batch_size, offset=offset)
                batch_genre_film_work_data = self.cur.fetchall()
                genre_film_work_data.extend(batch_genre_film_work_data)

                self.load_data(Person_Film_Work, limit=batch_size, offset=offset)
                batch_person_film_work_data = self.cur.fetchall()
                person_film_work_data.extend(batch_person_film_work_data)

                offset += batch_size

        except Exception as e:
            print(f"Error while extracting data from SQLite: {e}")

        return person_data, genre_data, film_work_data, genre_film_work_data, person_film_work_data


if __name__ == '__main__':
    batch_size = 100
    with conn_context(db_path) as sqlite_conn, postgres_cursor(dsn) as psql_conn:
        data = SQLiteExtractor(sqlite_conn)
        postgres_saver = PostgresSaver(psql_conn)
        try:
            postgres_saver.save_all_data([*data.extract_movies(batch_size=batch_size)])
        except Exception as e:
            print(f"Error during data migration: {e}")
