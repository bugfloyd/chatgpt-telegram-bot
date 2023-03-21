import sqlite3
from sqlite3 import Error
from datetime import datetime
import logging


class DatabaseException(Exception):
    def __init__(self, message, context=None):
        super().__init__(message)
        self.context = context

    def get_context(self):
        return self.context


class User:
    def __init__(self, user_id, first_name, role, username=None, last_name=None, telegram_chat_id=None,
                 creation_date=None):
        self.user_id = user_id
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.role = role  # 'owner', 'admin', or 'member'
        self.telegram_chat_id = telegram_chat_id
        self.creation_date = creation_date if creation_date else datetime.utcnow()


class UserManagement:
    def __init__(self, db_file="user_db.sqlite"):
        self.db_file = db_file
        self._create_connection()
        self._create_table()

    def _create_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
        except Error as e:
            print(e)

        self.conn = conn

    def _create_table(self):
        sql = """CREATE TABLE IF NOT EXISTS users (
                                        user_id INTEGER PRIMARY KEY,
                                        username TEXT,
                                        first_name TEXT NOT NULL,
                                        last_name TEXT,
                                        role TEXT NOT NULL,
                                        telegram_chat_id INTEGER,
                                        creation_date TEXT NOT NULL
                                    );"""
        self._run_sql(sql, None, False)

    def create_user(self, user):
        sql = '''INSERT INTO users(user_id, username, first_name, last_name, role, telegram_chat_id, creation_date)
                 VALUES(?,?,?,?,?,?,?)'''
        self._run_sql(sql, (
            user.user_id, user.username, user.first_name, user.last_name, user.role, user.telegram_chat_id,
            user.creation_date.isoformat()))

    def read_user(self, user_id):
        sql = "SELECT * FROM users WHERE user_id=?"

        row = self._run_sql(sql, (user_id,), False, "one")
        if row:
            return User(*row)
        return None

    def update_user(self, user):
        sql = '''UPDATE users
                 SET username = ? ,
                     first_name = ? ,
                     last_name = ? ,
                     role = ? ,
                     telegram_chat_id = ? ,
                     creation_date = ?
                 WHERE user_id = ?'''
        self._run_sql(sql, (user.username, user.first_name, user.last_name, user.role, user.telegram_chat_id,
                            user.creation_date.isoformat(), user.user_id))

    def delete_user(self, user_id):
        sql = 'DELETE FROM users WHERE user_id=?'
        self._run_sql(sql, (user_id,))

    def _run_sql(self, sql, data=None, commit=True, fetch=None):
        if self.conn is not None:
            try:
                cur = self.conn.cursor()
                if data is not None:
                    with self.conn.cursor() as c:
                        c.execute(sql, data)
                else:
                    with self.conn.cursor() as c:
                        c.execute(sql)

                if fetch is "one":
                    return cur.fetchone()
                elif fetch is "many":
                    return cur.fetchmany()  # todo

                if commit:
                    self.conn.commit()

            except Error as e:
                logging.exception(e)
                raise DatabaseException("Error while running the database operation")
