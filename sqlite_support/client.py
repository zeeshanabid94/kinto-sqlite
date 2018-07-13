import sqlite3
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class SQLiteClient:
    """Client class for SQLite."""

    # Creates a client object with given args.
    # db_path:- path to db file. Can be :memory:
    #   for in-memory dbs.
    # isolation_level:- specifies when transactions
    #   become visible to other connections.

    # See sqlite3 for python docs for more details.
    # https://docs.python.org/3/library/sqlite3.html
    def __init__(self, db_path, isolation_level = None):
        self._db_path = db_path
        self._isolation_level = isolation_level
        if self._db_path == ":memory:":
            self._connection = sqlite3.connect(self._db_path, isolation_level=self._isolation_level)
            self._connection.row_factory = sqlite3.Row

    # Returns a cursor object to execute sql
    # statements by connecting to the db.
    # return type:- sqlite.cursor

    @contextmanager
    def connect(self, force_commit = False):
        logger.info("Creating connection to SQLite DB on path: " + str(self._db_path))
        try:
            if self._db_path == ":memory:":
                cursor =  self._connection.cursor()
                yield cursor
            else:
                connection = sqlite3.connect(self._db_path, isolation_level=self._isolation_level)
                connection.row_factory = sqlite3.Row
                cursor = connection.cursor()
                yield cursor
                cursor.close()
        except Exception as e:
            logger.error(msg=str(e))
            raise e