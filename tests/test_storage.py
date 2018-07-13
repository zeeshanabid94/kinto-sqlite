import unittest
from pyramid import testing
import storage as storage_sqlite

from kinto.core.storage.testing import StorageTest

class SQLiteStorageTest(StorageTest, unittest.TestCase):
    backend = storage_sqlite
    settings = {
        'storage_location':':memory:'
    }
    def setUp(self):
        super(SQLiteStorageTest, self).setUp()

    def test_config_is_taken_in_account(self):
        config = testing.setUp(settings=self.settings)
        config.add_settings({
            "storage_location":":memory:"
        })
        backend = self.backend.load_from_config(config)
        self.assertEqual(
            backend._db_path, ":memory:"
        )
