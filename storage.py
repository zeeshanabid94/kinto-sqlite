# Storage Implementation for SQLite
# This file contains the main API used for storing, fetching
# and deleting data from SQLite database.
# Created By: Zeeshan Abid

import logging
import os
import json
import datetime

from kinto.core.storage import (StorageBase, DEFAULT_DELETED_FIELD,
                                DEFAULT_ID_FIELD, DEFAULT_MODIFIED_FIELD,
                                exceptions, generators)
from sqlite_support.queries import (CREATE_QUERY, READ_QUERY,
                                    DELETE_QUERY, UPDATE_QUERY)
from sqlite_support.migrator import SQLiteMigratorMixin

from sqlite_support.client import SQLiteClient

HERE = os.path.dirname(__file__)
logger = logging.getLogger(__name__)


class Storage(StorageBase, SQLiteMigratorMixin):
    """Storage backend using SQLite.

        Recommended in testing to provide a permanent storage
        solution for test data.

        Recommended in production for apps that need local
        persistent storage with transactions.

        Enable in configuration::

            How to enable this? Figure it out later

        Database location URI can be customized::

            Probably path to local sqlite file.

        SQLite does not require any username and password.
        Hence, make sure the database file is stored in a secure location
        that can not be accessed without application privileges.

        """

    name = 'storage'
    schema_version = 0
    schema_file = os.path.join(HERE, 'sqlite_support/schema.sql')
    migrations_directory = os.path.join(HERE, 'migrations')
    id_generator = generators.UUID4()

    # description:- Initialize method for this storage class
    # client:- SQLite client to use
    # max_fetch_size:- Maximum number of records to fetch
    #   if query returns more than one results.
    # readonly:- Boolean indicating if records are mutable.
    def __init__(self, client, max_fetch_size = 50, readonly=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client
        self._max_fetch_size = max_fetch_size
        self._readonly = readonly

    # returns the current version of the schema
    def get_installed_version(self):
        return self.schema_version
    # description:- Initializes the database schema
    def initialize_schema(self, dry_run=False):
        return self.create_or_migrate_schema(dry_run)

    def create(self, collection_id, parent_id, record, id_generator=None,
               id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        query = CREATE_QUERY
        table_name = "records"
        id_generator = id_generator or self.id_generator
        record = {**record}
        if id_field in record:
            try:
                existing = self.get(collection_id, parent_id, record[id_field])
                raise exceptions.UnicityError(id_field, existing)
            except exceptions.RecordNotFoundError:
                pass
        else:
            record[id_field] = id_generator()

        values_dict = {
            'id':record[id_field],
            'parent_id':parent_id,
            'collection_id':collection_id,
            'data':json.dumps(record),
            'last_modified':datetime.datetime.now(),
            'deleted':False
        }
        with self.client.connect() as conn:
            results = conn.execute(query.format(table_name), values_dict)
            record = self.get(collection_id, parent_id, record[id_field])

        print(record)
        return record


    def get(self, collection_id, parent_id, object_id,
            id_field=DEFAULT_ID_FIELD,
            modified_field=DEFAULT_MODIFIED_FIELD,
            auth=None):
        query = READ_QUERY
        table_name = "records"

        values_dict = {
            'id':object_id,
            'parent_id':parent_id,
            'collection_id':collection_id
        }

        object = None
        with self.client.connect() as conn:
            results = conn.execute(query.format(table_name), values_dict)
            object = results.fetchone()
            print(object)
            if object == None:
                raise exceptions.RecordNotFoundError(object_id)

        record = json.loads(object['data'])
        record[id_field] = object['id']
        record[modified_field] = object['last_modified']
        return record

    def update(self, collection_id, parent_id, object_id, record,
               id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):

        query = UPDATE_QUERY
        table_name = 'records'
        if id_field in record:
            del record[id_field]
        if modified_field in record:
            del record[modified_field]

        try:
            existing = self.get(collection_id, parent_id, object_id)


            value_dict = {
                'id':object_id,
                'parent_id':parent_id,
                'collection_id':collection_id,
                'data':json.dumps(record),
                'last_modified':datetime.datetime.now()
            }

            with self.client.connect() as conn:
                result = conn.execute(query.format(table_name), value_dict)
                print(result.fetchone())


        except exceptions.RecordNotFoundError:
            print("INSERTING NEW IN UPDATE")
            new_record = self.create(collection_id,parent_id,record, id_field=id_field, modified_field=modified_field)

            return new_record

    def flush(self, auth=None):
        pass



def load_from_config(config):
    storage_location = config.get_settings()['storage_location']
    client = SQLiteClient(storage_location)
    return Storage(client)