# Storage Implementation for SQLite
# This file contains the main API used for storing, fetching
# and deleting data from SQLite database.
# Created By: Zeeshan Abid

import logging
import os
import json
import time
from collections import defaultdict

from kinto.core.storage import (StorageBase, DEFAULT_DELETED_FIELD,
                                DEFAULT_ID_FIELD, DEFAULT_MODIFIED_FIELD,
                                exceptions, generators)
from sqlite_support.queries import (CREATE_QUERY, READ_QUERY,
                                    DELETE_QUERY, UPDATE_QUERY,
                                    GET_ALL_QUERY)
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
        # Get the query and set table name
        query = CREATE_QUERY
        table_name = "records"

        # Check which id generator to use
        id_generator = id_generator or self.id_generator
        record = {**record}

        # If id_field in record check if it
        # already is present in table
        if id_field in record:
            try:
                existing = self.get(collection_id, parent_id, record[id_field])
                raise exceptions.UnicityError(id_field, existing)
            except exceptions.RecordNotFoundError:
                pass
        else:
            # Generate ID
            record[id_field] = id_generator()

        # Prepare values for SQL statement
        values_dict = {
            'id':record[id_field],
            'parent_id':parent_id,
            'collection_id':collection_id,
            'data':json.dumps(record),
            'last_modified':time.time(),
            'deleted':False
        }

        # Execute
        with self.client.connect(commit = True) as conn:
            results = conn.execute(query.format(table_name = table_name), values_dict)
            record = self.get(collection_id, parent_id, record[id_field])

        return record


    def get(self, collection_id, parent_id, object_id,
            id_field=DEFAULT_ID_FIELD,
            modified_field=DEFAULT_MODIFIED_FIELD,
            auth=None):
        # Get read query and set table name
        query = READ_QUERY
        table_name = "records"

        # Prepare value dict for query
        values_dict = {
            'id':object_id,
            'parent_id':parent_id,
            'collection_id':collection_id
        }

        object = None

        # Execute query
        with self.client.connect() as conn:
            results = conn.execute(query.format(table_name = table_name), values_dict)
            object = results.fetchone()
            # If object not found raise error
            if object == None:
                raise exceptions.RecordNotFoundError(object_id)

        # Convert blob to dict because it is
        # stored as string
        record = json.loads(object['data'])
        record[id_field] = object['id']
        record[modified_field] = object['last_modified']
        return record

    def update(self, collection_id, parent_id, object_id, record,
               id_field=DEFAULT_ID_FIELD,
               modified_field=DEFAULT_MODIFIED_FIELD,
               auth=None):
        # Get update query and set table name
        query = UPDATE_QUERY
        table_name = 'records'

        # Deleted is usually false until we get a record with
        # deleted set to true
        deleted = False or (record.get('deleted') != None)

        # Delete id fields and deleted field as they are not stored with the
        # data blob.
        if 'deleted' in record:
            del record['deleted']
        if id_field in record:
            del record[id_field]
        if modified_field in record:
            del record[modified_field]

        # Prepare values
        value_dict = {
            'id':object_id,
            'parent_id':parent_id,
            'collection_id':collection_id,
            'data':json.dumps(record),
            'last_modified':time.time(),
            'deleted':deleted
        }

        # Execute
        with self.client.connect(commit = True) as conn:
            print(query.format(table_name = table_name))
            result = conn.execute(query.format(table_name = table_name), value_dict)

        # Try to get the update record
        try:
            record = self.get(collection_id, parent_id, object_id, id_field, modified_field)
            print("updated record", record)
            return record
        except exceptions.RecordNotFoundError:
            # if record not found
            # raise error if this wasn't a delete update
            if deleted:
                pass
            else:
                raise exceptions.RecordNotFoundError



    def delete(self, collection_id, parent_id, object_id,
               id_field=DEFAULT_ID_FIELD, with_deleted=True,
               modified_field=DEFAULT_MODIFIED_FIELD,
               deleted_field=DEFAULT_DELETED_FIELD,
               auth=None, last_modified=None):

        if with_deleted:
            # Deleted by marking it as delete
            record = self.get(collection_id, parent_id, object_id, id_field=id_field, modified_field=modified_field)
            record[deleted_field] = True

            # Use update to set delete. Does not
            # return a record when used like this
            self.update(collection_id, parent_id, object_id, record, id_field=id_field, modified_field= modified_field)

            # Last modified would be right after setting delet
            record[modified_field] = time.time()
            return record
        else:
            # Actually delete from database
            query = DELETE_QUERY
            table_name = "records"

            values_dict = {
                'id' : object_id,
                'parent_id': parent_id,
                'collection_id':collection_id
            }

            with self.client.connect() as conn:
                results = conn.execute(query.format(table_name = table_name), values_dict)
                if results <= 0:
                    raise exceptions.RecordNotFoundError

                deleted = results.fetchone()

            record = {}
            record[modified_field] = deleted['last_modified']
            record[deleted_field] = True
            return record


    def delete_all(self, collection_id, parent_id, filters=None,
                   sorting=None, pagination_rules=None, limit=None,
                   id_field=DEFAULT_ID_FIELD, with_deleted=True,
                   modified_field=DEFAULT_MODIFIED_FIELD,
                   deleted_field=DEFAULT_DELETED_FIELD,
                   auth=None):
        if with_deleted:
            # Deleted by marking it as delete
            pass
        else:
            # Actually delete from database
            pass

    def get_all(self, collection_id, parent_id, filters=None, sorting=None,
                pagination_rules=None, limit=None, include_deleted=False,
                id_field=DEFAULT_ID_FIELD,
                modified_field=DEFAULT_MODIFIED_FIELD,
                deleted_field=DEFAULT_DELETED_FIELD,
                auth=None):
        query = GET_ALL_QUERY
        table_name = "records"

        values_dict = {
            'parent_id':parent_id,
            'collection_id':collection_id
        }

        format_values = defaultdict(str)

        # Parent conditions
        if '*' in parent_id:
            format_values['parent_id_filter'] = 'parent_id LIKE :parent_id'
            values_dict['parent_id'] = parent_id.replace("*", "%")
        else:
            format_values['parent_id_filter'] = 'parent_id = :parent_id'

        # Deleted conditions
        if not include_deleted:
            format_values['conditions_deleted'] = 'AND NOT deleted'

    def flush(self, auth=None):
        pass



def load_from_config(config):
    storage_location = config.get_settings()['storage_location']
    client = SQLiteClient(storage_location)
    return Storage(client)