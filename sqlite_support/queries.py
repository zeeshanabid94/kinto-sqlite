# This file contains the queries that will be run
# for each of the methods supported by kinto.
# This allows for dynamically adding more queries
# or efficient queries later on.

# Created by: Zeeshan Abid


# This is a create query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
CREATE_QUERY = "INSERT INTO {table_name} (id, parent_id, collection_id, data, last_modified, deleted) VALUES " \
               "(:id, :parent_id, :collection_id, :data, :last_modified, :deleted) ;"

# This is a read query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
READ_QUERY = "SELECT * FROM {table_name} WHERE id=:id and parent_id = :parent_id and collection_id = :collection_id and deleted = False;"

# This is a delete query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
DELETE_QUERY = "DELERE FROM {table_name} WHERE id=:id and parent_id = :parent_id and collection_id = :collection_id;"

# This is a update query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
UPDATE_QUERY = CREATE_QUERY[:-1] + "ON CONFLICT(id, parent_id, collection_id) do UPDATE SET data =:data, deleted=:deleted, last_modified = :last_modified" \
               " WHERE id=:id and parent_id = :parent_id and collection_id = :collection_id;"

# This is a get all query. :NAME are placeholders
# for values. Provide values as a dict
# to client.execute method.
# Also, the {} args are for formatting the string
# in python
GET_ALL_QUERY = "WITH collection_filtered AS (" \
                "SELECT id, last_modified, data, deleted" \
                "FROM {table_name}"\
                "WHERE {parent_id_filter}" \
                "AND collection_id = :collection_id" \
                "{conditions_deleted} " \
                "{conditions_filter} " \
                "{sorting}" \
                ")," \
                "total_filtered AS (" \
                "SELECT COUNT(id) AS count_total" \
                "FROM collection_filtered" \
                "WHERE deleted = false" \
                ")" \
                "SELECT count_total," \
                "a.id, a.last_modified AS last_modified, a.data" \
                "FROM collection_filtered AS a," \
                "total_filtered" \
                "{pagination_rules}" \
                "LIMIT :pagination_limit;"