# This file contains the queries that will be run
# for each of the methods supported by kinto.
# This allows for dynamically adding more queries
# or efficient queries later on.

# Created by: Zeeshan Abid


# This is a create query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
CREATE_QUERY = "INSERT INTO {} (id, parent_id, collection_id, data, last_modified, deleted) VALUES " \
               "(:id, :parent_id, :collection_id, :data, :last_modified, :deleted) ;"

# This is a read query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
READ_QUERY = "SELECT * FROM {} WHERE id=:id and parent_id = :parent_id and collection_id = :collection_id and not deleted;"

# This is a delete query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
DELETE_QUERY = "DELERE FROM {} WHERE :field=:value;"

# This is a update query. :NAME are placeholders
# for values. Provide values as a dict
# to the clients.execute method.
UPDATE_QUERY = "UPDATE {} SET data =:data, deleted=false, last_modified = :last_modified" \
               " WHERE id=:id and parent_id = :parent_id and collection_id = :collection_id;"