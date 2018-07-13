--
-- Schema File that runs and creates tables,
-- triggers, indices and any other database
-- related things that would be needed
--

-- Table for actual data
CREATE TABLE IF NOT EXISTS records (
  id TEXT, -- ID field for each record
  parent_id TEXT, -- Bucked ID for each record
  collection_id TEXT, -- Collection ID for each record

  -- This is a timestamp field.
  -- This field allows us to keep track of
  -- the last modification to the record
  last_modified TIMESTAMP,

  -- The actual data field. This will need a
  -- json converted to serialize and deserialize
  -- the data as the data is textblob.
  data BLOB,

  -- Boolean attributed indicating if the record
  -- was deleted. This is used for lazy deletion
  deleted boolean,

  primary key(id, parent_id, collection_id)

  );

-- Table for timestamps
-- Note: I do not know what it will be used for.
-- Once I figure that out I can remove these comments,
-- and add better ones.
create table IF not exists timestamps (
    parent_id Text,
    collection_id Text,
    last_modified timestamp,

    primary key (parent_id, collection_id)
);

-- Table for meta data storage of the database.
-- Things like created_at, schema version, last_modified
-- will be stored here.
create table IF not exists metadata (
    name Text,
    value Text
);