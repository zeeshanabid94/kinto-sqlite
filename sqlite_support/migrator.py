"""This file contains the migration mixin for SQLite.
The mixin has the same behavior as the postgresql migrator,
but has different logging messages.
Hence, the methods are overriden to account for this."""

from kinto.core.storage.postgresql import migrator
import logging
import os

logger = logging.getLogger(__name__)

class SQLiteMigratorMixin(migrator.MigratorMixin):
    """Migrator for SQLite. Subclasses postgresql
    migration mixin as the behavior is the same.
    """

    # Basically the exact same thing as PostgreSQL,
    # but it has the PostgreSQL word replaced with SQLite
    # in the logger messages.
    def create_or_migrate_schema(self, dry_run=False):
        """Either create or migrate the schema, as needed."""
        version = self.get_installed_version()
        if not version:
            self.create_schema(dry_run)
            return

        logger.info('Detected SQLite {} schema version {}.'.format(self.name, version))
        if version == self.schema_version:
            logger.info('SQLite {} schema is up-to-date.'.format(self.name))
            return

        self.migrate_schema(version, dry_run)

    # Basically the exact same thing as PostgreSQL,
    # but it has the PostgreSQL word replaced with SQLite
    # in the logger messages.
    def create_schema(self, dry_run):
        """Actually create the schema from scratch using self.schema_file.

        You can override this if you want to add additional sanity checks.
        """
        logger.info('Create SQLite {} schema at version {} from {}'.format(
            self.name, self.schema_version, self.schema_file))
        if not dry_run:
            self._execute_sql_file(self.schema_file)
            logger.info('Created SQLite {} schema (version {}).'.format(
                self.name, self.schema_version))

    # Basically the exact same thing as PostgreSQL,
    # but it has the PostgreSQL word replaced with SQLite
    # in the logger messages.
    def migrate_schema(self, start_version, dry_run):
        migrations = [(v, v + 1) for v in range(start_version, self.schema_version)]
        for migration in migrations:
            expected = migration[0]
            current = self.get_installed_version()
            error_msg = 'SQLite {} schema: Expected version {}. Found version {}.'
            if not dry_run and expected != current:
                raise AssertionError(error_msg.format(self.name, expected, current))

            logger.info('Migrate SQLite {} schema from version {} to {}.'.format(
                self.name, *migration))
            filename = 'migration_{0:03d}_{1:03d}.sql'.format(*migration)
            filepath = os.path.join(self.migrations_directory, filename)
            logger.info('Execute SQLite {} migration from {}'.format(self.name, filepath))
            if not dry_run:
                self._execute_sql_file(filepath)
        logger.info('SQLite {} schema migration {}.'.format(
            self.name,
            'simulated' if dry_run else 'done'))

    # Helper method to execute the schema file
    def _execute_sql_file(self, filepath):
        """Helper method to execute the SQL in a file."""
        print("Creating schema")
        with open(filepath) as f:
            schema = f.read()

        # Since called outside request, force commit.
        with self.client.connect() as conn:
            results = conn.executescript(schema)
            print("Schema results")
            print(results.fetchone())




