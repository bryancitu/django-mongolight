from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    """
    Handles the creation of MongoDB databases and collections.
    """

    def create_test_db(self, verbosity=1, autoclobber=False, serialize=True, keepdb=False):
        """
        Create a test database. In MongoDB, this means creating a new database.
        """
        test_database_name = self._get_test_db_name()
        self.connection.settings_dict['NAME'] = test_database_name
        self.connection.connect()
        return test_database_name

    def destroy_test_db(self, old_database_name, verbosity=1, keepdb=False):
        """
        Destroy the test database. In MongoDB, this means dropping the database.
        """
        if not keepdb:
            self.connection.client.drop_database(
                self.connection.settings_dict['NAME'])

    def _get_test_db_name(self):
        """
        Generate a name for the test database.
        """
        return f"test_{self.connection.settings_dict['NAME']}"
