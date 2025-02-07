from django.db.backends.base.introspection import BaseDatabaseIntrospection


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """
    Introspection class for MongoDB.
    """

    def get_table_list(self, cursor):
        """
        Return a list of table (collection) names in the database.
        """
        return self.connection.connection.list_collection_names()

    def table_names(self, cursor=None, include_views=False):
        """
        Return a list of table (collection) names in the database.
        """
        return self.get_table_list(cursor)
