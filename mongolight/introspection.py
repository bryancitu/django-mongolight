from django.db.backends.base.introspection import BaseDatabaseIntrospection


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        return self.connection.list_collection_names()

    def get_table_description(self, cursor, table_name):
        # Obtener la estructura de la colección (pseudo-esquema)
        return []
