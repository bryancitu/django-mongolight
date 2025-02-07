from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_table = "/* MongoDB no requiere CREATE TABLE */"

    def create_model(self, model):
        """
        Create a collection for the given model.
        """
        collection = model._meta.db_table
        if collection not in self.connection.connection.list_collection_names():
            self.connection.connection.create_collection(collection)

        # Crear índices
        for field in model._meta.local_fields:
            if field.db_index:
                self.connection.connection[collection].create_index(
                    field.column)

    def delete_model(self, model):
        """
        Delete the collection for the given model.
        """
        collection_name = model._meta.db_table
        if collection_name in self.connection.connection.list_collection_names():
            self.connection.connection.drop_collection(collection_name)

    def add_field(self, model, field):
        if field.db_index:
            collection = model._meta.db_table
            self.connection.connection[collection].create_index(field.column)
