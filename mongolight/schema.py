from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_table = "/* MongoDB no requiere CREATE TABLE */"

    def create_model(self, model):
        collection = model._meta.db_table
        if collection not in self.connection.list_collection_names():
            self.connection.create_collection(collection)

        # Crear índices
        for field in model._meta.local_fields:
            if field.db_index:
                self.connection[collection].create_index(field.column)

    def delete_model(self, model):
        self.connection.drop_collection(model._meta.db_table)

    def add_field(self, model, field):
        if field.db_index:
            collection = model._meta.db_table
            self.connection[collection].create_index(field.column)
