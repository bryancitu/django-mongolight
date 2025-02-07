from django.db.backends.base.operations import BaseDatabaseOperations


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "mongolight.compiler"

    def sql_flush(self, style, tables, reset_sequences, allow_cascade=False):
        for table in tables:
            self.connection[table].delete_many({})
        return []

    def max_name_length(self):
        """
        Return the maximum length of table and column names.
        """
        return 255  # MongoDB no tiene un límite estricto, pero Django necesita un valor
