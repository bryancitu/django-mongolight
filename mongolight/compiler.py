# mongolight/compiler.py

from django.db.models.sql.compiler import SQLCompiler


class MongoCompiler(SQLCompiler):
    def as_mongo(self):
        query = self.query
        collection = query.model._meta.db_table
        where = self._translate_where(query.where)
        return {
            'collection': collection,
            'filter': where,
            'projection': self._get_projection(),
            'limit': query.low_mark,
            'skip': query.high_mark,
        }

    def _translate_where(self, node):
        # Translate Django's WHERE clauses to MongoDB filters
        pass

    def _get_projection(self):
        return {field.column: 1 for field in self.query.select}


class SQLInsertCompiler(MongoCompiler):
    def as_sql(self, *args, **kwargs):
        # Aquí puedes implementar la lógica de inserción o, si no es aplicable,
        # simplemente lanzar una excepción o redirigir a una implementación que use la API de MongoDB.
        raise NotImplementedError(
            "MongoDB backend does not support SQL insert compilation.")
