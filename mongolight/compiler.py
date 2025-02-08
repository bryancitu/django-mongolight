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
        """
        Recursively translates Django's WHERE node tree into a MongoDB filter dictionary.

        Assumptions:
        - Los nodos compuestos (WhereNode) tienen los atributos:
            - children: lista de nodos hijos o condiciones
            - connector: un string ('AND' o 'OR')
            - negated: booleano que indica si el nodo completo está negado
        - Los nodos hoja (condiciones) tienen:
            - lhs: el lado izquierdo (usualmente un campo, que posee el atributo 'target.column')
            - lookup_name: tipo de comparación (por ejemplo, 'exact', 'gt', 'lt', etc.)
            - rhs: valor a comparar
            - negated: booleano que indica si esa condición está negada

        Esta función traduce las condiciones de la siguiente manera:
        - exact: {campo: valor}
        - gt: {campo: {"$gt": valor}}
        - gte: {campo: {"$gte": valor}}
        - lt: {campo: {"$lt": valor}}
        - lte: {campo: {"$lte": valor}}
        - in: {campo: {"$in": valor}}
        - contains: {campo: {"$regex": valor, "$options": "i"}}

        Se combinan los filtros usando $and o $or según el atributo 'connector'.
        Si un nodo o condición está negado, se envuelve en un operador $not.
        """

        # Caso base: si no hay nodo o no tiene hijos, retornar filtro vacío.
        if not node or not getattr(node, 'children', []):
            return {}

        filters = []

        for child in node.children:
            # Si el hijo es otro nodo compuesto, se procesa de forma recursiva.
            if hasattr(child, 'children'):
                child_filter = self._translate_where(child)
            else:
                # Se asume que 'child' es una condición simple.
                try:
                    # Se asume que el campo se encuentra en child.lhs.target.column.
                    field_name = child.lhs.target.column
                except AttributeError:
                    # En caso de que no se encuentre, se utiliza la representación en string.
                    field_name = str(child.lhs)

                # Se obtiene el tipo de lookup (por defecto, 'exact')
                lookup = getattr(child, 'lookup_name', 'exact')
                value = child.rhs

                # Traducción según el tipo de lookup.
                if lookup == 'exact':
                    child_filter = {field_name: value}
                elif lookup == 'gt':
                    child_filter = {field_name: {"$gt": value}}
                elif lookup == 'gte':
                    child_filter = {field_name: {"$gte": value}}
                elif lookup == 'lt':
                    child_filter = {field_name: {"$lt": value}}
                elif lookup == 'lte':
                    child_filter = {field_name: {"$lte": value}}
                elif lookup == 'in':
                    child_filter = {field_name: {"$in": value}}
                elif lookup == 'contains':
                    # Se utiliza una expresión regular para la búsqueda 'contains'
                    child_filter = {field_name: {
                        "$regex": value, "$options": "i"}}
                else:
                    # Si el lookup no está contemplado, se usa una comparación exacta.
                    child_filter = {field_name: value}

            # Si la condición individual está negada, se envuelve en $not.
            if getattr(child, 'negated', False):
                # Se asume que la condición negada corresponde al campo utilizado.
                # En MongoDB, $not se aplica sobre la expresión de comparación.
                if field_name in child_filter:
                    child_filter = {field_name: {
                        "$not": child_filter[field_name]}}
                else:
                    # En caso de estructura distinta, se envuelve todo.
                    child_filter = {"$not": child_filter}

            filters.append(child_filter)

        # Combina los filtros según el conector del nodo actual.
        if node.connector == 'OR':
            combined_filter = {"$or": filters}
        else:  # Por defecto, se asume AND.
            if len(filters) > 1:
                combined_filter = {"$and": filters}
            else:
                combined_filter = filters[0]

        # Si el nodo completo está negado, se envuelve el filtro combinado en $not.
        if getattr(node, 'negated', False):
            combined_filter = {"$not": combined_filter}

        return combined_filter

    def _get_projection(self):
        return {field.column: 1 for field in self.query.select}


class SQLInsertCompiler(MongoCompiler):
    def as_sql(self, *args, **kwargs):
        """
        En este método debes transformar la consulta de inserción en una operación
        de inserción para MongoDB. Por ejemplo, extraer los datos a insertar y utilizar
        `insert_one` o `insert_many` según sea el caso.

        Nota: Django espera que se retorne una tupla (sql, params), pero dado que
        MongoDB no usa SQL, deberás adaptar el comportamiento. Una posibilidad es
        realizar la inserción directamente aquí y devolver una cadena vacía.
        """
        # Ejemplo simplificado (debes adaptar la extracción de datos según tu implementación):
        # Esto es solo ilustrativo; la forma de obtener los datos puede variar.
        data = self.query.values
        collection_name = self.query.model._meta.db_table

        # Realiza la inserción usando la conexión de MongoDB
        self.connection[collection_name].insert_one(data)

        # Retornamos una tupla vacía para cumplir con la interfaz.
        return ("", ())


# Asigna el compilador de inserciones para que Django lo encuentre
SQLInsertCompiler = SQLInsertCompiler
