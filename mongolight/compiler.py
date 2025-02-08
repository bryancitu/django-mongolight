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
        Traduce algunos lookups básicos:
          - exact: {campo: valor}
          - gt: {campo: {"$gt": valor}}
          - gte: {campo: {"$gte": valor}}
          - lt: {campo: {"$lt": valor}}
          - lte: {campo: {"$lte": valor}}
          - in: {campo: {"$in": valor}}
          - contains: {campo: {"$regex": valor, "$options": "i"}}
        """
        if not node or not getattr(node, 'children', []):
            return {}

        filters = []
        for child in node.children:
            if hasattr(child, 'children'):
                # Nodo compuesto: procesar recursivamente.
                child_filter = self._translate_where(child)
            else:
                # Nodo simple: se asume que es una condición.
                try:
                    # Se obtiene el nombre del campo (asumiendo que se encuentra en child.lhs.target.column)
                    field_name = child.lhs.target.column
                except AttributeError:
                    field_name = str(child.lhs)

                lookup = getattr(child, 'lookup_name', 'exact')
                value = child.rhs

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
                    child_filter = {field_name: {
                        "$regex": value, "$options": "i"}}
                else:
                    child_filter = {field_name: value}

            if getattr(child, 'negated', False):
                # Si la condición está negada, se envuelve en $not.
                if field_name in child_filter:
                    child_filter = {field_name: {
                        "$not": child_filter[field_name]}}
                else:
                    child_filter = {"$not": child_filter}
            filters.append(child_filter)

        if node.connector == 'OR':
            combined_filter = {"$or": filters}
        else:
            combined_filter = {"$and": filters} if len(
                filters) > 1 else filters[0]

        if getattr(node, 'negated', False):
            combined_filter = {"$not": combined_filter}

        return combined_filter

    def _get_projection(self):
        return {field.column: 1 for field in self.query.select}


class SQLInsertCompiler(MongoCompiler):
    def as_sql(self, *args, **kwargs):
        """
        Extrae los valores a insertar del query y realiza la inserción en MongoDB.

        Se soporta:
          - insert_values u objs en formato lista o tupla.
          - Si el primer elemento es una lista o tupla, se asume que es una fila con valores.
          - Si el primer elemento es un diccionario, se usa directamente.
          - Si el primer elemento es un valor único y el modelo tiene un solo campo, se crea un diccionario.
        """
        # Intentamos obtener los datos de inserción.
        if hasattr(self.query, 'insert_values') and self.query.insert_values:
            data = self.query.insert_values  # Puede ser lista o tupla.
        elif hasattr(self.query, 'objs') and self.query.objs:
            data = self.query.objs
        else:
            raise NotImplementedError(
                "No se encontraron valores para insertar en el query.")

        # Obtenemos el modelo y los nombres de sus campos.
        model = self.query.model
        field_names = [field.column for field in model._meta.concrete_fields]

        # Soportamos data en formato lista o tupla.
        if isinstance(data, (list, tuple)):
            # Obtenemos el primer conjunto de datos.
            first_row = data[0]
            if isinstance(first_row, (list, tuple)):
                document = dict(zip(field_names, first_row))
            elif isinstance(first_row, dict):
                document = first_row
            else:
                # Si first_row no es lista, tupla o diccionario, puede ser un valor único.
                if len(field_names) == 1:
                    document = {field_names[0]: first_row}
                else:
                    raise NotImplementedError(
                        "Formato de datos de inserción desconocido.")
        else:
            raise NotImplementedError(
                "Formato de datos de inserción desconocido.")

        # Realizamos la inserción en la colección correspondiente.
        collection_name = model._meta.db_table
        self.connection[collection_name].insert_one(document)

        # Retornamos una tupla vacía para cumplir con la interfaz de Django.
        return ("", ())


# Asignamos la clase de inserción para que Django la encuentre
SQLInsertCompiler = SQLInsertCompiler
