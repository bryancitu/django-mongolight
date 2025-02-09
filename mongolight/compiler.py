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

        Soporta distintos formatos:
         - Los datos pueden venir en los atributos 'insert_values', 'objs' o 'value_rows'.
         - Se espera que los datos sean una secuencia (lista o tupla) de filas, donde cada fila es:
              - Una secuencia (lista o tupla) con tantos elementos como columnas, o
              - Un diccionario con claves que correspondan a los nombres de las columnas.
         - En algunos casos, la query puede retornar una única fila (es decir, una secuencia
           con la cantidad de valores igual a la cantidad de campos).
        """
        # Intentamos obtener los datos a insertar desde distintos atributos.
        if hasattr(self.query, 'insert_values') and self.query.insert_values:
            data = self.query.insert_values
        elif hasattr(self.query, 'objs') and self.query.objs:
            data = self.query.objs
        elif hasattr(self.query, 'value_rows') and self.query.value_rows:
            data = self.query.value_rows
        else:
            raise NotImplementedError(
                "No se encontraron valores para insertar en el query.")

        model = self.query.model
        # Usamos las columnas definidas en la query si existen; de lo contrario, usamos los campos del modelo.
        if hasattr(self.query, 'columns') and self.query.columns:
            field_names = self.query.columns
        else:
            field_names = [
                field.column for field in model._meta.concrete_fields]

        # Procesamos los datos:
        # Si 'data' es una secuencia, comprobamos si su primer elemento es a su vez una secuencia.
        if isinstance(data, (list, tuple)):
            first_elem = data[0]
            # Caso 1: data es la fila completa (no es una lista de filas)
            if not isinstance(first_elem, (list, tuple, dict)):
                if len(data) != len(field_names):
                    raise NotImplementedError(
                        "El número de valores no coincide con el número de campos")
                document = dict(zip(field_names, data))
            else:
                # Caso 2: data es una lista de filas; tomamos la primera fila.
                if isinstance(first_elem, (list, tuple)):
                    document = dict(zip(field_names, first_elem))
                elif isinstance(first_elem, dict):
                    document = first_elem
                else:
                    if len(field_names) == 1:
                        document = {field_names[0]: first_elem}
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
