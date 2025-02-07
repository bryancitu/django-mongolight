import pymongo
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.client import BaseDatabaseClient
from .creation import DatabaseCreation  # Importa la clase DatabaseCreation
from .schema import DatabaseSchemaEditor
from .exceptions import DatabaseExceptions


class DatabaseOperations(BaseDatabaseOperations):
    """
    Operations class for MongoDB.
    """

    def max_name_length(self):
        """
        Return the maximum length of table and column names.
        """
        return 255  # MongoDB no tiene un límite estricto, pero Django necesita un valor


class DatabaseFeatures(BaseDatabaseFeatures):
    """
    Features specific to MongoDB.
    """
    supports_transactions = False  # MongoDB no soporta transacciones ACID
    can_return_columns_from_insert = False
    supports_timezones = False


class DatabaseClient(BaseDatabaseClient):
    """
    A simple client class to handle MongoDB connections.
    """

    def __init__(self, connection):
        self.connection = connection


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """
    Introspection class for MongoDB.
    """

    def get_table_list(self, cursor):
        """
        Return a list of table (collection) names in the database.
        """
        return self.connection.connection.list_collection_names()


class MongoCursor:
    """
    A custom cursor for MongoDB.
    """

    def __init__(self, connection):
        self.connection = connection

    def close(self):
        """
        Close the cursor. In MongoDB, this is a no-op.
        """
        pass

    def execute(self, query, params=None):
        """
        Execute a query. In MongoDB, this is handled by the connection.
        """
        pass

    def fetchone(self):
        """
        Fetch one result. Not used in MongoDB.
        """
        return None

    def fetchall(self):
        """
        Fetch all results. Not used in MongoDB.
        """
        return []


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mongodb'
    display_name = 'MongoLight'
    client_class = DatabaseClient  # Clase para manejar la conexión
    # Clase para manejar la creación de la base de datos
    creation_class = DatabaseCreation
    # Clase para manejar las características del backend
    features_class = DatabaseFeatures
    # Clase para manejar la introspección de la base de datos
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations  # Clase para manejar las operaciones del backend
    # Define las excepciones de la base de datos
    Database = DatabaseExceptions
    SchemaEditorClass = DatabaseSchemaEditor  # Asigna la clase SchemaEditor

    def __init__(self, settings_dict, *args, **kwargs):
        super().__init__(settings_dict, *args, **kwargs)
        self.client = None
        self.connection = None

    def get_connection_params(self):
        return {
            'host': self.settings_dict['HOST'],
            'port': int(self.settings_dict.get('PORT', 27017)),
            'username': self.settings_dict.get('USER', ''),
            'password': self.settings_dict.get('PASSWORD', ''),
            'authSource': self.settings_dict.get('AUTH_SOURCE', 'admin'),
        }

    def get_new_connection(self, conn_params):
        """
        Establece una nueva conexión a MongoDB.
        """
        client = pymongo.MongoClient(**conn_params)
        return client[conn_params.get('database', self.settings_dict['NAME'])]

    def create_cursor(self, name=None):
        """
        Create a custom cursor for MongoDB.
        """
        return MongoCursor(self.connection)

    def _connect(self):
        if self.connection is None:
            connection_params = self.get_connection_params()
            self.connection = self.get_new_connection(connection_params)
        return self.connection

    def _set_autocommit(self, autocommit):
        """
        MongoDB no soporta autocommit, así que este método no hace nada.
        """
        pass

    def close(self):
        """
        Close the connection to MongoDB.
        """
        if self.connection is not None:
            self.connection.client.close()  # Cierra el cliente de MongoDB
            self.connection = None
