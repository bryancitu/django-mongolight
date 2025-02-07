import pymongo
import pymongo.errors as mongo_errors
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.client import BaseDatabaseClient
from .creation import DatabaseCreation  # Importa la clase DatabaseCreation


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
    Database = mongo_errors

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
        return None  # MongoDB doesn't use cursors

    def _connect(self):
        if self.connection is None:
            connection_params = self.get_connection_params()
            self.connection = self.get_new_connection(connection_params)
        return self.connection
