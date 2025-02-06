import pymongo
from django.db.backends.base.base import BaseDatabaseWrapper
from .creation import DatabaseCreation  # Importa la clase DatabaseCreation


class DatabaseClient:
    """
    A simple client class to handle MongoDB connections.
    """

    def __init__(self, connection):
        self.connection = connection


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mongodb'
    display_name = 'MongoLight'
    client_class = DatabaseClient  # Clase para manejar la conexión
    # Clase para manejar la creación de la base de datos
    creation_class = DatabaseCreation

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

    def create_cursor(self, name=None):
        return None  # MongoDB doesn't use cursors

    def _connect(self):
        if self.connection is None:
            connection_params = self.get_connection_params()
            self.client = pymongo.MongoClient(**connection_params)
            self.connection = self.client[self.settings_dict['NAME']]
        return self.connection
