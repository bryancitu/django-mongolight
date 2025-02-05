import pymongo
from django.db.backends.base.base import BaseDatabaseWrapper


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = 'mongodb'
    display_name = 'MongoLight'

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
