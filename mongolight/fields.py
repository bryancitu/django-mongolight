from django.db import models
import bson


class ObjectIdField(models.Field):
    def db_type(self, connection):
        return 'objectid'

    def get_internal_type(self):
        return "ObjectIdField"

    def to_python(self, value):
        if isinstance(value, str):
            return bson.ObjectId(value)
        return value

    def get_prep_value(self, value):
        if isinstance(value, str):
            return bson.ObjectId(value)
        return value
