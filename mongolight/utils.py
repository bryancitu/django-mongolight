from bson import ObjectId
from django.core.exceptions import ValidationError


def validate_objectid(value):
    """
    Validate if a value is a valid ObjectId.
    """
    if not ObjectId.is_valid(value):
        raise ValidationError(f"'{value}' is not a valid ObjectId.")


def convert_to_objectid(value):
    """
    Convert a string to an ObjectId.
    """
    if isinstance(value, str) and ObjectId.is_valid(value):
        return ObjectId(value)
    return value


def get_collection_name(model):
    """
    Get the MongoDB collection name for a Django model.
    """
    return model._meta.db_table


def format_mongo_filter(django_filter):
    """
    Convert a Django filter to a MongoDB filter.
    """
    mongo_filter = {}
    for key, value in django_filter.items():
        if '__' in key:
            field_name, operator = key.split('__')
            mongo_operator = f'${operator}'
            mongo_filter[field_name] = {mongo_operator: value}
        else:
            mongo_filter[key] = value
    return mongo_filter
