from django.db.backends.base.features import BaseDatabaseFeatures


class DatabaseFeatures(BaseDatabaseFeatures):
    """
    Features specific to MongoDB.
    """
    supports_transactions = False  # MongoDB no soporta transacciones ACID
    can_return_columns_from_insert = False
    supports_timezones = False
