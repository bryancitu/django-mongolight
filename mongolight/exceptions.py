from pymongo import errors as mongo_errors

# Define excepciones personalizadas que Django espera


class DataError(mongo_errors.PyMongoError):
    pass


class IntegrityError(mongo_errors.PyMongoError):
    pass


class OperationalError(mongo_errors.PyMongoError):
    pass


class DatabaseError(mongo_errors.PyMongoError):
    pass


class InterfaceError(mongo_errors.PyMongoError):
    pass


class InternalError(mongo_errors.PyMongoError):
    pass


class ProgrammingError(mongo_errors.PyMongoError):
    pass


class NotSupportedError(mongo_errors.PyMongoError):
    pass

# Combina las excepciones de pymongo con las personalizadas


class DatabaseExceptions:
    Error = mongo_errors.PyMongoError
    DataError = DataError
    IntegrityError = IntegrityError
    OperationalError = OperationalError
    DatabaseError = DatabaseError
    InterfaceError = InterfaceError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError
