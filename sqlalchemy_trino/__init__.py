from sqlalchemy.dialects import registry

__version__ = '0.4.0'
registry.register("trino", "sqlalchemy_trino.dialect", "TrinoDialect")
