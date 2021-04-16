from sqlalchemy.dialects import registry

__version__ = '0.2.2'
registry.register("trino", "sqlalchemy_trino.dialect", "TrinoDialect")
