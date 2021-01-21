from textwrap import dedent
from typing import *

from sqlalchemy import exc, sql
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
from sqlalchemy.engine.url import URL
from trino.auth import BasicAuthentication

from . import compiler
from . import dbapi as trino_dbapi
from . import error
from . import result
from . import types


class TrinoExecutionContext(DefaultExecutionContext):
    def get_result_proxy(self):
        return result.TrinoResultProxy(self)

    # all unimplemented stuffs

    def create_server_side_cursor(self):
        super(TrinoExecutionContext, self).create_server_side_cursor()

    def result(self):
        super(TrinoExecutionContext, self).result()

    def get_rowcount(self):
        super(TrinoExecutionContext, self).get_rowcount()


class TrinoDialect(DefaultDialect):
    name = 'trino'
    driver = 'rest'
    paramstyle = 'pyformat'  # trino.dbapi.paramstyle

    execution_ctx_cls = TrinoExecutionContext
    statement_compiler = compiler.TrinoSQLCompiler
    ddl_compiler = compiler.TrinoDDLCompiler
    type_compiler = compiler.TrinoTypeCompiler
    preparer = compiler.TrinoIdentifierPreparer

    # Data Type
    supports_native_enum = False
    supports_native_boolean = True
    supports_native_decimal = True

    # Column options
    supports_sequences = False  # TODO: check
    supports_comments = True
    inline_comments = True
    supports_default_values = False

    # DDL
    supports_alter = True

    # DML
    supports_empty_insert = False  # TODO: check
    supports_multivalues_insert = True

    @classmethod
    def dbapi(cls):
        """
        ref: https://www.python.org/dev/peps/pep-0249/#module-interface
        """
        return trino_dbapi

    def create_connect_args(self, url: URL) -> Tuple[List[Any], Dict[str, Any]]:
        args, kwargs = super(TrinoDialect, self).create_connect_args(url)  # type: List[Any], Dict[str, Any]

        db_parts = kwargs.pop('database', 'hive').split('/')
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError(f'Unexpected database format {url.database}')

        username = kwargs.pop('username', 'anonymous')
        kwargs['user'] = username

        password = kwargs.pop('password', None)
        if password:
            kwargs['http_scheme'] = 'https'
            kwargs['auth'] = BasicAuthentication(username, password)

        return args, kwargs

    def get_columns(self, connection: Connection,
                    table_name: str, schema: str = None, **kw) -> List[types.ColumnInfo]:
        full_table = self._get_full_table(table_name, schema)
        try:
            rows = self._get_table_columns(connection, full_table)
            columns = []
            for row in rows:
                columns.append(types.ColumnInfo(
                    name=row.Column,
                    type=compiler.parse_sqltype(row.Type, row.Column),
                    nullable=getattr(row, 'Null', True),
                    default=None,
                ))
            return columns
        except error.TrinoQueryError as e:
            if e.error_name in (error.TABLE_NOT_FOUND, error.SCHEMA_NOT_FOUND, error.CATALOG_NOT_FOUND):
                raise exc.NoSuchTableError(full_table) from e
            raise

    def get_pk_constraint(self, connection: Connection,
                          table_name: str, schema: str = None, **kw) -> types.PrimaryKeyInfo:
        """Trino has no support for primary keys. Returns a dummy"""
        return types.PrimaryKeyInfo(name=None, constrained_columns=[])

    get_primary_keys = get_pk_constraint

    def get_foreign_keys(self, connection: Connection,
                         table_name: str, schema: str = None, **kw) -> List[types.ForeignKeyInfo]:
        """Trino has no support for foreign keys. Returns an empty list."""
        return []

    def get_schema_names(self, connection: Connection, **kw):
        query = "SHOW SCHEMAS"
        res = connection.execute(sql.text(query))
        return [row.Schema for row in res]

    def get_table_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        query = "SHOW TABLES"
        if schema:
            query = f"{query} FROM {self.identifier_preparer.quote_identifier(schema)}"
        res = connection.execute(sql.text(query))
        return [row.Table for row in res]

    def get_temp_table_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """Trino has no support for temporary tables. Returns an empty list."""
        return []

    def get_view_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent("""
            SELECT "table_name"
            FROM "information_schema"."views"
            WHERE "table_schema" = :schema
        """).strip()
        res = connection.execute(sql.text(query), schema=schema)
        return [row.table_name for row in res]

    def get_temp_view_names(self, connection: Connection, schema: str = None, **kw) -> List[str]:
        """Trino has no support for temporary views. Returns an empty list."""
        return []

    def get_view_definition(self, connection: Connection, view_name: str, schema: str = None, **kw):
        full_view = self._get_full_table(view_name, schema)
        query = f"SHOW CREATE VIEW {full_view}"
        try:
            res = connection.execute(sql.text(query))
            return res.first()[0]
        except error.TrinoQueryError as e:
            if e.error_name in (error.TABLE_NOT_FOUND, error.SCHEMA_NOT_FOUND, error.CATALOG_NOT_FOUND):
                raise exc.NoSuchTableError(full_view) from e
            raise

    def get_indexes(self, connection: Connection,
                    table_name: str, schema: str = None, **kw) -> List[types.IndexInfo]:
        pass

    def get_unique_constraints(self, connection: Connection,
                               table_name: str, schema: str = None, **kw) -> List[types.UniqueConstraintInfo]:
        """Trino has no support for unique constraints. Returns an empty list."""
        return []

    def get_check_constraints(self, connection: Connection,
                              table_name: str, schema: str = None, **kw) -> List[types.CheckConstraintInfo]:
        """Trino has no support for check constraints. Returns an empty list."""
        return []

    def get_table_comment(self, connection: Connection,
                          table_name: str, schema: str = None, **kw) -> str:
        pass

    def has_schema(self, connection: Connection, schema: str) -> bool:
        query = f"SHOW SCHEMAS LIKE '{schema}'"
        try:
            res = connection.execute(sql.text(query))
            return res.first() is not None
        except error.TrinoQueryError as e:
            if e.error_name in (error.TABLE_NOT_FOUND, error.SCHEMA_NOT_FOUND, error.CATALOG_NOT_FOUND):
                return False
            raise

    def has_table(self, connection: Connection,
                  table_name: str, schema: str = None) -> bool:
        query = "SHOW TABLES"
        if schema:
            query = f"{query} FROM {self.identifier_preparer.quote_identifier(schema)}"
        query = f"{query} LIKE '{table_name}'"
        try:
            res = connection.execute(sql.text(query))
            return res.first() is not None
        except error.TrinoQueryError as e:
            if e.error_name in (error.TABLE_NOT_FOUND, error.SCHEMA_NOT_FOUND, error.CATALOG_NOT_FOUND):
                return False
            raise

    def has_sequence(self, connection: Connection,
                     sequence_name: str, schema: str = None) -> bool:
        """Trino has no support for sequence. Returns False indicate that given sequence does not exists."""
        return False

    def _get_server_version_info(self, connection: Connection):
        query = dedent("""
            SELECT *
            FROM system.runtime.nodes
            WHERE coordinator = true AND state = 'active'
        """).strip()
        res = connection.execute(sql.text(query)).first()
        version = int(res.node_version)
        return tuple([version])

    def _get_default_schema_name(self, connection: Connection):
        dbapi_connection: trino_dbapi.Connection = connection.connection
        return dbapi_connection.schema

    def do_rollback(self, dbapi_connection):
        if dbapi_connection.transaction is not None:
            dbapi_connection.rollback()

    def do_begin_twophase(self, connection: Connection, xid):
        pass

    def do_prepare_twophase(self, connection: Connection, xid):
        pass

    def do_rollback_twophase(self, connection: Connection, xid,
                             is_prepared: bool = True, recover: bool = False) -> None:
        pass

    def do_commit_twophase(self, connection: Connection, xid,
                           is_prepared: bool = True, recover: bool = False) -> None:
        pass

    def do_recover_twophase(self, connection: Connection) -> None:
        pass

    def set_isolation_level(self, dbapi_conn: trino_dbapi.Connection, level):
        dbapi_conn._isolation_level = getattr(trino_dbapi.IsolationLevel, level)

    def get_isolation_level(self, dbapi_conn: trino_dbapi.Connection) -> str:
        level_names = ["AUTOCOMMIT",
                       "READ_UNCOMMITTED",
                       "READ_COMMITTED",
                       "REPEATABLE_READ",
                       "SERIALIZABLE"]
        return level_names[dbapi_conn.isolation_level]

    @staticmethod
    def _get_table_columns(connection: Connection, full_table: str):
        stmt = sql.text(f'SHOW COLUMNS FROM {full_table}')
        return connection.execute(stmt)

    def _get_full_table(self, table_name: str, schema: str = None, quote: bool = True):
        table_part = self.identifier_preparer.quote_identifier(table_name) if quote else table_name
        if schema:
            schema_part = self.identifier_preparer.quote_identifier(schema) if quote else schema
            return f'{schema_part}.{table_part}'

        return table_part
