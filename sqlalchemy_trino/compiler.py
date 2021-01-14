from sqlalchemy.sql import compiler


class TrinoSQLCompiler(compiler.SQLCompiler):
    pass


class TrinoDDLCompiler(compiler.DDLCompiler):
    pass


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    pass


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    pass
