from sqlalchemy.sql import compiler

# https://trino.io/docs/current/language/reserved.html
RESERVED_WORDS = {
    "alter",
    "and",
    "as",
    "between",
    "by",
    "case",
    "cast",
    "constraint",
    "create",
    "cross",
    "cube",
    "current_date",
    "current_path",
    "current_role",
    "current_time",
    "current_timestamp",
    "current_user",
    "deallocate",
    "delete",
    "describe",
    "distinct",
    "drop",
    "else",
    "end",
    "escape",
    "except",
    "execute",
    "exists",
    "extract",
    "false",
    "for",
    "from",
    "full",
    "group",
    "grouping",
    "having",
    "in",
    "inner",
    "insert",
    "intersect",
    "into",
    "is",
    "join",
    "left",
    "like",
    "localtime",
    "localtimestamp",
    "natural",
    "normalize",
    "not",
    "null",
    "on",
    "or",
    "order",
    "outer",
    "prepare",
    "recursive",
    "right",
    "rollup",
    "select",
    "table",
    "then",
    "true",
    "uescape",
    "union",
    "unnest",
    "using",
    "values",
    "when",
    "where",
    "with",
}


class TrinoSQLCompiler(compiler.SQLCompiler):
    pass


class TrinoDDLCompiler(compiler.DDLCompiler):
    pass


class TrinoTypeCompiler(compiler.GenericTypeCompiler):
    pass


class TrinoIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS
