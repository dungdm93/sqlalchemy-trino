from typing import *

from sqlalchemy.sql.type_api import TypeEngine


class SequenceInfo(TypedDict, total=False):
    name: str
    start: int
    increment: int
    minvalue: int
    maxvalue: int
    nominvalue: bool
    nomaxvalue: bool
    cycle: bool
    cache: int
    order: bool


class ColumnInfo(TypedDict, total=False):
    name: str
    type: TypeEngine
    nullable: bool
    default: Any
    autoincrement: bool
    sequence: SequenceInfo


class PrimaryKeyInfo(TypedDict, total=False):
    name: Optional[str]
    constrained_columns: List[str]


class ForeignKeyInfo(TypedDict, total=False):
    name: str
    constrained_columns: List[str]
    referred_schema: str
    referred_table: str
    referred_columns: List[str]


class IndexInfo(TypedDict, total=False):
    name: str
    column_names: List[str]
    unique: bool


class UniqueConstraintInfo(TypedDict, total=False):
    name: str
    column_names: List[str]
    # other options


class CheckConstraintInfo(TypedDict, total=False):
    name: str
    sqltext: str
    # other options
