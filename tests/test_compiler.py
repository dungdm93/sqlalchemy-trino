from sqlalchemy import Table, MetaData, Column, Integer, String, select

from sqlalchemy_trino.dialect import TrinoDialect

metadata = MetaData()
table = Table(
    'table',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
)


def test_limit_offset():
    statement = select(table).limit(10).offset(0)
    query = statement.compile(dialect=TrinoDialect())
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table" OFFSET :param_1\n LIMIT :param_2'


def test_limit():
    statement = select(table).limit(10)
    query = statement.compile(dialect=TrinoDialect())
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table"\n LIMIT :param_1'


def test_offset():
    statement = select(table).offset(0)
    query = statement.compile(dialect=TrinoDialect())
    assert str(query) == 'SELECT "table".id, "table".name \nFROM "table" OFFSET :param_1'
