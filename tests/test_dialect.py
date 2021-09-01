from sqlalchemy.engine import url
from sqlalchemy_trino.dialect import TrinoDialect


def test_trino_connection_string_user():
    dialect = TrinoDialect()
    username = 'test-user'
    u = url.make_url(f'trino://{username}@host')
    _, cparams = dialect.create_connect_args(u)

    assert cparams['user'] == username


def test_trino_connection_string_session_user():
    dialect = TrinoDialect()
    username = 'test-user'
    session_user = 'sess-user'
    u = url.make_url(f'trino://{username}@host/?sessionUser={session_user}')
    _, cparams = dialect.create_connect_args(u)

    assert cparams['user'] == session_user
