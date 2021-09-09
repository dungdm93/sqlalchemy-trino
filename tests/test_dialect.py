from sqlalchemy.engine import url
from sqlalchemy_trino.dialect import HTTPS, JWTAuthentication, TrinoDialect


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


def test_trino_connection_jwt_token():
    dialect = TrinoDialect()
    access_token = 'mock-token'
    u = url.make_url(f'trino://host/?accessToken={access_token}')
    _, cparams = dialect.create_connect_args(u)

    assert cparams['http_scheme'] == HTTPS
    assert isinstance(cparams['auth'], JWTAuthentication)
    assert cparams['auth'].token == access_token
