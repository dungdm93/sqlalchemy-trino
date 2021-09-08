sqlalchemy-trino
================
_[Trino](https://trino.io/) (f.k.a PrestoSQL) dialect for SQLAlchemy._

The primary purpose of this is provide a dialect for Trino that can be used with [Apache Superset](https://superset.apache.org/).
But other use-cases should works as well.
# Supported Trino version

Trino version 352 and higher

## Installation
The driver can either be installed through PyPi or from the source code.
### Through Python Package Index
```bash
pip install sqlalchemy-trino
```

### Latest from Source Code
```bash
pip install git+https://github.com/dungdm93/sqlalchemy-trino
```

## Usage
To connect from SQLAlchemy to Trino, use connection string (URL) following this pattern:
```
trino://<username>:<password>@<host>:<port>/catalog/[schema]
```

### JWT authentication

You can pass the JWT token via either `connect_args` or the query string
parameter `accessToken`:

```Python
from sqlalchemy.engine import create_engine
from trino.auth import JWTAuthentication

# pass access token via connect_args
engine = create_engine(
  'trino://<username>@<host>:<port>/',
  connect_args={'auth': JWTAuthentication('a-jwt-token')},
)

# pass access token via the query string param accessToken
engine = create_engine(
  'trino://<username>@<host>:<port>/?accessToken=a-jwt-token',
)
```

**Notice**: When using username and password, it will connect to Trino over TLS
connection automatically.

### User impersonation

It supports user impersonation with username and password based authentication only.

You can pass the session user (a.k.a., the user that will be impersonated) via
either [`connect_args`](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine.params.connect_args)
or the query string parameter `sessionUser`:

```Python
from sqlalchemy.engine import create_engine

# pass session user via connect_args
engine = create_engine(
  'trino://<username>:<password>@<host>:<port>/',
  connect_args={'user': 'user-to-be-impersonated'},
)

# pass session user via a query string parameter
engine = create_engine(
  'trino://<username>:<password>@<host>:<port>/?sessionUser=user-to-be-impersonated',
)
```
