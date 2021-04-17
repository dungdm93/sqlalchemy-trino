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
