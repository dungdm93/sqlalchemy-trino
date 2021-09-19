from typing import Optional

from sqlalchemy.engine import ResultProxy
from sqlalchemy.engine.result import ResultMetaData
from trino.client import TrinoQuery, TrinoResult  # noqa
from trino.dbapi import Cursor


class TrinoResultMetaData:
    """Trino only return cursor.description when first row is loaded."""

    def __init__(self, parent, cursor):
        self.__parent: ResultProxy = parent
        self.__cursor: Cursor = cursor
        self.__delegator: Optional[ResultMetaData] = None

    def _load(self):
        if self.__delegator is not None:
            return
        if not self.__cursor.description:
            raise Exception("Cursor must be loaded before call TrinoResultMetaData")
        self.__delegator = ResultMetaData(self.__parent, self.__cursor.description)

    def __getattr__(self, item):
        self._load()
        return getattr(self.__delegator, item)

    def __getstate__(self):
        return self.__delegator

    def __setstate__(self, state):
        self.__delegator = state


class TrinoResultProxy(ResultProxy):
    def _cursor_description(self):
        raise NotImplementedError("Unexpected any calls here")

    def _init_metadata(self):
        if (
            self.context.compiled
            and "compiled_cache" in self.context.execution_options
        ):
            if self.context.compiled._cached_metadata:
                self._metadata = self.context.compiled._cached_metadata
            else:
                self._metadata = self.context.compiled._cached_metadata = TrinoResultMetaData(self, self._saved_cursor)
        else:
            self._metadata = TrinoResultMetaData(self, self._saved_cursor)
