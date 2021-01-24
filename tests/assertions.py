from assertpy import add_extension, assert_that
from sqlalchemy.sql.sqltypes import ARRAY

from sqlalchemy_trino.datatype import SQLType, MAP, ROW


def assert_sqltype(this: SQLType, that: SQLType):
    if isinstance(this, type):
        this = this()
    if isinstance(that, type):
        that = that()
    assert_that(type(this)).is_same_as(type(that))
    if isinstance(this, ARRAY):
        assert_sqltype(this.item_type, that.item_type)
        if this.dimensions is None or this.dimensions == 1:
            assert_that(that.dimensions).is_in(None, 1)
        else:
            assert_that(this.dimensions).is_equal_to(this.dimensions)
    elif isinstance(this, MAP):
        assert_sqltype(this.key_type, that.key_type)
        assert_sqltype(this.value_type, that.value_type)
    elif isinstance(this, ROW):
        assert_that(len(this.attr_types)).is_equal_to(len(that.attr_types))
        for name, this_attr in this.attr_types.items():
            that_attr = this.attr_types[name]
            assert_sqltype(this_attr, that_attr)
    else:
        assert_that(str(this)).is_equal_to(str(that))


@add_extension
def is_sqltype(self, that):
    this = self.val
    assert_sqltype(this, that)
