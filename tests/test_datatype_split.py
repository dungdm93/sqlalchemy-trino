from typing import *

import pytest
from assertpy import assert_that

from sqlalchemy_trino import datatype

split_string_testcases = {
    '10': ['10'],
    '10,3': ['10', '3'],
    '"a,b",c': ['"a,b"', 'c'],
    '"a,b","c,d"': ['"a,b"', '"c,d"'],
    r'"a,\"b\",c",d': [r'"a,\"b\",c"', 'd'],
    r'"foo(bar,\"baz\")",quiz': [r'"foo(bar,\"baz\")"', 'quiz'],
    'varchar': ['varchar'],
    'varchar,int': ['varchar', 'int'],
    'varchar,int,float': ['varchar', 'int', 'float'],
    'array(varchar)': ['array(varchar)'],
    'array(varchar),int': ['array(varchar)', 'int'],
    'array(varchar(20))': ['array(varchar(20))'],
    'array(varchar(20)),int': ['array(varchar(20))', 'int'],
    'array(varchar(20)),array(varchar(20))': ['array(varchar(20))', 'array(varchar(20))'],
    'map(varchar, integer),int': ['map(varchar, integer)', 'int'],
    'map(varchar(20), integer),int': ['map(varchar(20), integer)', 'int'],
    'map(varchar(20), varchar(20)),int': ['map(varchar(20), varchar(20))', 'int'],
    'map(varchar(20), varchar(20)),array(varchar)': ['map(varchar(20), varchar(20))', 'array(varchar)'],
    'row(first_name varchar(20), last_name varchar(20)),int':
        ['row(first_name varchar(20), last_name varchar(20))', 'int'],
    'row("first name" varchar(20), "last name" varchar(20)),int':
        ['row("first name" varchar(20), "last name" varchar(20))', 'int'],
}


@pytest.mark.parametrize(
    'input_string, output_strings',
    split_string_testcases.items(),
    ids=split_string_testcases.keys()
)
def test_split_string(input_string: str, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string))
    assert_that(actual).is_equal_to(output_strings)


split_delimiter_testcases = [
    ('first,second', ',', ['first', 'second']),
    ('first second', ' ', ['first', 'second']),
    ('first|second', '|', ['first', 'second']),
    ('first,second third', ',', ['first', 'second third']),
    ('first,second third', ' ', ['first,second', 'third']),
]


@pytest.mark.parametrize(
    'input_string, delimiter, output_strings',
    split_delimiter_testcases,
)
def test_split_delimiter(input_string: str, delimiter: str, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string, delimiter=delimiter))
    assert_that(actual).is_equal_to(output_strings)


split_maxsplit_testcases = [
    ('one,two,three', -1, ['one', 'two', 'three']),
    ('one,two,three', 0, ['one,two,three']),
    ('one,two,three', 1, ['one', 'two,three']),
    ('one,two,three', 2, ['one', 'two', 'three']),
    ('one,two,three', 3, ['one', 'two', 'three']),
    ('one,two,three', 10, ['one', 'two', 'three']),

    (',one,two,three', 0, [',one,two,three']),
    (',one,two,three', 1, ['', 'one,two,three']),

    ('one,two,three,', 2, ['one', 'two', 'three,']),
    ('one,two,three,', 3, ['one', 'two', 'three', '']),
]


@pytest.mark.parametrize(
    'input_string, maxsplit, output_strings',
    split_maxsplit_testcases,
)
def test_split_maxsplit(input_string: str, maxsplit: int, output_strings: List[str]):
    actual = list(datatype.aware_split(input_string, maxsplit=maxsplit))
    assert_that(actual).is_equal_to(output_strings)
