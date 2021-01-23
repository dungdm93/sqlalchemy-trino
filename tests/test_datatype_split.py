from typing import *

import pytest
from assertpy import assert_that

from sqlalchemy_trino import datatype

split_string_testcases = {
    '10': ['10'],
    '10,3': ['10', '3'],
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
}


@pytest.mark.parametrize(
    'input_string, output_strings',
    split_string_testcases.items(),
    ids=split_string_testcases.keys()
)
def test_split_string(input_string: str, output_strings: List[str]):
    actual = list(datatype.split(input_string))
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
    actual = list(datatype.split(input_string, delimiter=delimiter))
    assert_that(actual).is_equal_to(output_strings)
