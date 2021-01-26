#!/bin/bash
set -eux

python setup.py sdist bdist_egg bdist_wheel

twine check  ./dist/*
twine upload ./dist/*
