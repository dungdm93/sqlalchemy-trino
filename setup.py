import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqlalchemy-trino",
    version="0.5.0",
    author="Dũng Đặng Minh",
    author_email="dungdm93@live.com",
    description="Trino dialect for SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dungdm93/sqlalchemy-trino",
    keywords=["sqlalchemy", "trino"],
    license="Apache 2.0",
    platforms=["any"],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
    ],
    python_requires='>=3.7',
    install_requires=[
        "trino[sqlalchemy]>=0.310",
    ],
)
