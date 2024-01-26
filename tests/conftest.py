"""fixtures"""
from unittest import mock

import pytest

import tablemap
from tablemap.connection import common


@pytest.fixture
def common_cursor():
    """create a test cursor from common.Cursor with all abstract methods stubbed"""

    # pylint: disable=too-many-instance-attributes
    class Cursor(common.Cursor):
        """test cursor class

        execute and fetchall are mocked to "sink" calls to the database
        insert_auto_pk is wrapped for called/not_called detection
        """

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.quote_ = "!"
            self.columns_ = ["A", "B"]
            self.primary_key_column_ = "pk"
            self.primary_key_ = 100
            self.rowcount = 1
            self.execute = None
            self.fetchall = None
            self.description = []

        @property
        def quote_char(self):
            return self.quote_

        def escape(self, value):
            return f">{value}<"

        async def columns(self, tablename):
            return self.primary_key_column_, self.columns_

        async def insert_auto_pk(self, insert_statement, pk_column):
            return insert_statement, self.primary_key_

    con = Cursor(None)
    con.execute = mock.AsyncMock()
    con.fetchall = mock.AsyncMock()
    con.insert_auto_pk = mock.AsyncMock(wraps=con.insert_auto_pk)
    return con


@pytest.fixture
def table():
    """return a mocked-up Table for testing"""

    class MyTable(tablemap.Table):
        """test table class"""

        table_name = "test_table"

    MyTable.insert = mock.AsyncMock(wraps=MyTable.insert)
    MyTable.update = mock.AsyncMock(wraps=MyTable.update)

    return MyTable


@pytest.fixture
def my_class():
    """return class for testing Adapter"""

    class MyClass:
        """class for testing Adapter"""

        # pylint: disable=invalid-name
        def __init__(self, pk=None, A=None, B=None, C=None):
            if pk:
                self.pk = pk
            if A:
                self.A = A
            if B:
                self.B = B
            if C:  # note: C is not in the database, so it is ignored
                self.C = C

        def serialize(self):
            """create a dictionary from an instance of this class"""
            result = {}
            for key in ("pk", "A", "B", "C"):
                if hasattr(self, key):
                    result[key] = getattr(self, key)
            return result

        @classmethod
        def factory(cls, *args, **kwargs):
            """build an instance of this class"""
            if len(args) == 1 and not kwargs:
                args0 = args[0]
                if isinstance(args0, dict):
                    kwargs = args0
            return cls(*args, **kwargs)

    return MyClass


@pytest.fixture
def adapter(my_class):  # pylint: disable=redefined-outer-name
    """return a mocked-up ObjectTable for testing"""

    class MyTable(tablemap.Adapter):
        """test table class"""

        table_name = "a_table"
        object_serializer = my_class.serialize
        object_factory = my_class.factory

    MyTable.before_save = mock.AsyncMock(wraps=MyTable.before_save)
    MyTable.after_load = mock.AsyncMock(wraps=MyTable.after_load)

    return MyTable
