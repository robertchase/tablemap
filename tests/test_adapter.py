"""test table operations"""
import asyncio
import random

import pytest


def test_update(common_cursor, adapter, my_class):
    """test the update method"""

    async def _test():
        data = my_class(pk=42, A=10, C=30)
        await adapter.update(common_cursor, data)
        assert adapter.last_query == ("UPDATE !a_table! SET !A!=>10< WHERE !pk!=>42<")

    asyncio.run(_test())


def test_insert_with_pk(common_cursor, adapter, my_class):
    """test the insert method with primary key"""

    async def _test():
        data = my_class(pk=42, A=10, C=30)
        await adapter.insert(common_cursor, data)
        assert adapter.last_query == (
            "INSERT INTO !a_table! (!A!,!pk!) VALUES (>10<,>42<)"
        )

    asyncio.run(_test())


def test_insert_without_pk(common_cursor, adapter, my_class):
    """test the insert method without primary key"""

    async def _test():
        common_cursor.primary_key_ = key = random.randint(100, 1000)
        data = my_class(A=10)
        await adapter.insert(common_cursor, data)
        assert adapter.last_query == ("INSERT INTO !a_table! (!A!) VALUES (>10<)")
        assert data.pk == key

    asyncio.run(_test())


def test_save_with_pk(common_cursor, adapter, my_class):
    """test the save method with primary key"""

    async def _test():
        data = my_class(pk=42, A=10)
        await adapter.save(common_cursor, data)
        assert adapter.last_query == ("UPDATE !a_table! SET !A!=>10< WHERE !pk!=>42<")

    asyncio.run(_test())


def test_save_without_pk(common_cursor, adapter, my_class):
    """test the save method without primary key"""

    async def _test():
        common_cursor.primary_key_ = key = random.randint(100, 1000)
        data = my_class(A=10, C=10)
        await adapter.save(common_cursor, data)
        assert adapter.last_query == ("INSERT INTO !a_table! (!A!) VALUES (>10<)")
        assert data.pk == key

    asyncio.run(_test())


def test_load(common_cursor, adapter):
    """test the load method"""

    async def _test():
        common_cursor.description = [["A"], ["B"]]
        common_cursor.fetchall.return_value = [[1, 2]]
        data = await adapter.load(common_cursor, 0)
        assert data.A == 1
        assert data.B == 2

    asyncio.run(_test())


def test_query(common_cursor, adapter):
    """test the query method"""

    async def _test():
        common_cursor.description = [["A"], ["B"]]
        common_cursor.fetchall.return_value = [[1, 2], [3, 4]]
        data = await adapter.query(common_cursor)
        assert len(data) == 2
        data1 = data[0]
        assert data1.A == 1
        assert data1.B == 2
        data2 = data[1]
        assert data2.A == 3
        assert data2.B == 4

    asyncio.run(_test())


def test_delete_with_pk(common_cursor, adapter):
    """test the delete method with primary key"""

    async def _test():
        key = random.randint(100, 1000)
        await adapter.delete(common_cursor, pk=key)
        assert adapter.last_query == f"DELETE FROM !a_table! WHERE !pk!=>{key}<"

        with pytest.raises(TypeError):
            await adapter.delete(common_cursor, pk=100, condition="abc")

        with pytest.raises(TypeError):
            await adapter.delete(common_cursor, "asdf", pk=100)

    asyncio.run(_test())


def test_delete_with_condition(common_cursor, adapter):
    """test the delete method"""

    async def _test():
        key = random.randint(100, 1000)
        await adapter.delete(common_cursor, condition="xyz=%s", args=key)
        assert adapter.last_query == f"DELETE FROM !a_table! WHERE xyz=>{key}<"

        with pytest.raises(TypeError):
            await adapter.delete(common_cursor, 1000)

    asyncio.run(_test())


def test_delete_with_object(common_cursor, adapter, my_class):
    """test the delete method"""

    async def _test():
        data = my_class(pk=42, A=10)
        await adapter.delete(common_cursor, data)
        assert adapter.last_query == "DELETE FROM !a_table! WHERE !pk!=>42<"

        with pytest.raises(TypeError):
            await adapter.delete(common_cursor, data, pk=100)

        with pytest.raises(TypeError):
            await adapter.delete(common_cursor, data, args=100)

    asyncio.run(_test())
