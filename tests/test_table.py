"""test table operations"""
import asyncio
import random
from unittest import mock

import tablemap


def test_setup(common_cursor, table):
    """test the setup method"""

    async def _test():
        assert table.is_init is False
        await table.setup(common_cursor)
        assert table.is_init is True
        assert table.pk == common_cursor.primary_key_column_
        assert table.fields == common_cursor.columns_
        assert table.quote_ == common_cursor.quote_
        assert table.calculated == ""

    asyncio.run(_test())


def test_update(common_cursor, table):
    """test the update method"""

    async def _test():
        row_count = await table.update(common_cursor, {"pk": 42, "A": 10})
        assert row_count == table.row_count
        assert table.last_query == "UPDATE !test_table! SET !A!=>10< WHERE !pk!=>42<"

    asyncio.run(_test())


def test_update_with_raw(common_cursor, table):
    """test the update method with raw column"""

    async def _test():
        await table.update(
            common_cursor,
            data={"pk": 42, "A": 10},
            raw={"time": "NOW()"},
        )
        assert table.last_query == (
            "UPDATE !test_table! SET !A!=>10<,!time!=NOW() WHERE !pk!=>42<"
        )

    asyncio.run(_test())


def test_insert_with_pk(common_cursor, table):
    """test the insert method with primary key present"""

    async def _test():
        common_cursor.insert_auto_pk = mock.AsyncMock()
        await table.insert(common_cursor, {"pk": 42, "A": 10})
        assert table.last_query == (
            "INSERT INTO !test_table! (!A!,!pk!) VALUES (>10<,>42<)"
        )
        common_cursor.insert_auto_pk.assert_not_called()

    asyncio.run(_test())


def test_insert_without_pk(common_cursor, table):
    """test the insert method with primary key absent"""

    async def _test():
        common_cursor.insert_auto_pk.reset_mock()
        common_cursor.primary_key_ = random.randint(100, 1000)
        table.last_id = None
        await table.insert(common_cursor, {"A": 10})
        assert table.last_query == ("INSERT INTO !test_table! (!A!) VALUES (>10<)")
        common_cursor.insert_auto_pk.assert_called_once()
        assert table.last_id == common_cursor.primary_key_

    asyncio.run(_test())


def test_insert_with_pk_with_raw(common_cursor, table):
    """test the insert method with primary key and raw column"""

    async def _test():
        await table.insert(
            common_cursor,
            data={"pk": 42, "A": 10},
            raw={"time": "NOW()"},
        )
        assert table.last_query == (
            "INSERT INTO !test_table! (!A!,!pk!,!time!) VALUES (>10<,>42<,NOW())"
        )

    asyncio.run(_test())


def test_insert_without_pk_with_raw(common_cursor, table):
    """test the insert method without primary key with raw column"""

    async def _test():
        common_cursor.insert_auto_pk.reset_mock()
        await table.insert(
            common_cursor,
            data={"A": 10},
            raw={"time": "NOW()"},
        )
        assert table.last_query == (
            "INSERT INTO !test_table! (!A!,!time!) VALUES (>10<,NOW())"
        )
        common_cursor.insert_auto_pk.assert_called_once()

    asyncio.run(_test())


def test_save_with_pk(common_cursor, table):
    """test the save method with primary key present"""

    async def _test():
        table.update.reset_mock()
        await table.save(common_cursor, {"pk": 42, "A": 10})
        table.update.assert_called_once()

    asyncio.run(_test())


def test_save_with_pk_with_raw(common_cursor, table):
    """test the save method with primary key present and raw column"""

    async def _test():
        table.update.reset_mock()
        args = (common_cursor, {"pk": 42, "A": 10}, {"time": "NOW()"})
        await table.save(*args)
        table.update.assert_called_once_with(*args)

    asyncio.run(_test())


def test_save_without_pk(common_cursor, table):
    """test the save method with primary key absent"""

    async def _test():
        table.insert.reset_mock()
        await table.save(common_cursor, {"A": 10})
        table.insert.assert_called_once()

    asyncio.run(_test())


def test_save_without_pk_with_raw(common_cursor, table):
    """test the save method with primary key absent and raw column"""

    async def _test():
        table.insert.reset_mock()
        args = (common_cursor, {"A": 10}, {"time": "NOW()"})
        await table.save(*args)
        table.insert.assert_called_once_with(*args)

    asyncio.run(_test())


def test_delete(common_cursor, table):
    """test the delete method"""

    async def _test():
        await table.delete(common_cursor, "are=%s", "wild things")
        assert table.last_query == ("DELETE FROM !test_table! WHERE are=>wild things<")

    asyncio.run(_test())


def test_load(common_cursor, table):
    """test the load method"""

    async def _test():
        common_cursor.description = [["A"], ["B"]]
        common_cursor.fetchall.return_value = [[1, 2], [3, 4]]
        rs = await table.load(common_cursor, key := random.randint(100, 1000))
        assert table.last_query == (
            f"SELECT * FROM !test_table! WHERE !pk!=>{key}< LIMIT 1"
        )
        assert rs == {"A": 1, "B": 2}

    asyncio.run(_test())


def test_query(common_cursor, table):
    """test the query method"""

    async def _test():
        common_cursor.description = [["A"], ["B"]]
        common_cursor.fetchall.return_value = [[1, 2], [3, 4]]
        rs = await table.query(common_cursor)
        assert table.last_query == "SELECT * FROM !test_table! WHERE 1=1"
        assert rs == [{"A": 1, "B": 2}, {"A": 3, "B": 4}]

    asyncio.run(_test())


def test_calculated(common_cursor):
    """test the Calculated field mechanism"""

    class CalcTable(tablemap.Table):
        """test table class with Calculated class fields"""

        table_name = "the_table"

        now = tablemap.Calculated("NOW()")
        another_field = tablemap.Calculated("10 + 10")

    async def _test():
        common_cursor.description = []
        common_cursor.fetchall.return_value = []
        await CalcTable.query(common_cursor)
        assert CalcTable.last_query == (
            "SELECT *, 10 + 10 AS !another_field!,NOW() AS !now!"
            " FROM !the_table! WHERE 1=1"
        )

    asyncio.run(_test())
