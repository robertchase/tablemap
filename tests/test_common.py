"""test common cursor methods"""
import asyncio
from unittest import mock


def test_ping(common_cursor):
    """test the ping method"""

    async def _test():
        common_cursor.fetchone = mock.AsyncMock(return_value=(1,))
        await common_cursor.ping()
        common_cursor.execute.assert_called_with("SELECT 1")

    asyncio.run(_test())


def test_commit(common_cursor):
    """test the commit method"""

    async def _test():
        await common_cursor.commit()
        common_cursor.execute.assert_called_with("COMMIT")

    asyncio.run(_test())


def test_rollback(common_cursor):
    """test the rollback method"""

    async def _test():
        await common_cursor.rollback()
        common_cursor.execute.assert_called_with("ROLLBACK")

    asyncio.run(_test())


def test_select(common_cursor):
    """test the select method"""

    async def _test():
        # columns and rows
        common_cursor.description = (("A",), ("B",))
        common_cursor.fetchall = mock.AsyncMock(return_value=((1, 2), (3, 4)))

        result = await common_cursor.select(stmt := "test select statement")
        assert result == [{"A": 1, "B": 2}, {"A": 3, "B": 4}]
        common_cursor.execute.assert_called_with(stmt)

    asyncio.run(_test())
