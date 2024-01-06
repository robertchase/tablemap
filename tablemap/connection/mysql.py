"""mysql database connection management"""
import aiomysql

from tablemap.connection import common


class MysqlConnector(common.Connector):
    """connector for mysql database"""

    async def connect(self):
        connection = await aiomysql.connect(*self.args, **self.kwargs)
        cursor = await connection.cursor()
        return MysqlCursor(cursor)

    async def close(self):
        pass


class MysqlCursor(common.Cursor):
    """mysql cursor extension"""

    @property
    def quote(self):
        return "`"

    def escape(self, value):
        return common.escape(value, "'", r"\'")

    async def columns(self, tablename):
        query = f"DESCRIBE {self.quote}{tablename}{self.quote}"
        cols = await self.select(query)
        pks = [f["Field"] for f in cols if f["Key"] == "PRI"]
        pk = pks[0] if len(pks) == 1 else None
        fields = [f["Field"] for f in cols if f["Field"] != pk]
        return pk, fields

    async def insert_auto_pk(self, insert_statement, _):
        await self.execute(insert_statement)
        return insert_statement, self.lastrowid
