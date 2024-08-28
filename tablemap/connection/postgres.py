"""postgres database connection management"""

import aiopg

from tablemap.connection import common


class PsqlConnector(common.Connector):
    """connector for postgres database"""

    async def connect(self):
        connection = await aiopg.connect(*self.args, **self.kwargs)
        cursor = await connection.cursor()
        await cursor.execute("BEGIN TRANSACTION")
        return PsqlCursor(cursor)

    async def close(self):
        pass


class PsqlCursor(common.Cursor):
    """pysql cursor extension"""

    @property
    def quote_char(self):
        return '"'

    def escape(self, value):
        return common.escape(value, "'", "''")

    async def columns(self, tablename):
        query = (
            "SELECT c.column_name AS fieldname"
            ", CASE WHEN u.column_name IS NULL THEN 0 ELSE 1 END AS pk"
            " FROM information_schema.columns c"
            " LEFT OUTER JOIN information_schema.constraint_column_usage u"
            " ON c.table_name = u.table_name"
            " AND c.column_name = u.column_name"
            f" WHERE c.table_name = '{tablename}'"
        )
        cols = await self.select(query)
        pks = [f["fieldname"] for f in cols if f["pk"] == 1]
        pk = pks[0] if len(pks) == 1 else None
        fields = [f["fieldname"] for f in cols if f["pk"] == 0]
        return pk, fields

    async def insert_auto_pk(self, insert_statement, pk_column):
        insert = f'{insert_statement} RETURNING "{pk_column}"'
        await self.execute(insert)
        (pk,) = await self.fetchone()
        return insert, pk
