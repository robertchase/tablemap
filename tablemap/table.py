"""mapper between table <==> dict

   fields are defined by inspection (no explicit models)

   dicts are filled with simple values that are in a serialized form (in other
   words no objects -- (de)serialization can happen at another layer)
"""


class Calculated:  # pylint: disable=too-few-public-methods
    """non-escaped value (for calculated fields)"""

    def __init__(self, value):
        self.value = value


class Table:
    """SQL table to dict mapper

    specify value for:
        table_name: name of the SQL table
    """

    table_name = None

    # these values are managed internally
    calculated = None
    pk = None
    fields = []
    quote_ = None
    last_id = None
    last_query = None
    row_count = None
    is_init = False

    @classmethod
    async def setup(cls, con):
        """prepare class for next operation

        idempotently initialize class with table attributes
        """
        cls.last_id = None
        cls.last_query = None
        cls.row_count = None
        if not cls.is_init:
            # extract fields names from database
            cls.pk, cls.fields = await con.columns(cls.table_name)

            # grab quote character from connection
            cls.quote_ = con.quote

            # setup calculated fields for the query method
            cls.calculated = ",".join(
                f"{v.value} AS {cls.quote(k)}"
                for k in dir(cls)
                if isinstance(v := getattr(cls, k), Calculated)
            )

            cls.is_init = True

    @classmethod
    def quote(cls, data: str) -> str:
        """properly quote a database table or column name"""
        return f"{cls.quote_}{data}{cls.quote_}"

    @classmethod
    async def save(cls, con, data: dict, raw: dict = None) -> int:
        """save data (dict) to underlying table

        raw is a dict of column_name/values that will not be escaped
        """
        if not isinstance(data, dict):
            raise ValueError("expecting a dict")
        await cls.setup(con)
        if cls.pk in data:
            await cls.update(con, data, raw)
        else:
            await cls.insert(con, data, raw)
        return cls.row_count

    @classmethod
    async def insert(cls, con, data: dict, raw: dict = None) -> int:
        """insert data (dict) into underlying table

        raw is a dict of column_name/values that will not be escaped
        """
        if not isinstance(data, dict):
            raise ValueError("expecting a dict")
        await cls.setup(con)
        ins = {k: con.escape(v) for k, v in data.items() if k in cls.fields}
        if cls.pk in data:
            ins[cls.pk] = con.escape(data[cls.pk])
        if raw:
            ins.update(raw)  # overlay insert with raw items
        if ins:
            cols = ",".join(f"{cls.quote(col)}" for col in ins.keys())
            vals = ",".join(v for v in ins.values())
            insert = f"INSERT INTO {cls.quote(cls.table_name)} ({cols}) VALUES ({vals})"

            if cls.pk in data:
                await con.execute(insert)
            else:
                insert, cls.last_id = await con.insert_auto_pk(insert, cls.pk)
            cls.last_query = insert

            cls.row_count = con.rowcount
        return cls.row_count

    @classmethod
    async def update(cls, con, data: dict, raw: dict = None) -> int:
        """update underlying table with values in data (dict)

        raw is a dict of column_name/values that will not be escaped
        """
        if not isinstance(data, dict):
            raise ValueError("expecting a dict")
        await cls.setup(con)
        if cls.pk not in data:
            raise AttributeError(f"primary key ({cls.pk}) not provided")
        upd = {k: con.escape(v) for k, v in data.items() if k in cls.fields}
        if raw:
            upd.update(raw)  # overlay update with raw items
        if upd:
            upd = ",".join(f"{cls.quote(k)}={v}" for k, v in upd.items())
            cls.last_query = (
                f"UPDATE {cls.quote(cls.table_name)}"
                f" SET {upd}"
                f" WHERE {cls.quote(cls.pk)}={con.escape(data[cls.pk])}"
            )
            await con.execute(cls.last_query)
            cls.row_count = con.rowcount
        return cls.row_count

    @classmethod
    async def delete(cls, con, condition, *args) -> int:
        """delete from underlying table"""
        await cls.setup(con)
        if args:
            args = [con.escape(arg) for arg in args]
            if len(args) == 1:
                args = args[0]
            condition = condition % args
        cls.last_query = (
            f"DELETE FROM {cls.quote(cls.table_name)}" f" WHERE {condition}"
        )
        await con.execute(cls.last_query)
        cls.row_count = con.rowcount
        return cls.row_count

    @classmethod
    def build(cls, con, condition: str, args, limit: int = None) -> str:
        """build a query string"""
        if args:
            if isinstance(args, (list, tuple)):
                args = [con.escape(arg) for arg in args]
            else:
                args = con.escape(args)
            condition = condition % args
        cols = f"SELECT *, {cls.calculated}" if cls.calculated else "SELECT *"
        query = f"{cols} FROM {cls.quote(cls.table_name)} WHERE {condition}"
        if limit:
            query += f" LIMIT {limit}"
        return query

    @classmethod
    async def load(cls, con, pk) -> dict:
        """return a dict (or None) for a row with primary_key=key"""
        await cls.setup(con)
        cls.last_query = cls.build(con, f"{cls.quote(cls.pk)}=%s", pk, limit=1)
        rs = await con.select(cls.last_query)
        cls.row_count = len(rs)
        return rs[0] if cls.row_count else None

    @classmethod
    async def query(cls, con, condition="1=1", args=None, limit=None):
        """return a list of dicts for each row matching condition"""
        await cls.setup(con)
        cls.last_query = cls.build(con, condition, args, limit)
        rs = await con.select(cls.last_query)
        cls.row_count = con.rowcount

        if limit == 1:
            if len(rs):
                rs = rs[0]
            else:
                rs = None
        return rs
