"""mapper between table <==> dict

   fields are defined by inspection (no explicit models)

   dicts are filled with simple values that are in a serialized form (in other
   words no objects -- (de)serialization can happen at another layer)
"""


class Calculated:  # pylint: disable=too-few-public-methods
    """non-escaped value (for calculated fields)"""

    def __init__(self, value):
        self.value = value


class SpecialHandling:  # pylint: disable=too-few-public-methods
    """do complex operations on select/insert/update

    The default handling is to con.quote column names on SELECT, and con.escape
    values on INSERT/UPDATE.

    If additional handling is required, for instance, adding GEO functions
    to transform data in the SQL statement, two functions can be supplied for
    performing this additional handling:

    1. read_column_fn(con: Connection, col_name: str) -> str

        Modify col_name (for instance, wrap it in a SQL function) and return the
        value. The returned value will be used in the SELECT statement (for
        query and for load). The returned value will not be passed to con.quote.

    2. save_fn(con: Connection, col_value: str) -> str

        Modify and return col_value. The returned value will be used in INSERT
        or UPDATE statements where the escaped col_value would otherwise have
        been used. The returned value will not be passed to con.escape.

    Either or both functions can be specified.
    """

    def __init__(self, read_column_fn=None, save_fn=None):
        self.read_column_fn = read_column_fn
        self.save_fn = save_fn


class Table:
    """SQL table to dict mapper

    specify value for:
        table_name: name of the SQL table
    """

    table_name = None

    # these values are managed internally
    query_fields = None
    special = {}
    pk = None
    fields = []
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

            # setup SpecialHandling fields
            cls.special = {
                k: v
                for k in dir(cls)
                if isinstance(v := getattr(cls, k), SpecialHandling)
            }

            # setup field list for the query method
            fields = [con.quote(cls.pk)]

            def read_column(con, column_name):
                """quote or perform special handling for queried columns"""
                result = con.quote(column_name)
                if column_name in cls.special:
                    special = cls.special[column_name]
                    if hasattr(special, "read_column_fn"):
                        result = (
                            special.read_column_fn(con, column_name)
                            + f" AS {con.quote(column_name)}"
                        )
                return result

            fields.extend(read_column(con, col) for col in cls.fields)
            fields.extend(
                f"{v.value} AS {con.quote(k)}"
                for k in dir(cls)
                if isinstance(v := getattr(cls, k), Calculated)
            )
            cls.query_fields = ",".join(fields)

            cls.is_init = True

    @classmethod
    def escape(cls, con, column_name, column_value):
        """escape or perform special handling for column values"""
        value = con.escape(column_value)
        if column_name in cls.special:
            special = cls.special[column_name]
            if hasattr(special, "save_fn"):
                value = special.save_fn(con, column_value)
        return value

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
        ins = {k: cls.escape(con, k, v) for k, v in data.items() if k in cls.fields}
        if cls.pk in data:
            ins[cls.pk] = con.escape(data[cls.pk])
        if raw:
            ins.update(raw)  # overlay insert with raw items
        if ins:
            cols = ",".join(f"{con.quote(col)}" for col in ins.keys())
            vals = ",".join(v for v in ins.values())
            insert = f"INSERT INTO {con.quote(cls.table_name)} ({cols}) VALUES ({vals})"

            cls.last_query = insert
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
        upd = {k: cls.escape(con, k, v) for k, v in data.items() if k in cls.fields}
        if raw:
            upd.update(raw)  # overlay update with raw items
        if upd:
            upd = ",".join(f"{con.quote(k)}={v}" for k, v in upd.items())
            cls.last_query = (
                f"UPDATE {con.quote(cls.table_name)}"
                f" SET {upd}"
                f" WHERE {con.quote(cls.pk)}={con.escape(data[cls.pk])}"
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
            f"DELETE FROM {con.quote(cls.table_name)}" f" WHERE {condition}"
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
        query = (
            f"SELECT {cls.query_fields}"
            f" FROM {con.quote(cls.table_name)} WHERE {condition}"
        )
        if limit:
            query += f" LIMIT {limit}"
        return query

    @classmethod
    async def load(cls, con, pk) -> dict:
        """return a dict (or None) for a row with primary_key=key"""
        await cls.setup(con)
        cls.last_query = cls.build(con, f"{con.quote(cls.pk)}=%s", pk, limit=1)
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
