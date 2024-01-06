"""routines common to all cursors"""
import abc


class Cursor(abc.ABC):
    """generic cursor extensions"""

    def __init__(self, cursor):
        self.cursor_ = cursor

    def __getattr__(self, name):
        return getattr(self.cursor_, name)  # delegate

    async def __aenter__(self):
        """support async with"""
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        """support async with"""
        if not exc_type:
            await self.commit()
        await self.close()

    async def ping(self):
        """simple round-trip to the database"""
        await self.execute("SELECT 1")
        rs = await self.fetchone()
        assert rs == (1,)

    async def commit(self):
        """commit helper function"""
        await self.execute("COMMIT")

    async def rollback(self):
        """rollback helper function"""
        await self.execute("ROLLBACK")

    async def select(self, query):
        """return a list of dicts from an arbitrary query"""
        await self.execute(query)
        resultset = await self.fetchall()
        col_names = [row[0] for row in self.description]
        return [dict(zip(col_names, row)) for row in resultset]

    @property
    @abc.abstractmethod
    def quote(self):
        """return character that delimits table or column names"""

    @abc.abstractmethod
    def escape(self, value):
        """escape value for safe use in a statement"""

    @abc.abstractmethod
    async def columns(self, tablename):
        """return primary_key_column_name, [column_name_1, ...] for a table

        list of column names excludes the primary key
        """

    @abc.abstractmethod
    async def insert_auto_pk(self, insert_statement, pk_column):
        """execute an insert statement with an auto increment primary key

        return tuple:
            insert_statement (possibly modified)
            primary key value of inserted row
        """


def escape(value, quote, escaped_quote):
    """handy escape function for SQL values"""
    if value is None:
        return "NULL"
    value = str(value)
    value = value.replace(quote, escaped_quote)
    return f"{quote}{value}{quote}"


class Connector(abc.ABC):
    """handy base connector class"""

    def __init__(self):
        self.args = None
        self.kwargs = None

    def setup(self, *args, **kwargs):
        """get args and kwargs for connection creation"""
        self.args = args
        self.kwargs = kwargs
        return self

    @abc.abstractmethod
    async def connect(self):
        """return a connection/cursor to the database"""

    @abc.abstractmethod
    async def close(self):
        """clean up any resources"""
