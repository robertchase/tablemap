"""provide a layer between objects and a Table mapper
"""

from tablemap.table import Table


class Adapter(Table):
    """object serialization/deserialization layer on top of Table

    specify values for:
        object_factory: callable that builds an instance from a dict
        object_serializer: callable that builds a dict from an instance
    """

    object_factory = callable  # object_factory(item: dict) -> object
    object_serializer = callable  # object_serializer(item: object) -> dict

    @classmethod
    # pylint: disable=unused-argument
    async def before_save(cls, con, data: dict) -> dict:
        """modify data before database insert/update"""
        return data

    @classmethod
    # pylint: disable=unused-argument
    async def after_load(cls, con, data: dict) -> dict:
        """modify data after load from database"""
        return data

    @classmethod
    async def save(cls, con, data, raw=None):
        serial = cls.object_serializer(data)
        serial = await cls.before_save(con, serial)
        if await super().save(con, serial, raw):
            if cls.pk and cls.last_id:
                setattr(data, cls.pk, cls.last_id)
        return cls.row_count

    @classmethod
    async def insert(cls, con, data, raw=None):
        item = data
        if is_obj := not isinstance(item, dict):
            item = cls.object_serializer(item)
            item = await cls.before_save(con, item)
        if await super().insert(con, item, raw):
            if is_obj and cls.pk and cls.last_id:
                setattr(data, cls.pk, cls.last_id)
        return cls.row_count

    @classmethod
    async def update(cls, con, data, raw=None):
        if not isinstance(data, dict):
            data = cls.object_serializer(data)
            data = await cls.before_save(con, data)
        await super().update(con, data, raw)

    @classmethod
    async def load(cls, con, pk):
        if rs := await super().load(con, pk):
            rs = await cls.after_load(con, rs)
            rs = cls.object_factory(rs)
        return rs

    @classmethod
    async def query(cls, con, *args, limit=None, offset=None, **kwargs):
        rs = await super().query(con, *args, limit=limit, offset=offset, **kwargs)

        async def _load(item: dict):
            item = await cls.after_load(con, item)
            return cls.object_factory(item)

        result = None
        if rs:
            if limit == 1:
                result = await _load(rs)
            else:
                result = [await _load(item) for item in rs]

        return result

    @classmethod
    # pylint: disable=arguments-differ
    async def delete(cls, con, condition=None, args=None, pk=None) -> int:
        await cls.setup(con)

        if args is None:
            args = []
        elif not isinstance(args, (list, tuple)):
            args = [args]

        if pk:
            if args:
                raise TypeError("pk is not compatible with args")
            if condition:
                raise TypeError("pk is not compatible with condition")
            result = await super().delete(con, f"{con.quote(cls.pk)}=%s", pk)
        elif condition and hasattr(condition, cls.pk):
            if pk:
                raise TypeError("object is not compatible with pk")
            if args:
                raise TypeError("object is not compatible with args")
            pk = getattr(condition, cls.pk)
            result = await super().delete(con, f"{con.quote(cls.pk)}=%s", pk)
        elif condition:
            if not isinstance(condition, str):
                raise TypeError("expecting condition to be a str")
            result = await super().delete(con, condition, *args)
        else:
            raise TypeError("no arguments specified")

        return result

    @classmethod
    async def count(cls, con, where_clause: str = "1=1") -> int:
        """Count rows that would be returned by "where_clause"."""
        return await con.select_one(
            f"SELECT COUNT(*) FROM {con.quote(cls.table_name)}" f" WHERE {where_clause}"
        )

    @classmethod
    async def exists(cls, con, pk) -> bool:
        """Check if primary key (pk) exists in table."""
        await cls.setup(con)
        return await con.select_one(
            f"SELECT COUNT(*) FROM {con.quote(cls.table_name)}"
            f" WHERE {con.quote(cls.pk)}={con.escape(pk)}"
        )
