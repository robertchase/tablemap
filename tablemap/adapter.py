"""provide a layer between objects and a Table mapper
"""
from tablemap.table import Table


class Adapter(Table):
    """object serialization/deserialization layer on top of Table

    specify values for:
        object_factory: callable that builds an instance from a dict
        object_serializer: callable that builds a dict from an instance
    """

    object_factory = object  # object_factory(item: dict) -> object
    object_serializer = object  # object_serializer(item: object) -> dict

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
        rs = await super().load(con, pk)
        rs = await cls.after_load(con, rs)
        return cls.object_factory(rs)

    @classmethod
    async def query(cls, con, *args, limit=None, **kwargs):
        rs = await super().query(con, *args, limit=limit, **kwargs)

        async def _load(item: dict):
            item = await cls.after_load(con, item)
            return cls.object_factory(item)

        result = None
        if rs:
            if limit == 1:
                result = await _load(rs)
            else:
                result = []
                for item in rs:
                    result.append(await _load(item))

        return result

    @classmethod
    async def delete(cls, con, where, *args):
        await cls.setup(con)
        if not isinstance(where, str):
            args = [getattr(where, cls.pk)]
            where = f"{cls.quote(cls.pk)}=%s"
        return await super().delete(con, where, *args)
