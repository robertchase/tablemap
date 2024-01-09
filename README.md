# tablemap
simple python database connector

[![Testing: pytest](https://img.shields.io/badge/testing-pytest-yellow)](https://docs.pytest.org)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://opensource.org/license/mit/)


## introduction

The `tablemap` package provides a simple mapping between a `python` class and a `SQL` table. The goal is to make loading, saving, and simple queries easy.

#### design philosophy

* `SQL` is a good language for relational operations, and should not be re-implemented with objects. Most queries&mdash;and all joins&mdash;should be written in `SQL`.
* `python` is a good general purpose language, and it should not be used to emulate or re-implement things that a relational database is already doing well.
* `CRUD` operations should happen with simple objects/relations by primary key.
* Pending or interdependent multi-object mutations should be handled with a database `transaction` instead of with `python` code.
*  Objects that are being synchronized with the database *should not have to carry around `SQL` knowledge*; instead, a separate class should handle all database interaction and *marshall* the data to/from the synchronized object.
* A database `cursor` is an explicit object and should be passed around like any other object. *No magic variables*.

#### tablemap data models

For a `model` of a `SQL` table, `tablemap` uses the table name to look up the associated `primary key` and column names&mdash;there is no need to re-specify these in `python`. The database already knows the column types and table constraints&mdash;`tablemap` doesn't try to re-implement or enforce these things in `python`. Instead, `tablemap` forms the appropriate `SQL` for simple `SELECT`, `INSERT` and `UPDATE` commands and lets any errors rise up to the caller. 

## How it works


#### the connector

`tablemap` wraps already available `asyncio-enabled` connectors for `mysql` or `postgres`. Here is an example of setting up a connector:

```
from tablemap.connector.mysql import MysqlConnector
    
DB = MysqlConnector()
DB.setup(host="localhost", db="test", user="fred")
```


#### the model

Here is a `User` object in `python`. It is a normal `python` class with no special database features:

```
class User:
    def __init__(self, account: str, id: int = None, is_active: bool = True):
        self.account = account
        self.id = id
        self.is_active = is_active
```

We need two functions, one to turn a `User` into a `dict`, and one to turn a `dict` into a `User`:

```
def serialize(user: User) -> dict:
    return {"account": user.account, "id": user.id, "is_active": user.is_active}
    
def deserialize(data: dict) -> User:
    return User(**dict)
```

We use these two functions to create a database *adapter* the performs database operations against the underlying table:

```
class UserTable(tablemap.Adapter):
    table_name = "user"
    object_serializer = serialize
    object_factory = deserialize
```

The following example gets a new database connection (cursor) and grabs the data in the user table associated with the primary key `100`.

```
from database import DB
from model import UserTable

async def main():
    async with await DB.connect() as con:
        user = await UserTable.load(con, 100)
        print(user)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

The `python` class and the underlying table don't have to contain all the same fields/columns. The `tablemap` methods only `INSERT` or `UPDATE` columns that actually exist in the `SQL` table; other fields are ignored. The `object_factory` function can filter out any fields returned from the underlying table that aren't needed in the `python` class. 

#### calculated fields

Consider this `Adapter`:

```
class MyTable(tablemap.Adapter):
    table_name = "token"

    is_expired = tablemap.Calculated("IFNULL(token_expire < NOW(), 1)")
```

The `is_expired` class attribute is a `Calculated` object. This means that the argument for the `Calculated` constructor will be executed with every query and returned as a field named after the class attribute that the object is assigned to. The query will be something like:

```
SELECT *, IFNULL(token_expire < NOW(), 1) AS is_expired FROM token ...
```

## operations

An `Adapter` has the following methods:

---
#### save

```
@classmethod
async def save(cls,
               con: tablemap.connection.common.Connector,
               data: object,
               raw: dict = None) -> int
```

`INSERT` or `UPDATE` a row in the underlying table with values from `data`.

###### parameters

*data* - `python` object that is tied to the underlying table

*raw* - `dict` whose keys are column names and whose values are raw `SQL` to be executed without being escaped. For instance, to set a timestamp to ten seconds from now, have the database perform the calculation by passing `{"ts_field": "NOW() + INTERVAL 10 SECONDS"}` in the `raw` parameter.

###### description

If `data` contains a primary key value, `UPDATE` is performed, else `INSERT`.
If an `INSERT` is performed, `data` will be updated with the database-generated primary key.

###### side effects

* the class variable `last_id` will contain the primary key of the inserted row, or None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return the number of rows affected (either 0 or 1 since the operation is by primary key)

---
#### insert

```
@classmethod
async def insert(cls,
                 con: tablemap.connection.common.Connector,
                 data: object,
                 raw: dict = None) -> int
```

`INSERT` a row into the underlying table with values from `data`.

###### parameters

*data* - `python` object that is tied to the underlying table

*raw* - `dict` whose keys are column names and whose values are raw `SQL` to be executed without being escaped.

###### description

If `data` does not contain a primary key value, the database-generated primary key will be updated in `data`.

The `save` method will always try to `UPDATE` a row if `data` contains a value for the primary key. Use the `insert` method when specifying your own value for primary key in order to force `INSERT` to be used.

###### side effects

* the class variable `last_id` will contain the primary key of the inserted row, or None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return the number of rows affected (either 0 or 1 since the operation is by primary key)


---
#### update

```
@classmethod
async def update(cls,
                 con: tablemap.connection.common.Connector,
                 data: object,
                 raw: dict = None) -> int
```

`UPDATE` an existing row into the underlying table with values from `data`.

###### parameters

*data* - `python` object that is tied to the underlying table

*raw* - `dict` whose keys are column names and whose values are raw `SQL` to be executed without being escaped.

###### description

All values contained in `data` will be included in the `UPDATE` statement, even if they are identical to what is in the database. The database may indicate that no rows were affected if none of the fields are different.

###### side effects

* the class variable `last_id` will contain None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return the number of rows affected (either 0 or 1 since the operation is by primary key)


---
#### load

```
@classmethod
async def load(cls,
               con: tablemap.connection.common.Connector,
               pk: str|int) -> object
```

Load an existing row from the underlying table by primary key.

###### parameters

*pk* - the primary key of the row to be loaded

###### description

The data in row is used to build a new object using the `object_factory`.

###### side effects

* the class variable `last_id` will contain None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return the resulting row after being processed by `object_factory` or None if not found


---
#### query

```
@classmethod
async def query(cls,
                con: tablemap.connection.common.Connector,
                condition: str = "1=1",
                args: list = None,
                limit: int = None) -> object|[object]|None
```

Query the underlying table.

###### parameters

*condition* - the where clause

*args* - substitution values for the where clause

*limit* - limit for the number of rows returned

###### description

The `SELECT` is executed, and each row returned is processed with the `object_factory` to create a list of `python` objects. If `limit=1`, then a single object is returned. If there is no match for the query, then `None` is returned.

Each value in `args` is escaped for `SQL` safety, and then substituted into the where clause (which uses `%s` placeholders for values).

This is intended for creating helper methods on the class. For instance:

```
@classmethod
async def by_name(cls, con, firstname, lastname):
    return await cls.query(con, "fname=%s, lname=%s", (firstname, lastname))
```

Where `firstname` and `lastname` are escaped by the `query` method, and then substituted into the `condition` string.

###### side effects

* the class variable `last_id` will contain None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return an object (the result of `object_factory`), a list of objects, or None


---
#### delete

```
@classmethod
async def delete(cls,
                 con: tablemap.connection.common.Connector,
                 condition: str|object = None,
                 args: int|str|tuple|list = None,
                 pk: int|str = None) -> int
```

Delete a row from the underlying table.

###### parameters

*condition* - where clause, or object to be deleted

*args* - substitution variables for *condition* (this is a scalar, a list or a tuple)

*pk* - primary key of row to be deleted

###### description

* If only *pk* is specified, that row will be deleted.
* If *condition* is specified and *condition* is an object with an attribute matching the primary key, then the row matching the primary key will be deleted.
* If *condition* is specified, then that condition will be used as the condition for the `DELETE` after any *args* are escaped and substituted.

###### side effects

* the class variable `last_id` will contain None
* the class variable `last_query` will contain the `SQL` statement executed
* the class variable `row_count` will contain the number of rows affected

###### return

Return the number of rows affected

---
#### before_save

```
@classmethod
async def before_save(cls,
                      con: tablemap.connection.common.Connector,
                      data: dict) -> dict
```

Modify data before saving.

###### parameters

*data* - `dict` representation of the `python` object (from `object_serializer`)

###### description

This method allows the data to be modified before being saved. This enables additional kinds of modifications of the data, like encryption.

Called from `save`, `insert` or `update` after `object_serializer`.

###### side effects

None

###### return

Return the `dict` as modified


---
#### before_save

```
@classmethod
async def after_load(cls,
                      con: tablemap.connection.common.Connector,
                      data: dict) -> dict
```

Modify data before creating an object.

###### parameters

*data* - `dict` representation of the `python` object.

###### description

This method allows the data to be modified before being passed to `object_factory`. This enables additional kinds of modifications of the data, like encryption.

Called from `load` or `query` before `object_factory`.

###### side effects

None

###### return

Return the `dict` as modified
