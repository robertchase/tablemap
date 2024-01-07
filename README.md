# tablemap
simple python database connector

[![Testing: pytest](https://img.shields.io/badge/testing-pytest-yellow)](https://docs.pytest.org)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](https://opensource.org/license/mit/)


## introduction

The `tablemap` package provides a simple mapping between a `python` `class` and a table in the database. The goal is to make loading, saving, and simple queries easy.

#### not your mother's ORM

This project is not a full-blown `ORM` implementation which lets you write `SQL` by writing `python`. If you like that sort of thing, then you may not like this.

#### design philosophy

`SQL` is a pretty good language for what it does, and it should be left alone rather than be reimplemented with objects. For a case like:

1. load a row by primary key
0. manipulate some values in the row
0. save the row back to the database,

it is convenient to have some object-to-table glue to make these elementary operations easy and safe. `tablemap` provides a lightweight model to accomplish this.

#### tablemap data models

For a `model` of a `SQL` table, `tablemap` simply takes the table name and looks in the database to determine the `primary key` and column names. The database already knows the column types and table constraints&mdash;`tablemap` doesn't try to re-implement or enforce these things in `python`. Instead, `tablemap` forms the appropriate `SQL` for simple `SELECT`, `INSERT` and `UPDATE` commands and lets any errors rise up to the caller.

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
    
def deserialize(data: Dict) -> User:
    return User(**dict)
```

We use these two functions to create a database *adapter* that we can use for simple database operations:

```
class UserTable(tablemap.Adapter):
    table_name = "user" [1]
    object_serializer = serialize [2]
    object_factory = deserialize [3]
```

An `Adapter` requires the name of the database table [1], a function to turn a `User` into a `dict` [2], and a function to turn a `dict` into a `User` [3]. It provides classmethods to manipulate the `SQL` table using `python` objects.

The following example gets a new database connection (cursor) and grabs the data in the user table associated with the primary key `100`.

```
from database import DB
from model import User, UserTable

async def main():
    async with await DB.connect() as con:
        user = await UserTable.load(con, 100)
        print(user)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
```

A real `User` object would likely be much more complex, implementing fields and methods necessary for business logic. A real `UserTable` object might include a number of special methods, for instance, a `by_account` method to query a `User` by account name.

#### separation of concerns

The `User` class in the example above knows *nothing* about the database. The `Adapter` is provided with logic to marshall the `User` in and out of a `dict`, and *adapts* that `dict` to the underlying `SQL` table. Additional methods can be added to `UserTable` to perform special queries or updates without breaking this separation.

The `python` program is not concerned with carrying around `database objects`; instead, it has normal instances that *can be* synced with a database at well-understood moments.

`tablemap` does not try to emulate or re-implement the things that the database is already doing. There is no `SQL` cache, no `python-based` structure of interdependent `SQL` objects, and no `flush`; operations immediately interact with the database, which&mdash;by design&mdash;manages complex pending work in a `transaction`.

#### no magic

The connection to the database (the cursor) is explicitly passed to where it is required, just as is any other necessary value.

## operations

