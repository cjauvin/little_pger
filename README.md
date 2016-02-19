# Little_PGer.py

## What is it?

It's a thin layer just a tad above SQL, for use with Postgres and
[psycopg2](http://www.initd.org/psycopg/), when you want to wrap
queries in a convenient way, using plain data structures (but you
don't feel like using an ORM, for some reason).

## Why?

Of course `psycopg2` already does a very fine job on its own, but in
the context of webapp backend development, I often found myself
wanting for an extra-frictionless way of shuffling around Ajax/JSON
data. As composing raw SQL queries quickly induces string-manipulation
fatigue, I gradually evolved `little_pger` for that simple purpose.

If you want to know more about it, I have also discussed its use in
some particular contexts, on my blog:

* <http://cjauvin.blogspot.com/2012/10/a-tribute-to-unsung-pattern.html>

* <http://cjauvin.blogspot.com/2013/04/impossibly-lean-access-control-with.html>

## To install

    $ pip install little_pger

or

    $ pip install -e git+git@github.com:cjauvin/little_pger.git#egg=little_pger

Note that `psycopg2` will be automatically installed if it isn't
already.

## Testing it with this README document

Note that this `README.md` file can be executed as a test suite. To do
so, simply create a dummy database (that you can destroy afterward):

    $ createdb little_pger_test -U <your_pg_user>

and set this variable appropriately for your setup:

```python
>>> pg_user = '' # an empty string works when the OS and PG users share the same name

```

Then simply execute the script with:

    $ python -m doctest -f -v README.md

Let's go!

```python
>>> from little_pger import LittlePGer

```

The first and mandatory parameter to a `LittlePGer` object is a
connection, either as a string or as a `psycopg2` object (resulting
from `psycopg2.connect`). A `LittlePGer` object can be used in two
ways. The first is as a context manager, which implies that the
transaction is encapsulated under the `with` statement (with a
`rollback` or `commit` performed automatically at exit):

```python
>>> conn_str = 'dbname=little_pger_test user={}'.format(pg_user)
>>> with LittlePGer(conn=conn_str, commit=False) as pg:
...     _ = pg.pg_version # (9, 5, 0) for me, perhaps not for you

```

You can also use it without the context manager:

```python
>>> pg = LittlePGer(conn=conn_str, commit=False)
>>> _ = pg.pg_version # (9, 5, 0) for me, perhaps not for you

```

in which case you are in charge of managing the transaction
yourself. In this document we will not use the context manager because
it makes things easier on the eyes.

## Insert and update

Suppose we have two SQL tables:

```python
>>> pg.sql("""
...     create table book (
...         book_id serial primary key,
...         author_id int,
...         title text,
...         n_pages int,
...         topics text[]
...     )
... """)

>>> pg.sql("""
...     create table author (
...         author_id serial primary key,
...         name text
...      )
... """)

```

you can `insert` a new `book`, along with its `author`:

```python
>>> book = pg.insert('book', values={'title': 'PG is Fun!'})
>>> author = pg.insert('author', values={'name': 'Joe Foo', 'author_id': 100})

```

and `update` it:

```python
>>> book = pg.update(
...     'book', set={'author_id': author['author_id'], 'n_pages': 200},
...     where={'book_id': book['book_id']}
... )
>>> sorted(book.items()) # just to clamp the field order
[('author_id', 100), ('book_id', 1), ('n_pages', 200), ('title', 'PG is Fun!'), ('topics', None)]

```

As shown above, `insert` and `update` by default return a `dict`
record. However, `insert` has a convenient `return_id` keyword
argument, which means that the primary key value of the newly
created record should be returned directly:

```python
>>> pg.insert(
...     'book', values={'title': 'Python and PG, a Love Story'},
...     return_id=True
... )
2

```

## Upsert

Even though `upsert` only appeared recently (with PG 9.5),
`little_pger` supports it for every version of PG, with a "fake
implementation" (i.e. check existence, then insert or update
accordingly) in the cases where it is not natively supported (and when
it is, a "real" implementation is used). Both implementations are
simplified versions where the primary key is implicitly used to
determine uniqueness.

```python
>>> # does not yet exist, will be created
>>> book_id = pg.upsert('book', set={'title': 'A Boring Story'}, return_id=True)
>>> book_id
3

>>> # already exists, will be updated
>>> book = pg.upsert('book', values={'n_pages': 123, 'book_id': book_id})
>>> book_id, book['book_id']
(3, 3)

```

`insert`, `update` and `upsert` all have a convenient `filter_values`
parameter which, if used, will remove any item in the `values` dict
that doesn't belong to the target table. Without it here, an exception
would be thrown, as the `book` table does not have a `publisher`
column:

```python
>>> _ = pg.upsert(
...     'book', filter_values=True,
...      values={'book_id': book_id, 'publisher': 'Joe North'}
... )

```

## Select

To `select` all books:

```python
>>> books = pg.select('book')
>>> len(books)
3

```

or a particular book:

```python
>>> books = pg.select('book', where={'book_id': book_id})
>>> len(books)
1

```

or:

```python
>>> book = pg.select1('book', where={'book_id': book_id})
>>> type(book)
<class 'psycopg2.extras.RealDictRow'>

```

It's easy to (inner) join books and authors:

```python
>>> book = pg.select1(
...     'book', join='author', where={'book_id': 1}
... )
>>> sorted(book.items()) # just to clamp the field order
[('author_id', 100), ('book_id', 1), ('n_pages', 200), ('name', 'Joe Foo'), ('title', 'PG is Fun!'), ('topics', None)]

```

or left join them:

```python
>>> book_author = pg.select1(
...     'book', left_join='author', where={'book_id': 2}
... )
>>> sorted(book_author.items()) # just to clamp the field order
[('author_id', None), ('book_id', 2), ('n_pages', None), ('name', None), ('title', 'Python and PG, a Love Story'), ('topics', None)]

```

Using a `tuple` value in the `where` clause:

```python
>>> books = pg.select('book', where={'book_id': (1, 2, 3)})
>>> len(books)
3

```

translates to a SQL query using the `in` operator:

```sql
select * from book where book_id in (1, 2, 3)
```

Make sure that you do not use `tuple`s and `list`s interchangeably
when working with `psycopg2` and `little_pger`, as they are used for
very different purposes. Python arrays translate into PG arrays (note
that the `book.topics` column has type `text[]`):

```python
>>> book = pg.update(
...     'book', set={'topics': ['database', 'programming']},
...     where={'book_id': 1}
... )
>>> book['topics']
['database', 'programming']

```

You can use operators other than `=`, like this:

```python
>>> books = pg.select('book', where={('book_id', '<='): 2})
>>> len(books)
2

```

Using a `set` (instead of a `tuple` or a `list`) will result in a
third type of semantics:

```python
>>> pg.select1(
...     'book', where={('title', 'like'): {'%PG%', '%Fun%'}}
... )['title']
'PG is Fun!'

```

which translates to:

```sql
select * from book where title like '%PG%' and title like '%Fun%'
```

which can be a powerful way to implement an autocomplete mechanism,
[as I explain in more details elsewhere](http://cjauvin.blogspot.ca/2012/10/a-tribute-to-unsung-pattern.html).

Until now we have assumed `*` selection, but the `what` keyword allows
for more flexibility:

```python
>>> res = pg.select(
...     'book', what={'*':1, 'title is not null': 'has_title'}
... )
>>> [book['has_title'] for book in res]
[True, True, True]

```

Similarly:

```python
>>> res = pg.select(
...     'book', left_join='author',
...      what=['name', 'count(*)'],
...      group_by='name', order_by='count desc'
... )
>>> res[0]['name'], int(res[0]['count'])
(None, 2)
>>> res[1]['name'], int(res[1]['count'])
('Joe Foo', 1)

```

## Delete

The `delete` function includes an option to "tighten" the
primary key sequence, to make sure that if you delete a row with some
ID that is the maximum one currently existing, it will be reused the
next time you create a new row (in other words: it prevents "gaps" in
the ID sequences).

Without `tighten_sequence`:

```python
>>> pg.delete('book', where={'book_id': 3})
>>> pg.insert('book', return_id=True)
4

```

With it:

```python
>>> pg.delete('book', where={'book_id': 4}, tighten_sequence=True)
>>> pg.insert('book', return_id=True)
3

```
