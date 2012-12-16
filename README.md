little_PGer.py
==============

A small set of functions for conveniently and pythonically wrapping
SQL commands when you work with Postgres +
[psycopg2](http://www.initd.org/psycopg/).

Suppose you have two SQL tables:

```sql
create table book (
    book_id serial primary key,
    author_id int,
    title text,
    n_pages int,
    topics text[]
);

create table author (
    author_id serial primary key,
    first_name text,
    last_name text
);
```

and a pair of `RealDict`-based `psycopg2` `connection`/`cursor`:

```python
conn = psycopg2.connect("dbname=...",
                        connection_factory=psycopg2.extras.RealDictConnection)
cur = conn.cursor()
```

you can `insert` and `update` a new `book` record:

```python
book = insert(cur, 'book', values={'title': 'PG is Fun!'})
book = update(cur, 'book', set={'n_pages': 200}, where={'title': 'PG is Fun!'})
```

Note that you are always responsible for managing the transaction when
you are done:

```python
conn.commit()
```

As shown above, `insert` and `update` by default return a `dict`
record. However, when using the `return_id` keyword arg with `insert`,
the `id` (typically the primary key as an integer) will be directly
returned, assuming the corresponding field is named `<table>_id`:

```python
book_id = insert(cur, 'book', values={'title':'PG is Fun!'}, return_id=True)
update(cur, 'book', values={'n_pages': 200}, where={'book_id': book_id})
```

Note that the `set` and `values` keywords are equivalent when using
`update`. To `select` all books written by a certain author:

```python
select(cur, 'book', where={'author_id': 100})
```

which will return a list of `dict` records, as expected. If a unique
match is expected

```python
book = select1r(cur, 'book', where={'author_id': 100})
title = select1(cur, 'book', 'title', where={'author_id': 100})
```

will both throw an exception if more than one rows are
retrieved. Using a `tuple` value in the `where` clause:

```python
select(cur, 'book', where={'author_id': (100, 200, 300)})
```

translates to a SQL query using the `in` operator:

```sql
select * from book where author_id in (100, 200, 300)
```

Make sure that you do not use `tuple`s and `list`s interchangeably
when working with `psycopg2` and `little_pger`, as a `list` is used
for a very different purpose as

```python
select(cur, 'book', where={'topics': ['database', 'programming']})
```

translates to this Postgres `array`-based query (note that the
`topics` column above is `text[]`, not `text`):

```sql
select * from book where topics = '{database, programming}'
```

While we're at it, using a `set` (instead of a `tuple` or a `dict`)
will result in a third type of semantics, as

```python
select(cur, 'book', where={('title', 'like'): {'%PG%', '%Fun%'}})
```

translates to:

```sql
select * from book where title like '%PG%' and title like '%Fun%'
```

which can be a powerful way to implement autocomplete mechanisms, [as
I explain in more details
elsewhere](http://cjauvin.blogspot.ca/2012/10/a-tribute-to-unsung-pattern.html).

