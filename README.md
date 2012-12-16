little_PGer.py
==============

A small set of functions for pythonically wrapping SQL commands when
you work with Postgres + [psycopg2](http://www.initd.org/psycopg/).

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

and a pair of `psycopg2` connection/cursor:

```python
conn = psycopg2.connect("dbname=...")
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
```

you can `insert` and `update` a new `book` record:

```python
insert(cur, 'book', values={'title': 'PG is Fun!'})
update(cur, 'book', set={'n_pages': 200}, where={'title': 'PG is Fun!'})
```

Note that you are always responsible for managing the transaction when you are done:

```python
conn.commit()
```

With the `return_id` option (which restricts the default `returning *`
clause to the primary key's value, assumed to be named `<table>_id`),
the `insert`/`update` above could also have been done this way:

```python
book_id = insert(cur, 'book', values={'title':'PG is Fun!'}, return_id=True)
update(cur, 'book', set={'n_pages': 200}, where={'book_id': book_id})
```
