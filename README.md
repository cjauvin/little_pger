little_PGer.py
==============

A small set of functions for pythonically wrapping SQL commands when
you work with Postgres + [Psycopg2](http://www.initd.org/psycopg/).

Suppose you have two SQL tables:

```sql
create table document (
    document_id serial primary key,
    author_id int,
    title text,
    type text check (type in ('book', 'article', 'essay'),
    topics text[]
);

create table author (
    author_id serial primary key,
    first_name text,
    last_name text
);
```

and a pair of connection/cursor:

```python
conn = psycopg2.connect("dbname=...")
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
```
