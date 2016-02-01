# Little_PGer.py

A thin layer just a tad above SQL, for use with Postgres and
[psycopg2](http://www.initd.org/psycopg/), when you want to wrap
queries in a convenient way, using plain data structures (but you
don't feel like using an ORM, for some reason).

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

    $ pip install -e -e git+git@github.com:cjauvin/little_pger.git#egg=little_pger

Note that `psycopg2` will be automatically installed if it isn't
already.

## Testing it with this README document

Note that this `README.md` file can be executed as a test. To do so,
simply create a simple database (that you can destroy afterward):

    $ createdb little_pger_test

And then simply execute the script with:

    $ python -m doctest -f -v README.md

Let's go!

    >>> from little_pger import LittlePGer

The first and mandatory parameter to a `LittlePGer` object is
either a connection string or a `psycopg2` connection object. Note
that in order for the user-less connection string below to work, you
will have either to create a PG user corresponding to your current OS
user, or add a `user=whatever` clause to the string.

`little_pger` can be used in two ways. The first is as a context
manager, which implies that the transaction is encapsulated under the
`with` statement (with a `rollback` or `commit` performed
automatically at exit):

    >>> with LittlePGer(conn="dbname=little_pger_test", commit=False) as pg:
    ...     _ = pg.pg_version # (9, 5, 0) for me, perhaps not for you

You can also use it without the context manager:

    >>> pg = LittlePGer(conn="dbname=little_pger_test", commit=False)
    >>> _ = pg.pg_version # (9, 5, 0) for me, perhaps not for you

in which case you are in charge of managing the transaction
yourself. In this document we will not use the context manager because
it makes things easier to follow.

## Insert and update

Suppose we have two SQL tables:

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

you can `insert` and `update` a new `book` record:

    >>> book = pg.insert('book', values={'title': 'PG is Fun!'})
    >>> joe = pg.insert('author', values={'name': 'Joe Foo', 'author_id': 100})
    >>> book = pg.update(
    ...     'book', set={'author_id': joe['author_id'], 'n_pages': 200},
    ...     where={'book_id': book['book_id']}
    ... )
    >>> sorted(book.items()) # just to clamp the field order
    [('author_id', 100), ('book_id', 1), ('n_pages', 200), ('title', 'PG is Fun!'), ('topics', None)]

As shown above, `insert` and `update` by default return a `dict`
record. However, `insert` has a convenient `return_id` keyword
argument, which means that only the primary key value of the newly
created record wille be returned:

    >>> book_id = pg.insert(
    ...     'book', values={'title': 'Python and PG, a Love Story'},
    ...     return_id=True
    ... )
    >>> book_id
    2

    >>> _ = pg.update('book', values={'n_pages': 450}, where={'book_id': book_id})

## Upsert

Even though `upsert` only appeared recently (with PG 9.5),
`little_pger` supports it for every version of PG, with a "fake
implementation" (i.e. check existence, then insert or update
accordingly) in the cases where it is not natively supported (and when
it is, a "real implementation is used). Both implementations are
simplified versions where the primary key is implicitly used to
determine uniqueness.

    >>> book_id = pg.upsert('book', set={'title': 'A Boring Story'}, return_id=True)
    >>> book = pg.upsert('book', values={'n_pages': 123, 'book_id': book_id})
    >>> book_id, book['book_id']
    (3, 3)

## Select

To `select` all books:

    >>> books = pg.select('book')
    >>> len(books)
    3

or a particular book:

    >>> books = pg.select('book', where={'book_id': book_id})
    >>> len(books)
    1

or:

    >>> book = pg.select1('book', where={'book_id': book_id})
    >>> type(book)
    <class 'psycopg2.extras.RealDictRow'>

It's easy to (inner) join books and authors:

    >>> book = pg.select1(
    ...     'book', join='author',
    ...     where={'book_id': 1}
    ... )
    >>> sorted(book.items()) # just to clamp the field order
    [('author_id', 100), ('book_id', 1), ('n_pages', 200), ('name', 'Joe Foo'), ('title', 'PG is Fun!'), ('topics', None)]

or left join them:

    >>> book_author = pg.select1(
    ...     'book', left_join='author',
    ...     where={'book_id': 2}
    ... )
    >>> sorted(book_author.items()) # just to clamp the field order
    [('author_id', None), ('book_id', 2), ('n_pages', 450), ('name', None), ('title', 'Python and PG, a Love Story'), ('topics', None)]

Using a `tuple` value in the `where` clause:

    >>> books = pg.select('book', where={'book_id': (1, 2, 3)})
    >>> len(books)
    3

translates to a SQL query using the `in` operator:

    select * from book where book_id in (1, 2, 3)

Make sure that you do not use `tuple`s and `list`s interchangeably
when working with `psycopg2` and `little_pger`, as they are used for
very different purposes. Python arrays translate into PG arrays (note
that the `book.topics` column has type `text[]`):

    >>> book = pg.update(
    ...     'book', set={'topics': ['database', 'programming']},
    ...     where={'book_id': 1}
    ... )
    >>> book['topics']
    ['database', 'programming']

    >>> pg.delete('book', where={'book_id': book_id}, tighten_sequence=True)
    >>> pg.count('book')
    2

    >>> pg.exists('book', where={'book_id': book_id})
    False

While we're at it, using a `set` (instead of a `tuple` or a `dict`)
will result in a third type of semantics, as

```python
select(cur, 'book', where={('title', 'like'): {'%PG%', '%Fun%'}})
```

translates to:

```sql
select * from book where title like '%PG%' and title like '%Fun%'
```

which can be a powerful way to implement an autocomplete mechanism,
[as I explain in more details elsewhere](http://cjauvin.blogspot.ca/2012/10/a-tribute-to-unsung-pattern.html).
Because this example wouldn't make sense with the `=` operator (which
`where` uses by default), note in passing the use of the `(<column>,
<operator>)` tuple as a `dict` key (which requires hashability) to
specify that we want the `like` operator for this query. Similarly,
this would work in the expected way:

```python
select(cur, 'book', where={('n_pages', '<='): 200})
```

Until now we have assumed `*` selection, but the `what` keyword allows
for more flexibility:

```python
select(cur, 'book', what={'*':1, 'title is not null': 'has_title'})
```

would be translated as

```sql
select *, title is not null as has_title from book
```

Similarly:

```python
select(cur, 'book', what=['author_id', 'count(*)'], group_by='author_id')
```

demonstrates the `group_by` keyword argument, working as expected. Our two
tables can also be inner joined easily:

```python
select(cur, 'book', {'book': 'b', 'author': 'a'}, join={'b.author_id': 'a.author_id'})
```

Finally, `little_pger` offers a bunch of other functions, working in
ways similar to the ones described above:

```python
selectId # directly returns the id, using the pkey_name argument or assuming <table_id>
count    # select count(*), returns integer
delete
exists   # return boolean
getCurrentPKeyValue
getNextPKeyValue
getColumns
```
