"""PostgreSQL/Psycopg2 helper "modulet" for common single table commands (select, insert, update, etc).

Christian Jauvin <cjauvin@gmail.com>
Created in January 2011
Updated in February 2012

"""

try:
    import psycopg2.extras
except ImportError:
    exit("Problem: the psycopg2 module doesn't seem to be available..")


def _getWhereClauseCompItem(c, v):
    if isinstance(c, tuple):
        assert len(c) == 2
        return c
    if isinstance(v, tuple):
        return (c, 'in')
    return (c, '=')


def _getWhereClause(items):
    return " and ".join(['%s %s %%s' % _getWhereClauseCompItem(c, v) for c, v in items])
            

def select(cursor, table, **kw):
    """SQL select statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    what -- projection items (str, list or dict, default '*')    
      ex1: what='color' --> "select color from .."
      ex2: what=['color', 'name'] --> "select color, name from .."
      ex3: what='max(price)' --> "select max(price) from .."
      ex4: what={'*':True, 'price is null':'is_price_valid'} --> "select *, price is null as is_price_valid from .."
      ex5: what=['age', 'count(*)'], group_by='age' --> "select age, count(*) from .. group by age"
    where -- where clause dict (default empty)
    order_by -- order by clause (str or list, default None)
    group_by -- group by clause (str or list, default None)
    limit -- limit clause (default None)
    offset -- offset clause (default None)
    rows -- all, or one row (default 'all'; if 'one', will assert that len <= 1)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)
    
    """
    assert set(kw.keys()).issubset(set(['what','where','order_by','group_by','limit','offset','rows','debug_print','debug_assert','_count'])), 'unknown keyword in pgsql_helper.select'
    what = kw.pop('what', '*')
    where = kw.pop('where', {})
    rows = kw.pop('rows', 'all')
    assert rows in ['all', 'one']
    #q = "set transform_null_equals to on; "
    q = ''
    proj_items = []
    if what: 
        if isinstance(what, dict): 
            proj_items = ['%s%s' % (w, ' as %s' % n if isinstance(n, basestring) else '') for w, n in what.items()]
        elif isinstance(what, basestring):
            proj_items = [what]
        else: 
            proj_items = list(what)
    if where:
        #where_clause = " and ".join(['%s %s %%s' % (c, 'in' if isinstance(v, tuple) else '=') for c, v in where.items()])
        where_clause = _getWhereClause(where.items())
        q += "select %s from %s where %s" % (', '.join(proj_items), table, where_clause)
    else:
        q += "select %s from %s" % (', '.join(proj_items), table)
    order_by = kw.pop('order_by', None)
    if order_by: 
        if isinstance(order_by, basestring): q += ' order by %s' % order_by
        else: q += ' order by %s' % ', '.join([e for e in order_by])
    group_by = kw.pop('group_by', None)
    if group_by: 
        if isinstance(group_by, basestring): q += ' group by %s' % group_by
        else: q += ' group by %s' % ', '.join([e for e in group_by])
    limit = kw.pop('limit', None)
    if limit: q += ' limit %s' % limit
    offset = kw.pop('offset', None)
    if offset: q += ' offset %s' % offset
    _execQuery(cursor, q, where.values(), **kw)
    results = cursor.fetchall()
    if rows == 'all':
        return results
    else:
        assert len(results) <= 1, 'your query returns more than one row'
        if results: return results[0]
        else: return None


def select1(cursor, table, column, **kw):
    """SQL select statement helper (syntactic sugar for single value select call).

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table
    column -- name of the column

    Optional keyword arguments:
    where -- where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)
    """                 
    assert set(kw.keys()).issubset(set(['where', 'debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.select1'
    value = select(cursor, table, what=column, rows='one', **kw)
    if value:
        return value[column if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]
    else: 
        return None


def select1r(cursor, table, **kw):
    """SQL select statement helper (syntactic sugar for single row select call).

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    what -- projection items (str, list or dict, default '*')    
      ex1: what='color' --> "select color from .."
      ex2: what=['color', 'name'] --> "select color, name from .."
      ex3: what='max(price)' --> "select max(price) from .."
      ex4: what={'*':True, 'price is null':'is_price_valid'} --> "select *, price is null as is_price_valid from .."
      ex5: what=['age', 'count(*)'], group_by='age' --> "select age, count(*) from .. group by age"
    where -- where clause dict (default empty)
    order_by -- order by clause (str or list, default None)
    group_by -- group by clause (str or list, default None)
    limit -- limit clause (default None)
    offset -- offset clause (default None)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """                 
    assert set(kw.keys()).issubset(set(['what','where','order_by','group_by','limit','offset','debug_print','debug_assert','_count'])), 'unknown keyword in pgsql_helper.select1r'
    return select(cursor, table, rows='one', **kw)


def selectId(cursor, table, **kw):
    """SQL select statement helper (fetch primary key value, assuming only one row).

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    where -- where clause dict (default empty)
    pkey_name -- if None (default), assume that the primary key name has the form "<table>_id"
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['where','pkey_name','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.selectId'
    pkey_name = kw.pop('pkey_name', '%s_id' % table)
    return select1(cursor, table, pkey_name, **kw)


def insert(cursor, table, **kw):
    """SQL insert statement helper, by default with a "returning *" clause.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    values -- dict with values to set (default empty)
    filter_values -- if True, trim values so that it contains only columns found in table (default False)
    return_id -- (potentially unsafe, use with caution) if True, will select the primary key value among the returning clause 
                 elements (assuming it has a "<table>_id" name form if using a dict-like cursor, or that it's 
                 at position 0 otherwise)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['values','filter_values','return_id','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.insert'
    values = kw.pop('values', {})
    return_id = kw.pop('return_id', False)
    if not values:
        q = "insert into %s default values returning *" % table
    else:
        if kw.pop('filter_values', False):
            columns = getColumns(cursor, table)
            values = dict([(c, v) for c, v in values.items() if c in columns])
        q = "insert into %s (%s) values (%s) returning *" % (table, ','.join(values.keys()), ','.join(['%s' for v in values]))    
    _execQuery(cursor, q, values.values(), **kw)
    returning = cursor.fetchone()
    if return_id:
        return returning['%s_id' % table if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]
    else:
        return returning
    

def update(cursor, table, **kw):
    """SQL update statement helper, with a "returning *" clause.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    set|values -- dict with values to set (either keyword works; default empty)
    where -- where clause dict (default empty)
    filter_values -- if True, trim values so that it contains only columns found in table (default False)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['set','values','where','filter_values','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.update'
    values = kw.pop('values', kw.pop('set', None))
    where = kw.pop('where', {})    
    if kw.pop('filter_values', False):
        columns = getColumns(cursor, table)
        values = dict([(c, v) for c, v in values.items() if c in columns])
    if not values: return
    if where:
        #where_clause = " and ".join(['%s %s %%s' % (c, 'in' if isinstance(v, tuple) else '=') for c, v in where.items()])
        where_clause = _getWhereClause(where.items())
        q = "update %s set (%s) = (%s) where %s returning *" % (table, ','.join(values.keys()), ','.join(['%s' for v in values]), where_clause)
    else:
        q = "update %s set (%s) = (%s) returning *" % (table, ','.join(values.keys()), ','.join(['%s' for v in values]))
    _execQuery(cursor, q, values.values() + where.values(), **kw)
    return cursor.fetchone()


def delete(cursor, table, **kw):
    """SQL delete statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table
    
    Optional keyword arguments:
    where -- where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['where','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.delete'
    where = kw.pop('where', {})
    if where:
        #where_clause = " and ".join(['%s %s %%s' % (c, 'in' if isinstance(v, tuple) else '=') for c, v in where.items()])
        where_clause = _getWhereClause(where.items())
        q = "delete from %s where %s" % (table, where_clause)
    else:
        q = "delete from %s" % table
    _execQuery(cursor, q, where.values(), **kw)


def count(cursor, table, **kw):
    """SQL select count statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    where -- where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """    
    assert set(kw.keys()).issubset(set(['where','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.count'
    row = select(cursor, table, what='count(*)', rows='one', **kw)
    return row['count' if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]


def exists(cursor, table, **kw):
    """Check whether at least one record exists.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    where -- where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """    
    assert set(kw.keys()).issubset(set(['where','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.exists'
    return select(cursor, table, limit=1, rows='one', **kw) is not None


def getCurrentPKeyValue(cursor, table, **kw):
    """Current value of the primary key.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    pkey_seq_name -- if None (default), assume that the primary key sequence name has the 
                     form "<table>_<table>_id_seq"
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['pkey_seq_name','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.getCurrentPKeyValue'
    pkey_seq_name = kw.pop('pkey_seq_name', '%s_%s_id_seq' % (table, table))
    _execQuery(cursor, "select currval(%s)", [pkey_seq_name], **kw)
    return cursor.fetchone()['currval' if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]


def getNextPKeyValue(cursor, table, pkey_seq_name=None):
    """Next value of the primary key.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    pkey_seq_name -- if None (default), assume that the primary key sequence name has the 
                     form "<table>_<table>_id_seq"
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['pkey_seq_name','debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.getNextPKeyValue'
    pkey_seq_name = kw.pop('pkey_seq_name', '%s_%s_id_seq' % (table, table))
    _execQuery(cursor, "select nextval(%s)", [pkey_seq_name], **kw)
    return cursor.fetchone()['nextval' if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]


def getNullableColumns(cursor, table):
    """Return all nullable columns.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['debug_print','debug_assert'])), 'unknown keyword in pgsql_helper.getNullableColumns'
    _execQuery(cursor, "select * from information_schema.columns where table_name = %s", [table], **kw)
    nullable_columns = []
    for row in cursor.fetchall():
        if cursor.__class__ not in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor]:
            row = dict(zip([rec[0] for rec in cursor.description], row))
        if row['is_nullable'] == 'YES': nullable_columns.append(row['column_name'])
    return nullable_columns


def getColumns(cursor, table):
    """Return all columns.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    """
    cursor.execute('select * from %s where 1=0' % table)
    return [rec[0] for rec in cursor.description]


def _execQuery(cursor, query, qvalues=[], **kw):
    """(Internal, should not be used) Execute a query.

    Mandatory positional arguments:
    cursor -- the cursor
    query -- query string (with %%s value placeholders if needed)
    qvalues -- query value list (default empty)

    Optional keyword arguments:
    transform_null_equals -- prepend 'set transform_null_equals to on' to query (default True)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['debug_print','debug_assert'])), 'unknown keyword in pgsql_helper._execQuery'

    query = "set transform_null_equals to on; " + query
    if kw.get('debug_print', False):
        print cursor.mogrify(query, qvalues)
    if kw.get('debug_assert', False):
        assert False, cursor.mogrify(query, qvalues)
    cursor.execute(query, qvalues)
