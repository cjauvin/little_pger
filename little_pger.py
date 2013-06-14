"""PostgreSQL/Psycopg2 helper "modulet" for common single table commands (select, insert, update, etc).

.. moduleauthor:: Christian Jauvin <cjauvin@gmail.com>

"""

import collections
try:
    import psycopg2.extras
except ImportError:
    exit("Problem: the psycopg2 module doesn't seem to be available..")


def select(cursor, table, **kw):
    """SQL select statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table (str for single table, list or dict for join)

    Optional keyword arguments:
    what -- projection items (str, list or dict, default '*')
      ex1: what='color' --> "select color from .."
      ex2: what=['color', 'name'] --> "select color, name from .."
      ex3: what='max(price)' --> "select max(price) from .."
      ex4: what={'*':True, 'price is null':'is_price_valid'} --> "select *, price is null as is_price_valid from .."
      ex5: what=['age', 'count(*)'], group_by='age' --> "select age, count(*) from .. group by age"
    join -- AND-joined join clause dict (default empty)
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    group_by -- group by clause (str or list, default None)
    order_by -- order by clause (str or list, default None)
    limit -- limit clause (default None)
    offset -- offset clause (default None)
    rows -- all, or one row (default 'all'; if 'one', will assert that len <= 1)
    get_count -- wrap the entire query inside a select count(*) outer query (default False)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['what','join','where','where_or','order_by','group_by',
                                        'limit','offset','rows','get_count','debug_print','debug_assert'])), 'unknown keyword in select'
    what = kw.pop('what', '*')
    join = kw.pop('join', {})
    where = kw.pop('where', {}).copy() # need a copy because we might pop an 'exists' item
    where_or = kw.pop('where_or', {}).copy() # idem
    order_by = kw.pop('order_by', None)
    group_by = kw.pop('group_by', None)
    limit = kw.pop('limit', None)
    offset = kw.pop('offset', None)
    rows = kw.pop('rows', 'all')
    assert rows in ('all', 'one')
    get_count = kw.pop('get_count', False)
    proj_items = []
    if what:
        if isinstance(what, dict):
            proj_items = ['%s%s' % (w, ' as %s' % n if isinstance(n, basestring) else '') for w, n in what.items()]
        elif isinstance(what, basestring):
            proj_items = [what]
        else:
            proj_items = list(what)
    if isinstance(table, dict):
        table = ['%s %s' % (t, a) for t, a in table.items()]
    elif isinstance(table, basestring):
        table = [table]
    else:
        table = list(table)
    q = "select %s from %s where true " % (', '.join(proj_items), ', '.join(table))
    if join:
        join_clause = ' and '.join('%s = %s' % (k, v) for k, v in join.items())
        q += ' and %s ' % join_clause
    if where:
        where_clause = _getWhereClause(where.items())
        q += " and %s" % where_clause
        where.pop('exists', None)
    if where_or:
        where_or_clause = _getWhereClause(where_or.items(), 'or')
        q += ' and (%s)' % where_or_clause
        where_or.pop('exists', None)
    if group_by:
        if isinstance(group_by, basestring): q += ' group by %s' % group_by
        else: q += ' group by %s' % ', '.join([e for e in group_by])
    if order_by:
        if isinstance(order_by, basestring): q += ' order by %s' % order_by
        else: q += ' order by %s' % ', '.join([e for e in order_by])
    if limit: q += ' limit %s' % limit
    if offset: q += ' offset %s' % offset
    if get_count:
        q = 'select count(*) from (%s) _' % q
        rows = 'one'
    _execQuery(cursor, q, where.values() + where_or.values(), **kw)
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
    join -- AND-joined join clause dict (default empty)
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)
    """
    assert set(kw.keys()).issubset(set(['join','where','where_or','debug_print','debug_assert'])), 'unknown keyword in select1'
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
    join -- AND-joined join clause dict (default empty)
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    group_by -- group by clause (str or list, default None)
    order_by -- order by clause (str or list, default None)
    limit -- limit clause (default None)
    offset -- offset clause (default None)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['what','join','where','where_or','order_by','group_by','limit','offset','debug_print','debug_assert'])), 'unknown keyword in select1r'
    return select(cursor, table, rows='one', **kw)


def selectId(cursor, table, **kw):
    """SQL select statement helper (fetch primary key value, assuming only one row).

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    pkey_name -- if None (default), assume that the primary key name has the form "<table>_id"
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['where','where_or','pkey_name','debug_print','debug_assert'])), 'unknown keyword in selectId'
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
    map_values -- dict containing a mapping to be performed on 'values' (e.g. {'': None}, to convert empty strings to nulls)
    return_id -- (potentially unsafe, use with caution) if True, will select the primary key value among the returning clause
                 elements (assuming it has a "<table>_id" name form if using a dict-like cursor, or that it's
                 at position 0 otherwise)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['values','filter_values','map_values','return_id','debug_print','debug_assert'])), 'unknown keyword in insert'
    values = kw.pop('values', {})
    return_id = kw.pop('return_id', False)
    if not values:
        q = "insert into %s default values returning *" % table
    else:
        if kw.pop('filter_values', False):
            columns = getColumns(cursor, table)
            values = dict([(c, v) for c, v in values.items() if c in columns])
        map_values = kw.pop('map_values', {})
        values =  dict((k, map_values.get(v, v) if isinstance(v, collections.Hashable) else v) for k, v in values.items())
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
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    filter_values -- if True, trim values so that it contains only columns found in table (default False)
    map_values -- dict containing a mapping to be performed on 'values' (e.g. {'': None}, to convert empty strings to nulls)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['set','values','where','where_or','filter_values','map_values','debug_print','debug_assert'])), 'unknown keyword in update'
    values = kw.pop('values', kw.pop('set', None))
    where = kw.pop('where', {})
    where_or = kw.pop('where_or', {})
    if kw.pop('filter_values', False):
        columns = getColumns(cursor, table)
        values = dict([(c, v) for c, v in values.items() if c in columns])
    map_values = kw.pop('map_values', {})
    values =  dict((k, map_values.get(v, v) if isinstance(v, collections.Hashable) else v) for k, v in values.items())
    q = 'update %s set (%s) = (%s)' % (table, ','.join(values.keys()), ','.join(['%s' for v in values]))
    if where:
        where_clause = _getWhereClause(where.items())
        q += " where %s" % where_clause
    if where_or:
        where_or_clause = _getWhereClause(where_or.items(), 'or')
        if where:
            q += ' and (%s)' % where_or_clause
        else:
            q += ' where %s' % where_or_clause
    q += ' returning *'
    _execQuery(cursor, q, values.values() + where.values() + where_or.values(), **kw)
    return cursor.fetchone()


def upsert(cursor, table, **kw):
    """SQL insert/update statement helper, with a "returning *" clause.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    set|values -- dict with values to set (either keyword works; default empty)
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    filter_values -- if True, trim values so that it contains only columns found in table (default False)
    map_values -- dict containing a mapping to be performed on 'values' (e.g. {'': None}, to convert empty strings to nulls)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    exists_kw = kw.copy()
    for k in exists_kw.keys():
        if k not in set(['what','where','where_or','debug_print','debug_assert']):
            exists_kw.pop(k)
    if exists(cursor, table, **exists_kw):
        for k in kw.keys():
            if k not in set(['set','values','where','where_or','filter_values','map_values','debug_print','debug_assert']):
                kw.pop(k)
        return update(cursor, table, **kw)
    else:
        if 'set' in kw:
            kw['values'] = kw.pop('set')
        for k in kw.keys():
            if k not in set(['values','filter_values','map_values','return_id','debug_print','debug_assert']):
                kw.pop(k)
        return insert(cursor, table, **kw)


def delete(cursor, table, **kw):
    """SQL delete statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    tighten_sequence -- if True, will decrement the pkey sequence when deleting the latest row,
                        and has no effect otherwise (default False)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['where','where_or','tighten_sequence','debug_print','debug_assert'])), 'unknown keyword in delete'
    where = kw.pop('where', {})
    where_or = kw.pop('where_or', {})
    q = "delete from %s" % table
    if where:
        where_clause = _getWhereClause(where.items())
        q += " where %s" % where_clause
    if where_or:
        where_or_clause = _getWhereClause(where_or.items(), 'or')
        if where:
            q += ' and (%s)' % where_or_clause
        else:
            q += ' where %s' % where_or_clause
    if kw.pop('tighten_sequence', False):
        pkey_name = getPKeyColumn(cursor, table)
        # here we assume that the pkey sequence name is 'table_pkey_seq', which it will be if
        # it was created implicitly; idea taken from: http://stackoverflow.com/a/244265/787842
        if pkey_name:
            q+= "; select setval('%s_%s_seq', coalesce((select max(%s) + 1 from %s), 1), false)" % (table, pkey_name, pkey_name, table)
    _execQuery(cursor, q, where.values() + where_or.values(), **kw)


def count(cursor, table, **kw):
    """SQL select count statement helper.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    join -- AND-joined join clause dict (default empty)
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    order_by -- order by clause (str or list, default None)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['what','join','where','where_or','group_by','debug_print','debug_assert'])), 'unknown keyword in count'
    if kw.get('group_by', None) is None:
        kw.pop('what', None) # if it's there, we can remove it safely, as it won't affect the row count
        row = select(cursor, table, what='count(*)', rows='one', **kw)
    else:
        row = select(cursor, table, get_count=True, **kw)
    return row['count' if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]


def exists(cursor, table, **kw):
    """Check whether at least one record exists.

    Mandatory positional arguments:
    cursor -- the cursor
    table -- name of the table

    Optional keyword arguments:
    what -- projection items (str, list or dict, default '*')
    where -- AND-joined where clause dict (default empty)
    where_or -- OR-joined where clause dict (default empty)
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    assert set(kw.keys()).issubset(set(['what','where','where_or','debug_print','debug_assert'])), 'unknown keyword in exists'
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
    assert set(kw.keys()).issubset(set(['pkey_seq_name','debug_print','debug_assert'])), 'unknown keyword in getCurrentPKeyValue'
    pkey_seq_name = kw.pop('pkey_seq_name', '%s_%s_id_seq' % (table, table))
    _execQuery(cursor, "select currval(%s)", [pkey_seq_name], **kw)
    return cursor.fetchone()['currval' if cursor.__class__ in [psycopg2.extras.DictCursor, psycopg2.extras.RealDictCursor] else 0]


def getNextPKeyValue(cursor, table, **kw):
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
    assert set(kw.keys()).issubset(set(['pkey_seq_name','debug_print','debug_assert'])), 'unknown keyword in getNextPKeyValue'
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
    assert set(kw.keys()).issubset(set(['debug_print','debug_assert'])), 'unknown keyword in getNullableColumns'
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


# http://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
def getPKeyColumn(cursor, table):
    cursor.execute("""
        select pg_attribute.attname as pkey_name
        from pg_index, pg_class, pg_attribute
        where
           pg_class.oid = %s::regclass and indrelid = pg_class.oid and
           pg_attribute.attrelid = pg_class.oid and
           pg_attribute.attnum = any(pg_index.indkey) and indisprimary;
    """, [table])
    return (cursor.fetchone() or {}).get('pkey_name')


################################################################################

def _flatten(values):
    v = []
    for val in values:
        if isinstance(val, set):
            v += list(val)
        else:
            v.append(val)
    return v


# returns a triple: (field, comp_operator, value placeholder)
def _getWhereClauseCompItem(c, v):
    if isinstance(c, tuple):
        assert len(c) in [2, 3]
        if len(c) == 2:
            # (field, comp_operator, value placeholder)
            return c + ('%s',)
        elif len(c) == 3:
            # (field, comp_operator, transformed value placeholder)
            return ('%s(%s)' % (c[2], c[0]), c[1], '%s(%%s)' % c[2])
    if isinstance(v, tuple):
        return (c, 'in', '%s')
    return (c, '=', '%s')


def _getWhereClause(items, type='and'):
    assert type in ('and', 'or')
    wc = []
    for c, v in items:
        if c == 'exists':
            assert isinstance(v, basestring)
            wc.append('exists (%s)' % v)
        elif isinstance(v, set):
            sub_wc = ' and '.join(['%s %s %s' % _getWhereClauseCompItem(c, vv) for vv in v])
            wc.append('(%s)' % sub_wc)
        else:
            wc.append('%s %s %s' % _getWhereClauseCompItem(c, v))
    return (" %s " % type).join(wc)


def _execQuery(cursor, query, qvalues=[], **kw):
    """(Internal, should not be used) Execute a query.

    Mandatory positional arguments:
    cursor -- the cursor
    query -- query string (with %%s value placeholders if needed)
    qvalues -- query value list (default empty)

    Optional keyword arguments:
    debug_print -- print query before executing it (default False)
    debug_assert -- throw assert exception (showing query), without executing it;
                    useful for web dev debugging (default False)

    """
    # Should I add a switch to prevent setting transform_null_equals to on?
    assert set(kw.keys()).issubset(set(['debug_print','debug_assert'])), 'unknown keyword in _execQuery'

    query = "set transform_null_equals to on; " + query
    qvalues = _flatten(qvalues)
    if kw.get('debug_print', False):
        print cursor.mogrify(query if isinstance(query, str) else query.encode('utf8'), qvalues)
    if kw.get('debug_assert', False):
        assert False, cursor.mogrify(query if isinstance(query, str) else query.encode('utf8'), qvalues)
    cursor.execute(query, qvalues)
