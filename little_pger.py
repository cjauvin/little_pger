"""A thin PostgreSQL/Psycopg2 wrapper, using only plain data structures.

.. moduleauthor:: Christian Jauvin <cjauvin@gmail.com>

"""
from __future__ import division, print_function, unicode_literals
import collections
from pkg_resources import get_distribution
import psycopg2.extras
import re
from six import string_types


__version__ = get_distribution('little_pger').version


# Helper functions

def _flatten(values):
    v = []
    for val in values:
        if isinstance(val, set):
            v += list(val)
        else:
            v.append(val)
    return v


# returns a triple: (field, comp_operator, value placeholder)
def _get_where_clause_comp_item(c, v):
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


def _get_where_clause(items, type_='and'):
    assert type_ in ('and', 'or')
    wc = []
    for c, v in items:
        if c == 'exists':
            assert isinstance(v, string_types)
            wc.append('exists (%s)' % v)
        elif isinstance(v, set):
            sub_wc = ' and '.join(['%s %s %s' % _get_where_clause_comp_item(c, vv) for vv in v])  # noqa
            wc.append('(%s)' % sub_wc)
        else:
            wc.append('%s %s %s' % _get_where_clause_comp_item(c, v))
    return (" %s " % type_).join(wc)


def _check_args(func_name, args, allowed_args):
    if not set(args) <= set(allowed_args):
        s = 'unexpected keyword argument(s) in {}: {}'
        s = s.format(func_name, list(set(args) - set(allowed_args)))
        raise TypeError(s)
    return True


TableInfos = collections.namedtuple(
    'TableInfo',
    ['pkey', 'columns']
)


class LittlePGerError(Exception):
    pass


class LittlePGer(object):

    def __init__(self, conn, commit=False):
        if isinstance(conn, string_types):
            self.conn = psycopg2.connect(conn)
        else:
            self.conn = conn
        self.cursor = self.conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        )
        self._table_infos = {}  # name -> TableInfos
        self._to_commit = commit
        self.pg_version = self.get_pg_version()
        v = self.pg_version
        if v[0] > 9 or (v[0] >= 9 and v[1] >= 5):
            self._upsert_impl = self._real_upsert
        else:
            self._upsert_impl = self._fake_upsert

    def __enter__(self):
        return LittlePGer(self.conn, self._to_commit)

    def __exit__(self, exc_type, exc_value, traceback):
        if self._to_commit:
            self.commit()
            # print('commit')
        else:
            self.conn.rollback()
            # print('rollback')

    def commit(self):
        if not self._to_commit:
            raise LittlePGerError('asking to commit when _to_commit is False')
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def get_pg_version(self):
        self.cursor.execute('select version() as ver')
        rec = self.cursor.fetchone()
        return tuple(
            [int(d) for d in re.search('PostgreSQL (\d+)[.](\d+)[.](\d+)', rec['ver']).groups()]  # noqa
        )

    def select(self, table, **kw):
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
        [inner_]join -- either: table `str` or (table, field) `tuple`, (or list of those)
        left_join -- similar to join
        right_join -- similar to join
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
        _check_args('select', kw.keys(), (
            'what', 'join', 'inner_join', 'left_join', 'right_join', 'where',
            'where_or', 'order_by', 'group_by', 'limit', 'offset', 'rows',
            'get_count', 'debug_print', 'debug_assert'
        ))
        what = kw.pop('what', '*')
        inner_join = kw.pop('join', kw.pop('inner_join', {}))
        left_join = kw.pop('left_join', {})
        right_join = kw.pop('right_join', {})
        where = kw.pop('where', {}).copy() # need a copy because we might pop an 'exists' item
        where_or = kw.pop('where_or', {}).copy() # idem
        order_by = kw.pop('order_by', None)
        group_by = kw.pop('group_by', None)
        limit = kw.pop('limit', None)
        offset = kw.pop('offset', None)
        rows = kw.pop('rows', 'all')
        if rows not in ('all', 'one'):
            raise LittlePGerError('rows arg should be either all or one')
        get_count = kw.pop('get_count', False)
        proj_items = []
        if what:
            if isinstance(what, dict):
                proj_items = [
                    '%s%s' % (w, ' as %s' % n if isinstance(n, string_types) else '')
                    for w, n in what.items()
                ]
            elif isinstance(what, string_types):
                proj_items = [what]
            else:
                proj_items = list(what)
        # if isinstance(table, dict):
        #     table = ['%s %s' % (t, a) for t, a in table.items()]
        # elif isinstance(table, string_types):
        #     table = [table]
        # else:
        #     table = list(table)
        # q = "select %s from %s " % (', '.join(proj_items), ', '.join(table))
        assert isinstance(table, string_types)
        q = "select {proj} from {table} ".format(
            proj=', '.join(proj_items),
            table=table
        )

        jj = [(inner_join, 'inner'), (left_join, 'left'), (right_join, 'right')]
        for join_elem, join_type in jj:
            if not join_elem:
                continue
            for e in (join_elem if isinstance(join_elem, list) else [join_elem]):
                if isinstance(e, string_types):
                    pkey = self.get_table_infos(e.split()[0]).pkey # split()[0] in case of a 'bla b' pattern
                    q += ' %s join %s using (%s)' % (join_type, e, pkey)
                elif isinstance(e, tuple):
                    # if len(e) == 3:
                    #     t, f1, f2 = e
                    #     q += ' %s join %s on %s = %s' % (join_type, t, f1, f2)
                    if len(e) == 2:
                        t, f = e
                        q += ' %s join %s using (%s)' % (join_type, t, f)
                    else:
                        raise LittlePGerError('wrong data type for `join`: can only be table_as_str, a (table_as_str, field_as_str) tuple, or a list of those')
                else:
                    raise LittlePGerError('wrong data type for `join`: can only be table_as_str, a (table_as_str, field_as_str) tuple, or a list of those')

        q += ' where true '

        if where:
            where_clause = _get_where_clause(where.items())
            q += " and %s" % where_clause
            where.pop('exists', None)
        if where_or:
            where_or_clause = _get_where_clause(where_or.items(), 'or')
            q += ' and (%s)' % where_or_clause
            where_or.pop('exists', None)
        if group_by:
            if isinstance(group_by, string_types): q += ' group by %s' % group_by
            else: q += ' group by %s' % ', '.join([e for e in group_by])
        if order_by:
            if isinstance(order_by, string_types): q += ' order by %s' % order_by
            else: q += ' order by %s' % ', '.join([e for e in order_by])
        if limit: q += ' limit %s' % limit
        if offset: q += ' offset %s' % offset
        if get_count:
            q = 'select count(*) from (%s) _' % q
            rows = 'one'
        self._exec_query(q, list(where.values()) + list(where_or.values()), **kw)
        results = self.cursor.fetchall()
        if rows == 'all':
            return results
        elif rows == 'one':
            if len(results) > 1:
                raise LittlePGerError('your query returns more than one row')
            return results[0] if results else None

    def select1(self, table, **kw):
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
        [inner_]join -- either: table `str` or (table, field) `tuple`, (or list of those)
        left_join -- similar to join
        right_join -- similar to join
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
        _check_args('select1', kw.keys(), (
            'what', 'join', 'inner_join', 'left_join', 'right_join', 'where',
            'where_or', 'order_by', 'group_by','limit','offset','debug_print',
            'debug_assert'
        ))
        return self.select(table, rows='one', **kw)

    def select_id(self, table, **kw):
        """SQL select statement helper (fetch primary key value, assuming only one row).

        Mandatory positional arguments:
        cursor -- the cursor
        table -- name of the table

        Optional keyword arguments:
        where -- AND-joined where clause dict (default empty)
        where_or -- OR-joined where clause dict (default empty)
        debug_print -- print query before executing it (default False)
        debug_assert -- throw assert exception (showing query), without executing it;
                        useful for web dev debugging (default False)

        """
        _check_args('select_id', kw.keys(), (
            'where', 'where_or', 'debug_print', 'debug_assert'
        ))
        pkey = self.get_table_infos(table).pkey
        row = self.select1(table, **kw)
        return row[pkey] if row else None

    def insert(self, table, **kw):
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
        _check_args('insert', kw.keys(), (
            'values', 'filter_values', 'map_values', 'return_id',
            'debug_print','debug_assert'
        ))
        values = kw.pop('values', {})
        return_id = kw.pop('return_id', False)
        if not values:
            q = "insert into %s default values returning *" % table
        else:
            if kw.pop('filter_values', False):
                columns = self.get_table_infos(table).columns
                values = {c: v for c, v in values.items() if c in columns}
            map_values = kw.pop('map_values', {})
            values =  {
                k: (map_values.get(v, v) if isinstance(v, collections.Hashable) else v)
                for k, v in values.items()
            }
            q = "insert into {table} ({fields}) values ({values}) returning *"
            q = q.format(
                table=table,
                fields=','.join(values.keys()),
                values=','.join('%s' for v in values)
            )
        self._exec_query(q, values.values(), **kw)
        returning = self.cursor.fetchone()
        if return_id:
            return returning[self.get_table_infos(table).pkey]
        else:
            return returning

    def update(self, table, **kw):
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
        _check_args('update', kw.keys(), (
            'set', 'values', 'where', 'where_or', 'filter_values',
            'map_values', 'debug_print','debug_assert'
        ))
        values = kw.pop('values', kw.pop('set', {}))
        where = kw.pop('where', {})
        where_or = kw.pop('where_or', {})
        if kw.pop('filter_values', False):
            columns = self.get_table_infos(table).columns
            values = {c: v for c, v in values.items() if c in columns}
        map_values = kw.pop('map_values', {})
        values =  {
            k: (map_values.get(v, v) if isinstance(v, collections.Hashable) else v)
            for k, v in values.items()
        }
        q = 'update {table} set ({fields}) = ({placeholders})'
        q = q.format(
            table=table,
            fields=','.join(values.keys()),
            placeholders=','.join(['%s' for _ in values])
        )
        if where:
            where_clause = _get_where_clause(where.items())
            q += " where %s" % where_clause
        if where_or:
            where_or_clause = _get_where_clause(where_or.items(), 'or')
            if where:
                q += ' and (%s)' % where_or_clause
            else:
                q += ' where %s' % where_or_clause
        q += ' returning *'
        vals = list(values.values()) + list(where.values()) + list(where_or.values())
        self._exec_query(q, vals, **kw)
        return self.cursor.fetchone()

    def upsert(self, table, **kw):
        """SQL insert/update statement helper, with a "returning *" clause.

        Mandatory positional arguments:
        cursor -- the cursor
        table -- name of the table

        Optional keyword arguments:
        set|values -- dict with values to set (either keyword works; default empty)
        filter_values -- if True, trim values so that it contains only columns found in table (default False)
        conflict_column -- if specified, will be the column against which any conflict is determined (if not, the primary key is used)
        map_values -- dict containing a mapping to be performed on 'values' (e.g. {'': None}, to convert empty strings to nulls)
        debug_print -- print query before executing it (default False)
        debug_assert -- throw assert exception (showing query), without executing it;
                        useful for web dev debugging (default False)

        """
        _check_args('upsert', kw.keys(), (
            'set', 'values', 'filter_values', 'map_values', 'return_id', 'debug_print', 'debug_assert'
        ))
        return self._upsert_impl(table, **kw)

    def _fake_upsert(self, table, **kw):
        values = kw.pop('values', kw.pop('set', {}))
        kw['values'] = values
        pkey = self.get_table_infos(table).pkey
        if pkey in values and select1(cursor, table, where={pkey: values[pkey]}):
            kw['values'] = values
            kw['where'] = {pkey: values[pkey]}
            return self.update(table, **kw)
        else:
            return self.insert(table, **kw)

    def _real_upsert(self, table, **kw):
        values = kw.pop('values', kw.pop('set', {}))
        return_id = kw.pop('return_id', False)
        if not values:
            q = "insert into %s default values returning *" % table
        else:
            pkey = self.get_table_infos(table).pkey
            if kw.pop('filter_values', False):
                columns = self.get_table_infos(table).columns
                values = {c: v for c, v in values.items() if c in columns}
                # remove null primary key value
                if pkey in values and not values[pkey]:
                    del values[pkey]
            map_values = kw.pop('map_values', {})
            H = collections.Hashable
            values = {
                k: map_values.get(v, v) if isinstance(v, H) else v
                for k, v in values.items()
            }
            fields = ','.join(values.keys())
            vals = ','.join(['%s' for _ in values])
            updates = ','.join(['%s = excluded.%s' % (c, c) for c in values.keys()])
            q = """
              insert into {table} ({fields}) values ({vals})
              on conflict ({pkey}) do update set {updates} returning *
            """
            q = q.format(
                table=table,
                fields=fields,
                vals=vals,
                pkey=pkey,
                updates=updates
            )
        self._exec_query(q, values.values(), **kw)
        returning = self.cursor.fetchone()
        if return_id:
            return returning[self.get_table_infos(table).pkey]
        else:
            return returning

    def delete(self, table, **kw):
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
        _check_args('delete', kw.keys(), (
            'where','where_or','tighten_sequence','debug_print','debug_assert'
        ))
        where = kw.pop('where', {})
        where_or = kw.pop('where_or', {})
        q = "delete from %s" % table
        if where:
            where_clause = _get_where_clause(where.items())
            q += " where %s" % where_clause
        if where_or:
            where_or_clause = _get_where_clause(where_or.items(), 'or')
            if where:
                q += ' and (%s)' % where_or_clause
            else:
                q += ' where %s' % where_or_clause
        ts_vals = []
        if kw.pop('tighten_sequence', False):
            pkey = self.get_table_infos(table).pkey
            q += "; select setval(%%s, coalesce((select max(%s) + 1 from %s), 1), false)" % (pkey, table)
            ts_vals = [self.get_pkey_sequence(table)]
        self._exec_query(q, list(where.values()) + list(where_or.values()) + ts_vals, **kw)

    def count(self, table, **kw):
        """SQL select count statement helper.

        Mandatory positional arguments:
        cursor -- the cursor
        table -- name of the table

        Optional keyword arguments:
        [inner_]join -- either: table `str` or (table, field) `tuple`, (or list of those)
        left_join -- similar to join
        right_join -- similar to join
        left_join -- .. (default empty)
        where -- AND-joined where clause dict (default empty)
        where_or -- OR-joined where clause dict (default empty)
        order_by -- order by clause (str or list, default None)
        debug_print -- print query before executing it (default False)
        debug_assert -- throw assert exception (showing query), without executing it;
                        useful for web dev debugging (default False)

        """
        _check_args('count', kw.keys(), (
            'what', 'join', 'inner_join', 'left_join', 'right_join', 'where',
            'where_or', 'group_by', 'debug_print', 'debug_assert'
        ))
        if kw.get('group_by', None) is None:
            kw.pop('what', None) # if it's there, we can remove it safely, as it won't affect the row count
            row = self.select(table, what='count(*)', rows='one', **kw)
        else:
            row = self.select(table, get_count=True, **kw)
        return int(row['count'])

    def exists(self, table, **kw):
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
        _check_args('exists', kw.keys(), (
            'what', 'where', 'where_or', 'debug_print', 'debug_assert'
        ))
        return self.select(table, limit=1, rows='one', **kw) is not None

    def get_table_infos(self, table):
        self._table_infos.setdefault(table, TableInfos(
            pkey = self.get_pkey_column(table),
            columns = self.get_columns(table)
        ))
        return self._table_infos[table]

    def get_columns(self, table):
        """Return all columns.

        Mandatory positional arguments:
        cursor -- the cursor
        table -- name of the table

        """
        self.cursor.execute('select * from %s where 1=0' % table)
        return [rec[0] for rec in self.cursor.description]

    # http://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    def get_pkey_column(self, table):
        self.cursor.execute("""
            select pg_attribute.attname as pkey_name
            from pg_index, pg_class, pg_attribute
            where
               pg_class.oid = %s::regclass and indrelid = pg_class.oid and
               pg_attribute.attrelid = pg_class.oid and
               pg_attribute.attnum = any(pg_index.indkey) and indisprimary;
        """, [table])
        return (self.cursor.fetchone() or {}).get('pkey_name')

    def _exec_query(self, query, qvalues=[], **kw):
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
        _check_args('_exec_query', kw.keys(), ('debug_print','debug_assert'))
        query = "set transform_null_equals to on; " + query
        qvalues = _flatten(qvalues)
        if kw.get('debug_print', False):
            print(self.cursor.mogrify(query if isinstance(query, str) else query.encode('utf8'), qvalues))
        if kw.get('debug_assert', False):
            assert False, self.cursor.mogrify(query if isinstance(query, str) else query.encode('utf8'), qvalues)
        self.cursor.execute(query, qvalues)

    def get_pkey_sequence(self, table):
        self.cursor.execute("""
            select pg_get_serial_sequence(%s, a.attname) seq_name
            from pg_index i, pg_class c, pg_attribute a
            where
               c.oid = %s::regclass and indrelid = c.oid and
               a.attrelid = c.oid and a.attnum = any(i.indkey) and indisprimary;
        """, [table, table])
        return (self.cursor.fetchone() or {}).get('seq_name')

    def get_current_pkey_value(self, table, **kw):
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
        _check_args('get_current_pkey_value', kw.keys(), (
            'pkey_seq_name','debug_print','debug_assert'
        ))
        pkey_seq_name = kw.pop('pkey_seq_name', '%s_%s_id_seq' % (table, table))
        self._exec_query('select currval(%s)', [pkey_seq_name], **kw)
        return self.cursor.fetchone()['currval']

    def get_next_pkey_value(self, table, **kw):
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
        _check_args('get_current_pkey_value', kw.keys(), (
            'pkey_seq_name','debug_print','debug_assert'
        ))
        pkey_seq_name = kw.pop('pkey_seq_name', '%s_%s_id_seq' % (table, table))
        self._exec_query('select nextval(%s)', [pkey_seq_name], **kw)
        return self.cursor.fetchone()['nextval']

    def get_nullable_columns(self, table, **kw):
        """Return all nullable columns.

        Mandatory positional arguments:
        cursor -- the cursor
        table -- name of the table

        Optional keyword arguments:
        debug_print -- print query before executing it (default False)
        debug_assert -- throw assert exception (showing query), without executing it;
                        useful for web dev debugging (default False)

        """
        _check_args('get_nullable_columns', kw.keys(), (
            'debug_print', 'debug_assert'
        ))
        self._exec_query("""
            select * from information_schema.columns
            where table_name = %s
        """, [table], **kw)
        nullable_columns = [
            row['column_name'] for row in self.cursor.fetchall()
            if row['is_nullable'] == 'YES'
        ]
        return nullable_columns

    def sql(self, q, qvals=None):
        self.cursor.execute(q, qvals)
