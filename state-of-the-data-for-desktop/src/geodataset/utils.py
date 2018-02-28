"""
    Common set of utilities to assist with operations.
"""
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
import re
from .index.rtree import Rtree
from datetime import datetime
import pandas as pd
import numpy as np
from six import string_types, integer_types

DATETIME_TYPES = (datetime,
                  np.datetime64,
                  pd.datetime,
                  pd.DatetimeIndex)

NUMERIC_TYPES = tuple(list(integer_types) + [
    np.int, np.int16,
    np.int32, np.integer,
    np.float, np.float32,
    np.float64, np.int8,
    np.int64, np.short])

STRING_TYPES = tuple(list(string_types) + \
    [str, np.str, np.unicode, chr])

#--------------------------------------------------------------------------
def sanitize_field_name(s, length=None, sub_value=None):
    """
    Modifies the string by replacing special characters by another value.
    It can also shorten a string if length is specified.

    Parmaters:
     :s: string value
     :length: optional integer value must be > 0
     :sub_value: optional replacement value instead of empty string

    Returns:
     string

    Usage:
    >>> sv = 'how much for the doggie in the window? $20.99?'
    >>> print (sanitize_field_name(sv))
    howmuchforthedoggieinthewindow2099
    """
    if sub_value is None:
        sub_value = ""
    s = re.sub('\W+', sub_value, s)
    if isinstance(length, integer_types) and \
       length > 0 and \
       len(s) > length:
        s = s[:length]
    return s
#--------------------------------------------------------------------------
def chunks(l, n):
    """yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]
#--------------------------------------------------------------------------
def sjoin(left_df, right_df, how='inner', op='intersects',
          lsuffix='left', rsuffix='right'):
    """Spatial join of two GeoDataFrames.

    Parameters
    ----------
    left_df, right_df : GeoDataFrames
    how : string, default 'inner'
        The type of join:

        * 'left': use keys from left_df; retain only left_df geometry column
        * 'right': use keys from right_df; retain only right_df geometry column
        * 'inner': use intersection of keys from both dfs; retain only
          left_df geometry column
    op : string, default 'intersection'
        Binary predicate, one of {'intersects', 'contains', 'within'}.
        See http://toblerity.org/shapely/manual.html#binary-predicates.
    lsuffix : string, default 'left'
        Suffix to apply to overlapping column names (left GeoDataFrame).
    rsuffix : string, default 'right'
        Suffix to apply to overlapping column names (right GeoDataFrame).

    """
    from .index import rtree

    allowed_hows = ['left', 'right', 'inner']
    if how not in allowed_hows:
        raise ValueError("`how` was \"%s\" but is expected to be in %s" % \
                         (how, allowed_hows))

    allowed_ops = ['contains', 'within', 'intersects']
    if op not in allowed_ops:
        raise ValueError("`op` was \"%s\" but is expected to be in %s" % \
                         (op, allowed_ops))

    if op == "within":
        # within implemented as the inverse of contains; swap names
        left_df, right_df = right_df, left_df

    if left_df.sr != right_df.sr:
        print('Warning: CRS does not match!')

    tree_idx = Rtree()
    right_df_bounds = right_df['geometry'].apply(lambda x: x.bounds)
    for i in right_df_bounds.index:
        tree_idx.insert(i, right_df_bounds[i])

    idxmatch = (left_df['geometry'].apply(lambda x: x.bounds)
                .apply(lambda x: list(tree_idx.intersection(x))))
    idxmatch = idxmatch[idxmatch.apply(len) > 0]

    if idxmatch.shape[0] > 0:
        # if output from  join has overlapping geometries
        r_idx = np.concatenate(idxmatch.values)
        l_idx = np.concatenate([[i] * len(v) for i, v in idxmatch.iteritems()])

        # Vectorize predicate operations
        def find_intersects(a1, a2):
            return a1.intersects(a2)

        def find_contains(a1, a2):
            return a1.contains(a2)

        predicate_d = {'intersects': find_intersects,
                       'contains': find_contains,
                       'within': find_contains}

        check_predicates = np.vectorize(predicate_d[op])

        result = (
            pd.DataFrame(
                np.column_stack(
                    [l_idx,
                     r_idx,
                     check_predicates(
                         left_df['geometry']
                         .apply(lambda x: prepared.prep(x))[l_idx],
                         right_df['geometry'][r_idx])
                     ]))
        )

        result.columns = ['index_%s' % lsuffix, 'index_%s' % rsuffix, 'match_bool']
        result = (
            pd.DataFrame(result[result['match_bool']==1])
            .drop('match_bool', axis=1)
        )

    else:
        # when output from the join has no overlapping geometries
        result = pd.DataFrame(columns=['index_%s' % lsuffix, 'index_%s' % rsuffix])

    if op == "within":
        # within implemented as the inverse of contains; swap names
        left_df, right_df = right_df, left_df
        result = result.rename(columns={
            'index_%s' % (lsuffix): 'index_%s' % (rsuffix),
            'index_%s' % (rsuffix): 'index_%s' % (lsuffix)})

    if how == 'inner':
        result = result.set_index('index_%s' % lsuffix)
        return (
            left_df
            .merge(result, left_index=True, right_index=True)
            .merge(right_df.drop('geometry', axis=1),
                   left_on='index_%s' % rsuffix, right_index=True,
                   suffixes=('_%s' % lsuffix, '_%s' % rsuffix))
        )
    elif how == 'left':
        result = result.set_index('index_%s' % lsuffix)
        return (
            left_df
            .merge(result, left_index=True, right_index=True, how='left')
            .merge(right_df.drop('geometry', axis=1),
                   how='left', left_on='index_%s' % rsuffix, right_index=True,
                   suffixes=('_%s' % lsuffix, '_%s' % rsuffix))
        )
    elif how == 'right':
        return (
            left_df
            .drop('geometry', axis=1)
            .merge(result.merge(right_df,
                                left_on='index_%s' % rsuffix, right_index=True,
                                how='right'), left_index=True,
                   right_on='index_%s' % lsuffix, how='right')
            .set_index('index_%s' % rsuffix)
        )