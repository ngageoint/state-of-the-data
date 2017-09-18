"""
"""
from __future__ import print_function
from __future__ import division
import json
from warnings import warn
from .index.rtree import Rect
from .base import BaseSpatialPandas

import arcpy
import numpy as np
from arcpy import da
import pandas as pd
from pandas import Series, DataFrame
from pandas.core.indexing import _NDFrameIndexer
from pandas.util.decorators import cache_readonly

OLD_PANDAS = issubclass(Series, np.ndarray)

def _convert_array_args(args):
    if len(args) == 1 and isinstance(args[0], arcpy.Geometry):
        args = ([args[0]],)
    return args


class _CoordinateIndexer(_NDFrameIndexer):
    """ Indexing by coordinate slices """
    def _getitem_tuple(self, tup):
        obj = self.obj
        xs, ys = tup
        # handle numeric values as x and/or y coordinate index
        if type(xs) is not slice:
            xs = slice(xs, xs)
        if type(ys) is not slice:
            ys = slice(ys, ys)
        # don't know how to handle step; should this raise?
        if xs.step is not None or ys.step is not None:
            warn("Ignoring step - full interval is used.")
        xmin, ymin, xmax, ymax = obj.total_bounds
        bbox = Rect(xs.start or xmin,
                   ys.start or ymin,
                   xs.stop or xmax,
                   ys.stop or ymax)
        idx = obj.intersects(bbox)
        return obj[idx]
def _is_empty(x):
    try:
        return x.is_empty
    except:
        return False
########################################################################
class GeoSeries(BaseSpatialPandas, Series):
    """"""
    _metadata = ['name', 'spatial_reference', 'sr']
    #----------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """Constructor"""
        if not OLD_PANDAS:
            args = _convert_array_args(args)
        sr = kwargs.pop('sr', None)

        super(GeoSeries, self).__init__(*args, **kwargs)
        self.sr = sr
        self._delete_index()
    def __new__(cls, *args, **kwargs):
        kwargs.pop('sr', None)
        if OLD_PANDAS:
            args = _convert_array_args(args)
            arr = Series.__new__(cls, *args, **kwargs)
        else:
            arr = Series.__new__(cls)
        if type(arr) is GeoSeries:
            return arr
        else:
            return arr.view(GeoSeries)
    #----------------------------------------------------------------------
    def append(self, *args, **kwargs):
        return self._wrapped_pandas_method('append', *args, **kwargs)
    #----------------------------------------------------------------------
    @property
    def geometry(self):
        return self
    #----------------------------------------------------------------------
    def _wrapped_pandas_method(self, mtd, *args, **kwargs):
        """Wrap a generic pandas method to ensure it returns a GeoSeries"""
        val = getattr(super(GeoSeries, self), mtd)(*args, **kwargs)
        if type(val) == Series:
            val.__class__ = GeoSeries
            val.sr = self.sr
            val._delete_index()
        return val
    #----------------------------------------------------------------------
    @classmethod
    def from_featureclass(cls, filepath):
        """
        Alternate constructor to create a GeoSeries from a feature class
        Inputs:
         filename: file path to the feature class
        Ouput:
         GeoSeries
        """
        if arcpy.Exists(filepath) == False:
            raise ValueError("Feature class: %s does not exist" % filepath)
        desc = arcpy.Describe(filepath)
        if hasattr(desc, "shapeFieldName") == False:
            raise ValueError("Input must have a geometry column")
        fields = [desc.shapeFieldName + "@"]
        geoms = [row[0] for row in da.SearchCursor(filepath, fields)]

        g = GeoSeries(geoms)
        g.sr = arcpy.Describe(filepath).spatialReference.factoryCode
        return g
    #----------------------------------------------------------------------
    @property
    def __geo_interface__(self):
        """Returns a GeoSeries as a python feature collection
        """
        #raise NotImplementedError()
        #from geopandas import GeoDataFrame
        return DataFrame({'geometry': self}).__geo_interface__()
    @property
    def _constructor(self):
        return GeoSeries
    def __getitem__(self, key):
        return self._wrapped_pandas_method('__getitem__', key)

    def sort_index(self, *args, **kwargs):
        return self._wrapped_pandas_method('sort_index', *args, **kwargs)

    def take(self, *args, **kwargs):
        return self._wrapped_pandas_method('take', *args, **kwargs)

    def select(self, *args, **kwargs):
        return self._wrapped_pandas_method('select', *args, **kwargs)

    @property
    def _can_hold_na(self):
        return False

    def __finalize__(self, other, method=None, **kwargs):
        """ propagate metadata from other to self """
        # NOTE: backported from pandas master (upcoming v0.13)
        for name in self._metadata:
            object.__setattr__(self, name, getattr(other, name, None))
        return self

    def copy(self, order='C'):
        """
        Make a copy of this GeoSeries object

        Parameters
        ----------
        deep : boolean, default True
            Make a deep copy, i.e. also copy data

        Returns
        -------
        copy : GeoSeries
        """
        return GeoSeries(self.values.copy(order), index=self.index,
                      name=self.name).__finalize__(self)

    def isnull(self):
        """Null values in a GeoSeries are represented by empty geometric objects"""
        non_geo_null = super(GeoSeries, self).isnull()
        val = self.apply(_is_empty)
        return np.logical_or(non_geo_null, val)

    def fillna(self, value=None, method=None, inplace=False,
               **kwargs):
        """Fill NA/NaN values with a geometry (empty polygon by default).

        "method" is currently not implemented for pandas <= 0.12.
        """
        if value is None:
            value = arcpy.Point(0,0)
        return super(GeoSeries, self).fillna(value=value, method=method,
                                             inplace=inplace, **kwargs)

    def align(self, other, join='outer', level=None, copy=True,
              fill_value=None, **kwargs):
        if fill_value is None:
            fill_value = arcpy.Point(0,0)
        left, right = super(GeoSeries, self).align(other, join=join,
                                                   level=level, copy=copy,
                                                   fill_value=fill_value,
                                                   **kwargs)
        if isinstance(other, GeoSeries):
            return GeoSeries(left), GeoSeries(right)
        else: # It is probably a Series, let's keep it that way
            return GeoSeries(left), right


    def __contains__(self, other):
        """Allow tests of the form "geom in s"

        Tests whether a GeoSeries contains a geometry.

        Note: This is not the same as the geometric method "contains".
        """
        if isinstance(other, arcpy.Geometry):
            return np.any(self.equals(other))
        else:
            return False

    def plot(self, *args, **kwargs):
        raise NotImplementedError()#return plot_series(self, *args, **kwargs)

    #plot.__doc__ = plot_series.__doc__

    def to_json(self, **kwargs):
        """
        Returns a Esri JSON string representation of the series

        Parameters
        ----------
        *kwargs* that will be passed to json.dumps().
        """
        return json.dumps(self.__geo_interface__, **kwargs)
GeoSeries._create_indexer('cx', _CoordinateIndexer)
