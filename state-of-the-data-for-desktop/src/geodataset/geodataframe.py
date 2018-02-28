"""
SpatialDataFrame Object
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from six import string_types, integer_types
import pandas as pd
from pandas import DataFrame, Series, Index
import numpy
from .base import BaseSpatialPandas
from .geoseries import GeoSeries
from six import PY2, PY3
from six import string_types
import arcpy
from arcpy import Geometry
GEO_COLUMN_DEFAULT = "SHAPE"
GEOM_TYPES = (arcpy.Point, arcpy.Polygon,
              arcpy.Geometry, arcpy.PointGeometry,
              arcpy.Polyline, arcpy.Multipatch,
              arcpy.Multipoint)


class SpatialDataFrame(BaseSpatialPandas, DataFrame):
    """
    Pandas dataframe extended to accept geo-spatial data.
    """
    _internal_names = ['_data', '_cacher', '_item_cache', '_cache',
                       'is_copy', '_subtyp', '_index',
                       '_default_kind', '_default_fill_value', '_metadata',
                       '__array_struct__', '__array_interface__']
    _metadata = ['sr', '_geometry_column_name']
    _geometry_column_name = GEO_COLUMN_DEFAULT
    #----------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        sr = kwargs.pop('sr', None)
        geometry = kwargs.pop('geometry', None)
        super(SpatialDataFrame, self).__init__(*args, **kwargs)

        if isinstance(sr, integer_types):
            self.sr = arcpy.SpatialReference(sr)
        elif isinstance(sr, string_types):
            self.sr = arcpy.SpatialReference(text=sr)
        else:
            self.sr = sr
        if geometry is not None:
            # Handles case when a user passes arcpy.Point objects instead of
            # arcpy.PointGeometry.
            if isinstance(geometry, (list, tuple)) and \
               len(geometry) > 0:
                g = geometry[0]
                if isinstance(g, arcpy.Point):
                    gtrans = []
                    for g in geometry:
                        if isinstance(g, arcpy.Point):
                            g = arcpy.PointGeometry(g)
                        gtrans.append(g)
                    geometry = gtrans
            self.set_geometry(geometry, inplace=True)
        self._delete_index()
    #----------------------------------------------------------------------
    @property
    def _constructor(self):
        """constructor for class as per Pandas' github page"""
        return SpatialDataFrame
    #----------------------------------------------------------------------
    def __geo_interface__(self):
        """returns the object as an Feature Collection JSON string"""
        template = {
            "type": "FeatureCollection",
            "features": []
        }
        geom_type = self.geometry_type
        if geom_type.lower() == "point":
            geom_type = "Point"
        elif geom_type.lower() == "polyline":
            geom_type = "LineString"
        elif geom_type.lower() == "polygon":
            geom_type = "Polygon"
        df_copy = self.copy(deep=True)
        df_copy['geom_json'] = self.geometry.JSON
        df_copy['SHAPE'] = df_copy['geom_json']
        del df_copy['geom_json']
        for index, row in df_copy.iterrows():
            geom = row['SHAPE']
            del row['SHAPE']
            template['features'].append(
                {"type" : geom_type,
                "geometry" : pd.json.loads(geom),
                "attributes":row}
            )
        return pd.json.dumps(template)
    @property
    def geoextent(self):
        """returns the extent of the spatial dataframe"""
        return self.series_extent
    #----------------------------------------------------------------------
    def __getstate__(self):
        meta = {k: getattr(self, k, None) for k in self._metadata}
        return dict(_data=self._data, _typ=self._typ,
                    _metadata=self._metadata, **meta)
    #----------------------------------------------------------------------
    def __setattr__(self, attr, val):
        if attr.lower() in ['geometry', 'shape', 'shape@']:
            object.__setattr__(self, attr, val)
        else:
            super(SpatialDataFrame, self).__setattr__(attr, val)
    #----------------------------------------------------------------------
    def _get_geometry(self):
        """returns the geometry series"""
        if self._geometry_column_name not in self.columns:
            raise AttributeError("Geometry Column Not Present: %s" % self._geometry_column_name)
        return self[self._geometry_column_name]
    #----------------------------------------------------------------------
    def _set_geometry(self, col):
        """sets the geometry for the panda's dataframe"""
        if isinstance(col, (GeoSeries, list, numpy.array, numpy.ndarray, Series)):
            self.set_geometry(col, inplace=True)
        else:
            raise ValueError("Must be a list, np.array, or GeoSeries")
    #----------------------------------------------------------------------
    geometry = property(fget=_get_geometry,
                        fset=_set_geometry,
                        fdel=None,
                        doc="Get/Set the geometry data for SpatialDataFrame")
    #----------------------------------------------------------------------
    def __finalize__(self, other, method=None, **kwargs):
        """propagate metadata from other to self """
        # merge operation: using metadata of the left object
        if method == 'merge':
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other.left, name, None))
        # concat operation: using metadata of the first object
        elif method == 'concat':
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other.objs[0], name, None))
        else:
            for name in self._metadata:
                object.__setattr__(self, name, getattr(other, name, None))
        return self
    #----------------------------------------------------------------------
    def copy(self, deep=True):
        """
        Make a copy of this SpatialDataFrame object
        Parameters:

        :deep: boolean, default True
               Make a deep copy, i.e. also copy data
        Returns:
         :copy: of SpatialDataFrame
        """
        data = self._data
        if deep:
            data = data.copy()
        return SpatialDataFrame(data).__finalize__(self)
    #----------------------------------------------------------------------
    def plot(self, *args, **kwargs):
        """ writes the spatial dataframe to a map """
        raise NotImplementedError("plot is not implmented")
    #----------------------------------------------------------------------
    @staticmethod
    def from_featureclass(filename, **kwargs):
        """
        Returns a SpatialDataFrame from a feature class.
        Inputs:
         filename: full path to the feature class
        Optional Parameters:
         sql_clause: sql clause to parse data down
         where_clause: where statement
         sr: spatial reference object

        """
        from .io import from_featureclass
        return from_featureclass(filename=filename, **kwargs)
    #----------------------------------------------------------------------
    def to_featureclass(self,
                        out_location, out_name,
                        overwrite=True, skip_invalid=True):
        """converts a SpatialDataFrame to a feature class

        Parameters:
         :out_location: save location workspace
         :out_name: name of the feature class to save as
         :overwrite: boolean. True means to erase and replace value, false
          means to append
         :skip_invalids: if True, any bad rows will be ignored.
        Output:
         tuple of feature class path and list of bad rows by index number.
        """
        from .io import to_featureclass
        return to_featureclass(df=self,
                                out_location=out_location,
                                out_name=out_name,
                                overwrite=overwrite, skip_invalid=skip_invalid)
    #----------------------------------------------------------------------
    def set_geometry(self, col, drop=False, inplace=False, sr=None):
        """
        Set the SpatialDataFrame geometry using either an existing column or
        the specified input. By default yields a new object.

        The original geometry column is replaced with the input.

        Parameters:
        ----------
        keys: column label or array
        drop: boolean, default True
         Delete column to be used as the new geometry
        inplace: boolean, default False
         Modify the SpatialDataFrame in place (do not create a new object)
        sr : str/result of fion.get_sr (optional)
         Coordinate system to use. If passed, overrides both DataFrame and
         col's sr. Otherwise, tries to get sr from passed col values or
         DataFrame.
        Returns:
        -------
        SpatialDataFrame
        """
        # Most of the code here is taken from DataFrame.set_index()
        if inplace:
            frame = self
        else:
            frame = self.copy()

        if not sr:
            sr = getattr(col, 'sr', self.sr)

        to_remove = None
        geo_column_name = self._geometry_column_name
        if isinstance(col, (GeoSeries, Series, list, numpy.ndarray)):
            level = col
        elif hasattr(col, 'ndim') and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        else:
            try:
                level = frame[col].values
            except KeyError:
                raise ValueError("Unknown column %s" % col)
            except:
                raise
            if drop:
                to_remove = col
                geo_column_name = self._geometry_column_name
            else:
                geo_column_name = col

        if to_remove:
            del frame[to_remove]

        if isinstance(level, GeoSeries) and level.sr != sr:
            # Avoids caching issues/sr sharing issues
            level = level.copy()
            level.sr = sr

        # Check that we are using a listlike of geometries
        if not all(isinstance(item, GEOM_TYPES) or not item for item in level):
            raise TypeError("Input geometry column must contain valid geometry objects.")
        frame[geo_column_name] = level
        frame._geometry_column_name = geo_column_name
        frame.sr = sr
        frame._delete_index()
        if not inplace:
            return frame
        self = frame
    #----------------------------------------------------------------------
    def __getitem__(self, key):
        """
        If the result is a column containing only 'geometry', return a
        GeoSeries. If it's a DataFrame with a 'geometry' column, return a
        SpatialDataFrame.
        """
        result = super(SpatialDataFrame, self).__getitem__(key)
        geo_col = self._geometry_column_name
        if isinstance(key, string_types) and key == geo_col:
            result.__class__ = GeoSeries
            result.sr = self.sr
            result._delete_index()
        elif isinstance(result, DataFrame) and geo_col in result:
            result.__class__ = SpatialDataFrame
            result.sr = self.sr
            result._geometry_column_name = geo_col
            result._delete_index()
        elif isinstance(result, DataFrame) and geo_col not in result:
            result.__class__ = DataFrame
        return result
    #----------------------------------------------------------------------
    def reproject(self, spatial_reference, transformation=None, inplace=False):
        """
        Reprojects a given dataframe into a new coordinate system.

        """
        if isinstance(spatial_reference, arcpy.SpatialReference):
            sr = spatial_reference
        elif isinstance(spatial_reference, int):
            sr = arcpy.SpatialReference(spatial_reference)
        elif isinstance(spatial_reference, string_types):
            sr = arcpy.SpatialReference(text=spatial_reference)
        else:
            raise ValueError("spatial_referernce must be of type: int, string or arcpy.SpatialReference")

        if inplace:
            df = self
        else:
            df = self.copy()
        geom = df.geometry.projectAs(sr, transformation)
        geom.sr = sr.factoryCode
        df.geometry = geom
        if inplace:
            return df
    #----------------------------------------------------------------------
    def select_by_location(self, other, matches_only=True):
        """
        Selects all rows in a given SpatialDataFrame based on a given geometry

        Inputs:
         other: arcpy.Geometry object
         matches_only: boolean value, if true, only matched records will be
          returned, else a field called 'select_by_location' will be added
          to the dataframe with the results of the select by location.
        """
        if isinstance(other, Geometry):
            if self.geometry_type.lower() == 'point':
                res = self.within(other)
            else:
                res = self.overlaps(other)
            if matches_only:
                return self[res]
            else:
                self['select_by_location'] = res
        else:
            raise ValueError("Input must be a geometry")
    #----------------------------------------------------------------------
    def merge_datasets(self, other):
        """
        This operation combines two dataframes into one new DataFrame.
        If the operation is combining two SpatialDataFrames, the
        geometry_type must match.
        """
        if isinstance(other, SpatialDataFrame) and \
            other.geometry_type == self.geometry_type:
            return pd.concat(objs=[self, other], axis=0)
        elif isinstance(other, DataFrame):
            return pd.concat(objs=[self, other], axis=0)
        elif isinstance(other, Series):
            self['merged_datasets'] = other
        elif isinstance(other, SpatialDataFrame) and \
             other.geometry_type != self.geometry_type:
            raise ValueError("Spatial DataFrames must have the same geometry type.")
        else:
            raise ValueError("Merge datasets cannot merge types %s" % type(other))
    #----------------------------------------------------------------------
    def erase(self, other, inplace=False):
        """
        Erases
        """
        if inplace:
            df = self
        else:
            df = self.copy()
        if isinstance(other, Geometry):
            df.geometry = self.geometry.symmetricDifference(other)
            return df
        else:
            raise ValueError("Input must be of type arcpy.Geometry, not %s" % type(other))
    #----------------------------------------------------------------------
    def convex_hull(self):
        """
        Creates a Convex Hull for all Geometries.
        Output:
          SpatialDataFrame
        """
        geom = arcpy.MinimumBoundingGeometry_management(in_features=self.geometry.tolist(),
                                                out_feature_class=Geometry(),
                                                geometry_type="CONVEX_HULL")[0]
        gs = GeoSeries([geom])
        gs.sr = geom.spatialReference.factoryCode or None
        return SpatialDataFrame(geometry=gs, data=[])
###########################################################################
def _dataframe_set_geometry(self, col, drop=False, inplace=False, sr=None):
    if inplace:
        raise ValueError("Can't do inplace setting when converting from"
                         " DataFrame to SpatialDataFrame")
    gf = SpatialDataFrame(self)
    # this will copy so that BlockManager gets copied
    return gf.set_geometry(col, drop=drop, inplace=False, sr=sr)

if PY3:
    DataFrame.set_geometry = _dataframe_set_geometry
else:
    import types
    DataFrame.set_geometry = types.MethodType(_dataframe_set_geometry, None,
                                              DataFrame)