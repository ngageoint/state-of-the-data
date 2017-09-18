"""
Reads shapefiles, feature classes, table into a spatial dataframe
"""
from __future__ import print_function
from __future__ import division
import os
import six
import copy
import logging
import tempfile
from warnings import warn
import numpy as np
import pandas as pd
from six import iteritems, integer_types
from datetime import datetime
from ..utils import NUMERIC_TYPES, STRING_TYPES, DATETIME_TYPES
from ..utils import sanitize_field_name
try:
    import arcpy
    from arcpy import da
    HASARCPY = True
except:
    warn(message="ArcPy not found.")
    HASARCPY = False

#--------------------------------------------------------------------------
def from_featureclass(filename, **kwargs):
    """
    Returns a GeoDataFrame from a feature class.
    Inputs:
     filename: full path to the feature class
    Optional Parameters:
     sql_clause: sql clause to parse data down
     where_clause: where statement
     sr: spatial reference object
     fields: list of fields to extract from the table
    """
    if HASARCPY:
        from .. import SpatialDataFrame
        afn = None
        lfn = None
        desc = arcpy.Describe(filename)
        if hasattr(desc, 'areaFieldName'):
            afn = desc.areaFieldName
        if hasattr(desc, 'lengthFieldName'):
            lfn = desc.lengthFieldName
        sql_clause = kwargs.pop('sql_clause', (None,None))
        where_clause = kwargs.pop('where_clause', None)
        sr = kwargs.pop('sr', None)
        fields = kwargs.pop('fields', None)
        if not fields:
            fields = [field.name for field in arcpy.ListFields(filename) \
                      if field.type not in ['Geometry']]
        geom_fields = list(fields) + ['SHAPE@']
        flds = list(fields) + ['SHAPE']
        for f in [afn, lfn, "SHAPE.AREA", "SHAPE.LEN"]:
            if not f is None:
                if f.lower() in flds:
                    flds.remove(f.lower())
                elif f in flds:
                    flds.remove(f)
                if f.lower() in geom_fields:
                    geom_fields.remove(f.lower())
                elif f in geom_fields:
                    geom_fields.remove(f)
            del f

        vals = []
        frames = []
        with arcpy.da.SearchCursor(filename,
                                   field_names=geom_fields,
                                   where_clause=where_clause,
                                   sql_clause=sql_clause,
                                   spatial_reference=sr) as rows:
            sdf = SpatialDataFrame(columns=flds)
            for row in rows:
                vals.append(dict(zip(flds, row)))
                if len(vals) == 25000:
                    frames.append(SpatialDataFrame.from_dict(data=vals))
                    vals = []
                del row
            del rows
        if len(vals) > 0:
            frames.append(SpatialDataFrame.from_dict(data=vals))
        sdf = pd.concat(frames)
        sdf.reset_index(drop=True, inplace=True)
        del frames
        if sr is None:
            sdf.sr = sr
        else:
            sdf.sr = sdf.geometry[0].spatialReference
        return sdf
    return

#--------------------------------------------------------------------------
def to_featureclass(df, out_name, out_location=None,
                    overwrite=True, out_sr=None,
                    skip_invalid=True):
    """
    converts a SpatialDataFrame to a feature class

    Parameters:
     :out_location: path to the workspace
     :out_name: name of the output feature class table
     :overwrite: True, the data will be erased then replaced, else the
      table will be appended to an existing table.
     :out_sr: if set, the data will try to reproject itself
     :skip_invalid: if True, the cursor object will not raise an error on
      insertion of invalid data, if False, the first occurence of invalid
      data will raise an exception.
    Returns:
     path to the feature class
    """
    cols = []
    dt_idx = []
    invalid_rows = []
    idx = 0
    max_length = None
    if out_location:
        if os.path.isdir(out_location) == False and \
           out_location.lower().endswith('.gdb'):
            out_location = arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(out_location),
                                                         out_name=os.path.basename(out_location))[0]
        elif os.path.isdir(out_location) == False and \
             out_name.lower().endswith('.shp'):
            os.makedirs(out_location)
        elif os.path.isfile(out_location) == False and \
             out_location.lower().endswith('.sde'):
            raise ValueError("The sde connection file does not exist")
    else:
        if out_name.lower().endswith('.shp'):
            out_location = tempfile.gettempdir()
        elif HASARCPY:
            out_location = arcpy.env.scratchGDB
        else:
            out_location = tempfile.gettempdir()
            out_name = out_name + ".shp"
    fc = os.path.join(out_location, out_name)
    df = df.copy() # create a copy so we don't modify the source data.
    if out_name.lower().endswith('.shp'):
        max_length = 10
    for col in df.columns:
        if df[col].dtype.type in NUMERIC_TYPES:
            df[col] = df[col].fillna(0)
        elif df[col].dtype.type in DATETIME_TYPES:
            dt_idx.append(idx)
        else:
            df.loc[df[col].isnull(), col] = ""
        idx += 1
        col = sanitize_field_name(s=col,
                                  length=max_length)
        cols.append(col)
        del col
    df.columns = cols

    if arcpy.Exists(fc) and \
       overwrite:
        arcpy.Delete_management(fc)
    if arcpy.Exists(fc) ==  False:
        sr = df.sr
        if sr is None:
            sr = df['SHAPE'].loc[df['SHAPE'].first_valid_index()]
        fc = arcpy.CreateFeatureclass_management(out_path=out_location,
                                                 out_name=out_name,
                                                 geometry_type=df.geometry_type.upper(),
                                                 spatial_reference=sr)[0]
    oidField = arcpy.Describe(fc).oidFieldName
    col_insert = copy.copy(df.columns).tolist()
    lower_col_names = [f.lower() for f in col_insert]
    if "SHAPE" in df.columns:
        idx = col_insert.index("SHAPE")
        col_insert[idx] = "SHAPE@"
    if oidField.lower() in lower_col_names:
        val = col_insert.pop(lower_col_names.index(oidField.lower()))
        del df[val]
    existing_fields = [field.name.lower() for field in arcpy.ListFields(fc)]
    for col in col_insert:
        if col.lower().find('shape') == -1 and \
           col.lower not in existing_fields:
            arcpy.AddField_management(in_table=fc, field_name=col,
                                      field_type=_infer_type(df, col))
    icur = da.InsertCursor(fc, col_insert)
    for index, row in df.iterrows():
        if len(dt_idx) > 0:
            row = row.tolist()
            for i in dt_idx:
                row[i] = row[i].to_pydatetime()
                del i
            try:
                icur.insertRow(row)
            except:
                invalid_rows.append(index)
                if skip_invalid == False:
                    raise Exception("Invalid row detected at index: %s" % index)
        else:
            try:
                icur.insertRow(row.tolist())
            except:
                invalid_rows.append(index)
                if skip_invalid == False:
                    raise Exception("Invalid row detected at index: %s" % index)

        del row
    del icur
    if len(invalid_rows) > 0:
        t = ",".join([str(r) for r in invalid_rows])
        #import warnings
        print('The following rows could not be written to the table: %s' % t)
    return fc
#--------------------------------------------------------------------------
def _infer_type(df, col):
    """
    internal function used to get the datatypes for the feature class if
    the dataframe's _field_reference is NULL or there is a column that does
    not have a dtype assigned to it.

    Input:
     dataframe - spatialdataframe object
    Ouput:
      field type name
    """
    nn = df[col].notnull()
    nn = list(df[nn].index)
    if len(nn) > 0:
        val = df[col][nn[0]]
        if isinstance(val, six.string_types):
            return "TEXT"
        elif isinstance(val, tuple(list(six.integer_types) + [np.int32])):
            return "INTEGER"
        elif isinstance(val, (float, np.int64 )):
            return "FLOAT"
        elif isinstance(val, datetime):
            return "DATE"
    return "TEXT"

