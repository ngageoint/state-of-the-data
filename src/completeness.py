"""-----------------------------------------------------------------------------
Name: completeness.py
Purpose: Creates the completness indicator from two caomparable feature
        classes.
Description: This tool calculates the completeness score of a curve feature
        dataset by comparing your features to an alternative source of features
        (for example, Open Street Map features). Feature comparisons should be
        apples-to-apples. For example, you should compare road features to road
        features or water features to water features.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 8, 2015
Upated: Fall 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

from __future__ import division
import pandas as pd
import numpy as np
import os
import sys
from collections import Counter

import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import SpatialDataFrame
from arcgis.geometry import filters
from arcgis.geometry import Geometry

##from geodataset import SpatialDataFrame
##
##import arcpy
##from arcpy import env
##from arcpy import da
##if sys.version_info.major == 3:
##    from arcpy import mp as mapping
##else:
##    from arcpy import mapping

FIELDS = ['TDS_DENSITY',
    'COMP_DENSITY',
    'COMPLETENESS_VALUE',
    'DIFFERENCE']

#--------------------------------------------------------------------------
class FunctionError(Exception):
    """ raised when a function fails to run """
    pass
#--------------------------------------------------------------------------
def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, __file__, synerror

###--------------------------------------------------------------------------
##def extend_table(table, rows=None):
##    """
##    Adds the required columns to the table and appends new records if
##    given.
##    """
##    try:
##        if rows is None:
##            rows = []
##        dtypes = np.dtype(
##            [
##                ('_ID', np.int),
##                ('TDS_DENSITY', np.float64),
##                ('COMP_DENSITY', np.float64),
##                ('COMPLETENESS_VALUE', np.float64),
##                ('DIFFERENCE', np.float64)
##            ]
##        )
##        array = np.array(rows, dtypes)
##        da.ExtendTable(table, "OID@", array, "_ID", False)
##        return table
##    except:
##        line, filename, synerror = trace()
##        raise FunctionError(
##                {
##                "function": "extend_table",
##                "line": line,
##                "filename": filename,
##                "synerror": synerror,
##                "arc" : str(arcpy.GetMessages(2))
##                }
##        )
def get_score(ratio, baseVal, inputVal):
    if inputVal > 0:
        #ratio = baseVal/inputVal
        if (ratio >= 0 and ratio <= 0.5):
            result = 1
        elif (ratio > 0.5 and ratio <= 0.75):
            result = 2
        elif (ratio > 0.75 and ratio <= 1.25):
            result = 3
        elif (ratio > 1.25 and ratio <= 1.5):
            result = 4
        elif (ratio > 1.5):
            result = 5
        else:
            result = 0
    else:
        if baseVal > 0:
            result = 5
        else:
            result = 0

    return result
#--------------------------------------------------------------------------
def completeness(gis, df_after, df_before, output_features, grid_filter, geom):
    """ main driver of program """
    try:

        out_fl = FeatureLayer(gis=gis, url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
                return_all_records=True).df

        geometry_type = df_after.geometry_type

        sq = df_before[df_before.geometry.notnull()].geometry.disjoint(geom) == False
        df_before = df_before[sq].copy()
        before_count = len(df_before)
        sq = df_after[df_after.geometry.notnull()].geometry.disjoint(geom) == False
        df_after = df_after[sq].copy()
        after_count = len(df_after)
        geoms_after = df_after.clip(geom.extent)
        geoms_before = df_before.clip(geom.extent)

        geoms_before_sdf = SpatialDataFrame(geometry=geoms_before)
        geoms_after_sdf = SpatialDataFrame(geometry=geoms_after)

        q_after = geoms_after_sdf.geometry.JSON == '{"paths":[]}'
        geoms_after_sdf = geoms_after_sdf[~q_after].copy()
        geoms_after_sdf.reset_index(inplace=True, drop=True)
        q_before = geoms_before_sdf.geometry.JSON == '{"paths":[]}'
        geoms_before_sdf = geoms_before_sdf[~q_before].copy()
        geoms_before_sdf.reset_index(inplace=True, drop=True)

        if geometry_type == "Polygon":
            before_val = geoms_before_sdf.geometry.get_area('GEODESIC','SQUAREKILOMETERS').sum()
            after_val = geoms_after_sdf.geometry.get_area('GEODESIC','SQUAREKILOMETERS').sum()
            if after_val > 0:
                score = get_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_score(0, before_val, after_val)

            out_sdf[FIELDS[0]][0] = round(before_val,1)
            out_sdf[FIELDS[1]][0] = round(after_val,1)
            out_sdf[FIELDS[3]][0] = round(before_val - after_val,1)
            out_sdf[FIELDS[2]][0] = score

        elif geometry_type == "Polyline":
            before_val = geoms_before_sdf.geometry.get_length('GEODESIC','KILOMETERS').sum()
            after_val = geoms_after_sdf.geometry.get_length('GEODESIC','KILOMETERS').sum()

            if after_val > 0:
                score = get_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_score(0, before_val, after_val)

            out_sdf[FIELDS[0]][0] = round(before_val,1)
            out_sdf[FIELDS[1]][0] = round(after_val,1)
            out_sdf[FIELDS[3]][0] = round(before_val - after_val,1)
            out_sdf[FIELDS[2]][0] = score

        else:
            before_count = len(geoms_before_sdf)
            after_count = len(geoms_after_sdf)
            if after_count > 0:
                score = get_score(ratio=before_count/after_count,
                        baseVal=before_count,
                        inputVal=after_count)
            else:
                score = get_score(ratio=0,
                        baseVal=before_count,
                        inputVal=after_count)

            out_sdf[FIELDS[0]][0] = before_count
            out_sdf[FIELDS[1]][0] = after_count
            out_sdf[FIELDS[3]][0] = before_count - after_count
            out_sdf[FIELDS[2]][0] = score

        del sq
        del df_after
        del df_before
        del geom

        return out_sdf, out_fl

        #arcpy.SetParameterAsText(4, out_grid)
    except FunctionError as f_e:
        messages = f_e.args[0]

    except:
        line, filename, synerror = trace()

#--------------------------------------------------------------------------
##if __name__ == "__main__":
##    #env.overwriteOutput = True
##    argv = tuple(arcpy.GetParameterAsText(i)
##    for i in range(arcpy.GetArgumentCount()))
##    main(*argv)