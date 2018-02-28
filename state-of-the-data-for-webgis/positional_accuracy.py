"""-----------------------------------------------------------------------------
Name: positional_accuracy.py
Purpose: Statistically summarizes positional accuracy values that are stored
        in a field within a feature class.
Description: This tool statistically summarizes the positional accuracy values
        that are help in a user defined attribute field in the selected
        feature class.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.5+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 19, 2015
Modified: August 24, 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""
from __future__ import division
from __future__ import print_function
import os
import sys
import numpy as np
import pandas as pd
from collections import Counter

#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import sotd_config as config

FIELDS = ('MEAN', 'MEDIAN',
              'MODE', 'MIN_',
              'MAX_', 'NO_DATE_CNT',
              'NO_DATE_PCT', 'FEATURE_CNT',
              'PA_SCORE', "TIER")


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
#--------------------------------------------------------------------------
def get_score(mean):
    value = 0
    if mean > 0:
        if mean >= 0 and mean < 15:
            value = 5
        elif mean >= 15 and mean <= 25:
            value = 4
        elif mean > 25 and mean <= 50:
            value = 3
        elif mean > 50 and mean <= 100:
            value = 2
        else:
            value = 1
    elif mean == -1:
        # no samples
        value = 0

    return value
#--------------------------------------------------------------------------
def get_tier(score):
    """
    """
    cat = 'Tin'
    if score == 5: # ranges
        cat = "Platinum"
    elif score == 4:
        cat = "Gold"
    elif score == 3:
        cat = 'Silver'
    elif score == 2:
        cat = "Bronze"
    elif score == 1:
        cat = "Tin"
    else:
        cat = "No Ranking"
    return cat
#--------------------------------------------------------------------------
##                ('_ID', np.int),
##                ('MEAN', np.float64),
##                ('MEDIAN', np.float64),
##                ('MODE', np.float64),
##                ('MIN', np.float64),
##                ('MAX', np.float64),
##                ('NO_DATE_CNT', np.int32),
##                ('NO_DATE_PCT', np.float64),
##                ('FEATURE_CNT', np.int32),
##                ('PA_SCORE', np.int32),
##                ("TIER", '|S1024')
#--------------------------------------------------------------------------
def positional_accuracy(gis, df_current, output_features, grid_filter, geom, value_field):
    """ main driver of program """
    try:
        PDVERSION = [int(v) for v in pd.__version__.split('.')]

        out_fl = FeatureLayer(gis=gis, url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
            return_all_records=True).df

        sq = df_current['SHAPE'].disjoint(geom) == False
        df_current = df_current[sq].copy()
        if len(df_current) > 0:
            df_notnull = df_current.loc[df_current[value_field].notnull() == True]
            if PDVERSION[1] <= 16:
                df_notnull = df_notnull.drop(value_field, axis=1).join(df_notnull[value_field].astype(float,raise_on_error=False)).copy()
            elif PDVERSION[1] > 16:
                df_notnull = df_notnull.drop(value_field, axis=1).join(df_notnull[value_field].apply(pd.to_numeric, errors='coerce')).copy()  # CHANGES NON NUMERIC ROWS to NaN
            df_notnull = df_notnull.loc[df_notnull[value_field].notnull() == True].copy() # Drops NaN values
            not_null_count = len(df_notnull)
            null_count = len(df_current) - not_null_count
            if PDVERSION[1] == 16:
                try:
                    s = df_notnull.loc[df_notnull[value_field] != 'No Information', value_field].copy().astype(np.float64)
                except:
                    s = df_notnull.loc[df_notnull[value_field].astype(str) != 'No Information', value_field].copy().astype(np.float64)
            elif PDVERSION[1] > 16:
                s = df_notnull.drop(value_field, axis=1).join(df_notnull[value_field].apply(pd.to_numeric, errors='coerce'))[value_field].copy() # Drops Text Fields
            s = s[s.notnull() == True].copy() # Drops NaN values
            mean = s.mean()
            median = s.median()
            mode = s.mode()
            if len(mode) > 0:
                mode = mode[0]
            else:
                mode = 0
            mmax = s.max()
            mmin = s.min()
            score = get_score(mean)
            null_percent = float(null_count) * 100.0 / float(len(df_current))

            if not pd.isnull(mean):
                out_sdf[FIELDS[0]][0]=round(mean,1)
            else:
                out_sdf[FIELDS[0]][0]=-1
            if not pd.isnull(median):
                out_sdf[FIELDS[1]][0]=median
            else:
                out_sdf[FIELDS[1]][0]=-1

            if not pd.isnull(mode):
                out_sdf[FIELDS[2]][0]=mode
            else:
                out_sdf[FIELDS[2]][0]=-1

            if not pd.isnull(mmin):
                out_sdf[FIELDS[3]][0]=mmin
            else:
                out_sdf[FIELDS[3]][0]=-1

            if not pd.isnull(mmax):
                out_sdf[FIELDS[4]][0]=mmax
            else:
                out_sdf[FIELDS[4]][0]=-1

            out_sdf[FIELDS[5]][0]=null_count
            out_sdf[FIELDS[6]][0]=round(null_percent,1)
            out_sdf[FIELDS[7]][0]=len(df_current)#not_null_count
            out_sdf[FIELDS[8]][0]=score
            out_sdf[FIELDS[9]][0]=get_tier(score)

            del df_notnull
            del mean
            del median
            del mode
            del mmax
            del mmin
            del score
            del null_percent
        else:
            out_sdf[FIELDS[0]][0]=-1
            out_sdf[FIELDS[1]][0]=-1
            out_sdf[FIELDS[2]][0]=-1
            out_sdf[FIELDS[3]][0]=-1
            out_sdf[FIELDS[4]][0]=-1
            out_sdf[FIELDS[5]][0]=0
            out_sdf[FIELDS[6]][0]=0
            out_sdf[FIELDS[7]][0]=0
            out_sdf[FIELDS[8]][0]=0
            out_sdf[FIELDS[9]][0]="No Ranking"
            #r = tuple([oid] + [-1]*5 + [0] * 4 + ["No Ranking"])

        return out_sdf, out_fl


    except FunctionError as f_e:
        messages = f_e.args[0]
        #arcpy.AddError("error in function: %s" % messages["function"])
        #arcpy.AddError("error on line: %s" % messages["line"])
        #arcpy.AddError("error in file name: %s" % messages["filename"])
        #arcpy.AddError("with error message: %s" % messages["synerror"])
        #arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        #arcpy.AddError("error on line: %s" % line)
        #arcpy.AddError("error in file name: %s" % filename)
        #arcpy.AddError("with error message: %s" % synerror)
