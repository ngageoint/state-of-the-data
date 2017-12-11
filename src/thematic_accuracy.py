"""-----------------------------------------------------------------------------
Name: thematic_accuracy.py
Purpose: Creates the themtic accuracy score from the feature level metadata.
Description: This tool scores the thematic accuracy of a TDS 6.1 curve feature
        class. The tool uses global population data to infer an appropriate
        collection scale for the data, compares that scale to the
        scalability trigger, and generates a score based on that comparison.
        The score represents whether we think the scale is appropriate.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August, 2015
Modified: August 12, 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""
from __future__ import division
from __future__ import print_function
import os
import sys
from geodataset import SpatialDataFrame
import numpy as np
import pandas as pd


#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import sotd_config as config

import arcpy
from arcpy import env
from arcpy import da

FIELDS = ['DOM_SCALE',
    'DOM_COUNT',
    'DOM_PER',
    'MIN_SCALE',
    'MIN_PER',
    'MAX_SCALE',
    'MAX_PER',
    'CNT_2500',
    'CNT_5000',
    'CNT_12500',
    'CNT_25000',
    'CNT_50000',
    'CNT_100000',
    'CNT_250000',
    'CNT_500000',
    'CNT_1000000',
    'PER_2500',
    'PER_5000',
    'PER_12500',
    'PER_25000',
    'PER_50000',
    'PER_100000',
    'PER_250000',
    'PER_500000',
    'PER_1000000',
    'COUNT_',           #Add Underscore if AGOL
    'MISSION_PLANNING',
    'POPULATION_SCALE',
    'THEM_ACC_SCORE']

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
def get_equal_breaks_score(mean):
    """"""
    ratio = mean
    if (ratio >= 0 and ratio <= 0.5):
        return "G"
    elif (ratio > 0.5 and ratio <= 1.0):
        return "R"
    elif (ratio > 1.0 and ratio <= 1.5):
        return "L"
    elif (ratio > 1.5 and ratio <= 2.0):
        return "S/U"
    else:
        return 0
#--------------------------------------------------------------------------
def get_msp(scale):
    if scale >= 500000:
        msp = 'STRATEGIC'
    elif scale >= 250000:
        msp = 'OPERATIONAL'
    elif scale >= 25000:
        msp = 'TACTICAL'
    elif scale >= 5000:
        msp = 'URBAN'
    else:
        msp = 'UNDEFINED'
    return msp
#--------------------------------------------------------------------------
def extend_table(table, rows=None):
    """
    Adds the required columns to the table and appends new records if
    given.
    """
    try:
        if rows is None:
            rows = []
        dtypes = np.dtype(
            [
                ('_ID', np.int),
                ('DOM_SCALE', np.float64),
                ('DOM_COUNT', np.int32),
                ('DOM_PER', np.float64),
                ('MIN_SCALE', np.float64),
                ('MIN_PER', np.float64),
                ('MAX_SCALE', np.float64),
                ('MAX_PER', np.float64),
                ('CNT_2500', np.int32),
                ('CNT_5000', np.int32),
                ('CNT_12500', np.int32),
                ('CNT_25000', np.int32),
                ('CNT_50000', np.int32),
                ('CNT_100000', np.int32),
                ('CNT_250000', np.int32),
                ('CNT_500000', np.int32),
                ('CNT_1000000', np.int32),
                ('PER_2500', np.float64),
                ('PER_5000', np.float64),
                ('PER_12500', np.float64),
                ('PER_25000', np.float64),
                ('PER_50000', np.float64),
                ('PER_100000', np.float64),
                ('PER_250000', np.float64),
                ('PER_500000', np.float64),
                ('PER_1000000', np.float64),
                ('COUNT', np.int32),
                ('MISSION_PLANNING', '|S1024'),
                ('POPULATION_SCALE', '|S1024'),
                ('THEM_ACC_SCORE', np.float64)
            ]
        )
        array = np.array(rows, dtypes)
        da.ExtendTable(table, "OID@", array, "_ID", False)
        return table
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "extend_table",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
def create_grls(grid, population, output_features):
    """Creates a table to join to the grid dataset"""
    try:
        #output_features = os.path.join(env.scratchGDB, "temp_grid")
        reclass_population = os.path.join(env.scratchFolder, "rast_temp.tif")
        zonal_table = os.path.join(env.scratchGDB, 'zonalstats') #in_memory\\table"
        if arcpy.Exists(reclass_population):
            arcpy.Delete_management(reclass_population)
        if arcpy.Exists(zonal_table):
            arcpy.Delete_management(zonal_table)
        output_features = arcpy.CopyFeatures_management(grid, output_features)#[0]
        arcpy.AddMessage(output_features)
        arcpy.AddMessage(reclass_population)
        arcpy.AddMessage(zonal_table)


        arcpy.gp.Reclassify_sa(population, "VALUE", "0 0;1 2;2 2;3 2;4 2;5 2;6 1;7 1;8 1;9 1;10 1", reclass_population, "DATA")
        arcpy.gp.ZonalStatisticsAsTable_sa(output_features, "OBJECTID", reclass_population,zonal_table, "DATA", "ALL")
        #zonal_oid = arcpy.Describe(zonal_table).OIDFieldName
        arcpy.JoinField_management(output_features, "OBJECTID",
                                   zonal_table, "OBJECTID_1",
                                   "Count;Area;Min;Max;Range;Variety;Majority;Minority;Median;Mean;Std;Sum")
        arcpy.Delete_management(reclass_population)
        return output_features
    except:
        line, filename, synerror = trace()
        raise FunctionError(
            {
                "function": "create_grls",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
        )
def most_common(lst):
    return max(set(lst), key=lst.count), lst.count(max(set(lst), key=lst.count))

def minimun(lst):
    return min(lst), lst.count(min(lst))

def maximum(lst):
    return max(lst), lst.count(max(lst))

def population_scale(domScale, GRLS):
    if (domScale == 5000 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 50000 and GRLS == 'G'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'R'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'L'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'S/U'):
        POPULATION_SCALE = 2
    elif (domScale == 100000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale == 100000 and GRLS == 'R'):
        POPULATION_SCALE = 3
    elif (domScale == 100000 and GRLS == 'L'):
        POPULATION_SCALE = 2
    elif (domScale == 100000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    elif (domScale == 250000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale == 250000 and GRLS == 'R'):
        POPULATION_SCALE = 3
    elif (domScale == 250000 and GRLS == 'L'):
        POPULATION_SCALE = 2
    elif (domScale == 250000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    elif (domScale >= 500000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale >= 500000 and GRLS == 'R'):
        POPULATION_SCALE = 2
    elif (domScale >= 500000 and GRLS == 'L'):
        POPULATION_SCALE = 1
    elif (domScale >= 500000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    else:
        POPULATION_SCALE = 0
    return POPULATION_SCALE
#--------------------------------------------------------------------------
def thematic_accuracy(gis, df_current, output_features, grid_filter, geom, value_field):
    """ main driver of program """
    try:

        out_fl = FeatureLayer(gis=gis, url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
            return_all_records=True).df

        sq = df_current['SHAPE'].disjoint(geom) == False
        df_current = df_current[sq].copy()

        if len(df_current) > 0:
            count = len(df_current)
            max_val = df_current[value_field].max()
            max_scale = 100 * (len(df_current[df_current[value_field] == max_val])/count)
            min_val = df_current[value_field].min()
            min_scale = 100 * (len(df_current[df_current[value_field] == min_val])/count)
            vc = df_current[value_field].value_counts()
            common = df_current[value_field].mode() # Used in MSP
            if len(common) > 0:
                common = common[0]
                common_count = vc[common]
                common_per = (vc[common]/count) * 100
            else:
                common = min_val
                common_count = 1
                common_per = 100
            count_2500 = 0
            count_5000 = 0
            count_12500 = 0
            count_25000 = 0
            count_50000 = 0
            count_100000 = 0
            count_250000 = 0
            count_500000 = 0
            count_1000000 = 0
            if 2500 in vc:
                count_2500 = vc[2500]
            if 5000 in vc:
                count_5000 = vc[5000]
            if 12500 in vc:
                count_12500 = vc[12500]
            if 25000 in vc:
                count_25000 = vc[25000]
            if 50000 in vc:
                count_50000 = vc[50000]
            if 100000 in vc:
                count_100000 = vc[100000]
            if 250000 in vc:
                count_250000 = vc[250000]
            if 500000 in vc:
                count_500000 = vc[500000]
            if 1000000 in vc:
                count_1000000 = vc[1000000]

            MSP = get_msp(scale=common) # SHOULD UPDATE MISSION_PLANNING FIELD

            SCORE_VALUE = get_equal_breaks_score(mean=out_sdf['MEAN'][0])# get_equal_breaks_score(output_features, ['MEAN','EQUAL']) # PUT SCORE IN EQUAL
            GRLS = SCORE_VALUE
            domScale = common
            # FIELD 1 is the source, Field 2 is the field to be updated
            #df_current['EQUAL'] = SCORE_VALUE # ASSIGNS EQUAL TO LANSCAN_SCALE
            #29 field

            out_sdf[FIELDS[0]][0]=common# median
            out_sdf[FIELDS[1]][0]=common_count # % common
            out_sdf[FIELDS[2]][0]=round(common_per,1)
            out_sdf[FIELDS[3]][0]=min_val
            out_sdf[FIELDS[4]][0]=round(min_scale,1)
            out_sdf[FIELDS[5]][0]=max_val
            out_sdf[FIELDS[6]][0]=round(max_scale,1)
            out_sdf[FIELDS[7]][0]=count_2500
            out_sdf[FIELDS[8]][0]=count_5000
            out_sdf[FIELDS[9]][0]=count_12500
            out_sdf[FIELDS[10]][0]=count_25000
            out_sdf[FIELDS[11]][0]=count_50000
            out_sdf[FIELDS[12]][0]=count_100000
            out_sdf[FIELDS[13]][0]=count_250000
            out_sdf[FIELDS[14]][0]=count_500000
            out_sdf[FIELDS[15]][0]=count_1000000
            out_sdf[FIELDS[16]][0]=round(count_2500*100/count,1)
            out_sdf[FIELDS[17]][0]=round(count_5000*100/count,1)
            out_sdf[FIELDS[18]][0]=round(count_12500*100/count,1)
            out_sdf[FIELDS[19]][0]=round(count_25000*100/count,1)
            out_sdf[FIELDS[20]][0]=round(count_50000*100/count,1)
            out_sdf[FIELDS[21]][0]=round(count_100000*100/count,1)
            out_sdf[FIELDS[22]][0]=round(count_250000*100/count,1)
            out_sdf[FIELDS[23]][0]=round(count_500000*100/count,1)
            out_sdf[FIELDS[24]][0]=round(count_1000000*100/count,1)
            out_sdf[FIELDS[25]][0]=count
            out_sdf[FIELDS[26]][0]=str(MSP) #MISSION_PLANNING FIELD
            out_sdf[FIELDS[27]][0]=SCORE_VALUE#), # THEMATIC SCALE VALUE
            out_sdf[FIELDS[28]][0]=population_scale(common, SCORE_VALUE) # POPULATION_SCALE

        else:
        #    results.append(tuple([oid]  + [-1] * 25 + [0] + ['N/A']*2 + [0]))
            out_sdf[FIELDS[0]][0]=-1
            out_sdf[FIELDS[1]][0]=-1
            out_sdf[FIELDS[2]][0]=-1
            out_sdf[FIELDS[3]][0]=-1
            out_sdf[FIELDS[4]][0]=-1
            out_sdf[FIELDS[5]][0]=-1
            out_sdf[FIELDS[6]][0]=-1
            out_sdf[FIELDS[7]][0]=-1
            out_sdf[FIELDS[8]][0]=-1
            out_sdf[FIELDS[9]][0]=-1
            out_sdf[FIELDS[10]][0]=-1
            out_sdf[FIELDS[11]][0]=-1
            out_sdf[FIELDS[12]][0]=-1
            out_sdf[FIELDS[13]][0]=-1
            out_sdf[FIELDS[14]][0]=-1
            out_sdf[FIELDS[15]][0]=-1
            out_sdf[FIELDS[16]][0]=-1
            out_sdf[FIELDS[17]][0]=-1
            out_sdf[FIELDS[18]][0]=-1
            out_sdf[FIELDS[19]][0]=-1
            out_sdf[FIELDS[20]][0]=-1
            out_sdf[FIELDS[21]][0]=-1
            out_sdf[FIELDS[22]][0]=-1
            out_sdf[FIELDS[23]][0]=-1
            out_sdf[FIELDS[24]][0]=-1
            out_sdf[FIELDS[25]][0]=0
            out_sdf[FIELDS[26]][0]='N/A'
            out_sdf[FIELDS[27]][0]='N/A'
            out_sdf[FIELDS[28]][0]=0

        del geom
        del sq
        del df_current

        return out_sdf, out_fl

    except FunctionError as f_e:
        messages = f_e.args[0]
##        #arcpy.AddError("error in function: %s" % messages["function"])
##        #arcpy.AddError("error on line: %s" % messages["line"])
##        #arcpy.AddError("error in file name: %s" % messages["filename"])
##        #arcpy.AddError("with error message: %s" % messages["synerror"])
##        #arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
##        #arcpy.AddError("error on line: %s" % line)
##        #arcpy.AddError("error in file name: %s" % filename)
##        #arcpy.AddError("with error message: %s" % synerror)