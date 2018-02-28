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
import os
import sys
from geodataset import SpatialDataFrame
import numpy as np
import pandas as pd
import arcpy
from arcpy import env
from arcpy import da

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
def validate_workspace(wrksp):
    """
    Validates and ensures output workspace exists
    """
    try:
        if wrksp.lower().endswith('.gdb') and \
           os.path.isdir(wrksp) == False:
                return arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(wrksp),
                                                     out_name=os.path.basename(wrksp))[0]
        elif wrksp.lower().endswith('.sde') and \
             os.path.isfile(wrksp) == False:
            raise ValueError("SDE workspace must exist before using it.")
        elif os.path.isdir(wrksp) == False:
            os.makedirs(wrksp)
            return wrksp
        else:
            return wrksp
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "validate_workspace",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
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
def create_grls(grid, population):
    """Creates a table to join to the grid dataset"""
    try:
        output_features = os.path.join(env.scratchGDB, "temp_grid")
        reclass_population = os.path.join(env.scratchFolder, "rast_temp.tif")
        zonal_table = os.path.join(env.scratchGDB, 'zonalstats') #in_memory\\table"
        if arcpy.Exists(reclass_population):
            arcpy.Delete_management(reclass_population)
        if arcpy.Exists(zonal_table):
            arcpy.Delete_management(zonal_table)
        output_features = arcpy.CopyFeatures_management(grid, output_features)[0]
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
def main(*argv):
    """ main driver of program """
    try:
        arcpy.CheckOutExtension('spatial')

        features =   str(argv[0])#.split(';')
        value_field = argv[1].upper()
        population = argv[2]
        polygon_grid = argv[3]
        out_grid = argv[4]
        out_fc_exists = arcpy.Exists(out_grid)
        output_gdb, out_name = os.path.split(out_grid)

        #  Local Variables
        #
        scratchFolder = env.scratchFolder
        scratchGDB = env.scratchGDB
        grid_polygon = None
        fcs = []
        results = []

        fc = features
        #for fc in features:
        #out_name = "srcTA_%s_%s" % (os.path.basename(fc[:-3]), fc[-3:])
        #out_grid = os.path.join(output_gdb, out_name)
        if not out_fc_exists:
            print("creating fc")
            output_gdb = validate_workspace(output_gdb)
            print ("process the raster dataset")
            polygon_grid = create_grls(grid=polygon_grid,
                                   population=population)
            polygon_grid = extend_table(table=polygon_grid)
            print ('end raster processing')
            arcpy.AddMessage("Working on feature class: %s" % os.path.basename(fc))
            arcpy.CopyFeatures_management(polygon_grid, out_grid)
            grid_sdf = SpatialDataFrame.from_featureclass(filename=out_grid)
        else:
            print("FC exists")
            arcpy.MakeFeatureLayer_management(out_grid, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            if len(oids) >1:
                oids_string = str(tuple(oids))
            else:
                oids_string = str('('+ str(oids[0]) + ')')

            where_clause = 'OBJECTID IN ' + oids_string
            grid_sdf = SpatialDataFrame.from_featureclass(out_grid,
                                        where_clause=where_clause)


        data_sdf = SpatialDataFrame.from_featureclass(fc, fields=[value_field])
        index = data_sdf.sindex
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            oid = row[1].OBJECTID
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids = list(index.intersect(ext))
            df_current = data_sdf.loc[data_sdf.index.isin(row_oids)]
            sq = df_current['SHAPE'].disjoint(geom) == False
            df_current = df_current[sq].copy()

            if len(df_current) > 0:
                #arcpy.AddMessage(str(len(df_current)))
                #print ('do stuff here')
                #df_current['EQUAL'] = 0
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
                # 1).
                MSP = get_msp(scale=common) # SHOULD UPDATE MISSION_PLANNING FIELD
                #

                SCORE_VALUE = get_equal_breaks_score(mean=row[1].MEAN)# get_equal_breaks_score(output_features, ['MEAN','EQUAL']) # PUT SCORE IN EQUAL
                GRLS = SCORE_VALUE
                domScale = common
                # FIELD 1 is the source, Field 2 is the field to be updated
                #df_current['EQUAL'] = SCORE_VALUE # ASSIGNS EQUAL TO LANSCAN_SCALE
                #29 field
                result = (oid,# OID
                          common,# median
                          common_count, # % common
                          round(common_per,1),
                          min_val,#
                          round(min_scale,1),#
                          max_val,#
                          round(max_scale,1),#
                          count_2500,
                          count_5000,
                          count_12500,
                          count_25000,
                          count_50000,
                          count_100000,
                          count_250000,
                          count_500000,
                          count_1000000,
                          round(count_2500*100/count,1),
                          round(count_5000*100/count,1),
                          round(count_12500*100/count,1),
                          round(count_25000*100/count,1),
                          round(count_50000*100/count,1),
                          round(count_100000*100/count,1),
                          round(count_250000*100/count,1),
                          round(count_500000*100/count,1),
                          round(count_1000000*100/count,1),
                          count,
                          str(MSP), #MISSION_PLANNING FIELD
                          SCORE_VALUE,#), # THEMATIC SCALE VALUE
                          population_scale(common, SCORE_VALUE) # POPULATION_SCALE
                          )
                #arcpy.AddMessage(result)
                results.append(result)
                #arcpy.AddMessage('appended result')
            else:
                #arcpy.AddMessage('appending the default result.')
                results.append(tuple([oid]  + [-1] * 25 + [0] + ['N/A']*2 + [0]))
                #arcpy.AddMessage('append the default result.')
            del geom, oid
            del row_oids
            del sq
            del df_current
        del fc
        fcs.append(extend_table(table=out_grid, rows=results))
        del results
        del grid_sdf,data_sdf, index
        #arcpy.SetParameter(5, fcs)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        arcpy.AddError("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except FunctionError as f_e:
        messages = f_e.args[0]
        arcpy.AddError("error in function: %s" % messages["function"])
        arcpy.AddError("error on line: %s" % messages["line"])
        arcpy.AddError("error in file name: %s" % messages["filename"])
        arcpy.AddError("with error message: %s" % messages["synerror"])
        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
    finally:
        arcpy.CheckInExtension('spatial')
#--------------------------------------------------------------------------
if __name__ == "__main__":
    #env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)