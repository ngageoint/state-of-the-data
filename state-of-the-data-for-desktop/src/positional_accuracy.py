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

import os
import sys
import numpy as np
import pandas as pd
from collections import Counter
import geodataset as geomotion
import arcpy
from arcpy import env
from arcpy import da



out_fields = ('MEAN', 'MEDIAN',
              'MODE', 'MIN',
              'MAX', 'NO_DATE_CNT',
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
                ('MEAN', np.float64),
                ('MEDIAN', np.float64),
                ('MODE', np.float64),
                ('MIN', np.float64),
                ('MAX', np.float64),
                ('NO_DATE_CNT', np.int32),
                ('NO_DATE_PCT', np.float64),
                ('FEATURE_CNT', np.int32),
                ('PA_SCORE', np.int32),
                ("TIER", '|S1024')
            ]
        )
        array = np.array(rows, dtypes)
        da.ExtendTable(table, "OID@", array, "_ID", False)
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        features =  str(argv[0])#.split(';')
        value_field = str(argv[1])
        polygon_grid = argv[2]
        output_fc = argv[3]
        out_fc_exists = arcpy.Exists(output_fc)
        fc = features

        output_gdb, out_name = os.path.split(output_fc)

        #  Local Variables
        #
        scratchFolder = env.scratchFolder
        scratchGDB = env.scratchGDB
        grid_polygon = None
        results = []
        fcs = []
        PDVERSION = [int(v) for v in pd.__version__.split('.')]
        #  Logic
        #

        #for fc in features:
        field_lookup = {field.name: field.name for field in arcpy.ListFields(fc)}
        if value_field in field_lookup:
            value_field = field_lookup[value_field]
        else:
            raise ValueError("Invalid field name: %s. Field not found." % value_field)
        f_name = os.path.basename(fc)
        g_name = out_name

        if not out_fc_exists:
            output_gdb = validate_workspace(output_gdb)
            grid_polygon = arcpy.CopyFeatures_management(in_features=polygon_grid,
                                                     out_feature_class=os.path.join(output_gdb, g_name))[0]
            extend_table(grid_polygon)
            grid_sdf = geomotion.SpatialDataFrame.from_featureclass(grid_polygon)
            table = grid_polygon
        else:
            arcpy.MakeFeatureLayer_management(output_fc, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            if len(oids) >1:
                oids_string = str(tuple(oids))
            else:
                oids_string = str('('+ str(oids[0]) + ')')

            where_clause = 'OBJECTID IN ' + oids_string
            grid_sdf = geomotion.SpatialDataFrame.from_featureclass(output_fc,
                                        where_clause=where_clause)
            table = output_fc
            #extend_table(output_fc)
            #results.append(output_fc)

        poly_desc = arcpy.Describe(polygon_grid)
        fc_desc = arcpy.Describe(fc)
        if poly_desc.extent.within(fc_desc.extent):

            temp_fc = 'in_memory/clip'
            arcpy.AddMessage('Clipping features to polygon')
            arcpy.Clip_analysis(fc, polygon_grid, temp_fc)
            arcpy.AddMessage('Created in_memory fc')
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(temp_fc,
                                                            fields=[value_field])
            arcpy.AddMessage('features read into spatial dataframe after clipping')
        else:
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(fc, fields=[value_field])
            arcpy.AddMessage('features read into spatial dataframe without clipping')

        index = data_sdf.sindex
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            oid = row[1].OBJECTID
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids = list(index.intersect(ext))
            df_current = data_sdf.loc[data_sdf.index.isin(row_oids)]#.copy()
            sq = df_current['SHAPE'].disjoint(geom) == False
            df_current = df_current[sq].copy()
            #  Perform positional accuracy analysis
            #
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

                results.append((oid, round(mean,1), median,
                                mode, mmin, mmax,
                                null_count, round(null_percent,1),
                                not_null_count,
                                score, get_tier(score)))
                del df_notnull
                del mean
                del median
                del mode
                del mmax
                del mmin
                del score
                del null_percent
            else:
                r = tuple([oid] + [-1]*5 + [0] * 4 + ["No Ranking"])
                results.append(r)
            if len(results) > 1000:
                print(table)
                extend_table(table=table,
                             rows=results)
                results = []
            del row_oids
            del ext
            del geom
            del oid
            del df_current
        if len(results) > 0:
            print(table)
            extend_table(table=table,
                             rows=results)
            results = []
        del index
        del grid_polygon
        del grid_sdf
        del data_sdf
        fcs.append(fc)
        del fc
        #arcpy.SetParameter(4, fcs)
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
#--------------------------------------------------------------------------
if __name__ == "__main__":
    #env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)