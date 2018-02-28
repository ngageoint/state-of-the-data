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
from geodataset import SpatialDataFrame

import arcpy
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping
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
                ('TDS_DENSITY', np.float64),
                ('COMP_DENSITY', np.float64),
                ('COMPLETENESS_VALUE', np.float64),
                ('DIFFERENCE', np.float64)
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
def main(*argv):
    """ main driver of program """
    try:
        before_feature = argv[0]
        after_feature = argv[1]
        polygon_grid = argv[2]
        out_grid = argv[3]
        out_fc_exists = arcpy.Exists(out_grid)

        #output_gdb = argv[3]
        #  Local Variable
        #
        scratchFolder = env.scratchFolder
        scratchGDB = env.scratchGDB
        results = []
        #out_grid = os.path.join(output_gdb, os.path.basename(before_feature) + "_compare")
        #  Logic
        #
        output_gdb = os.path.dirname(out_grid)

        output_gdb = validate_workspace(output_gdb)
        #  Create the grid
        #
        if not out_fc_exists:
            out_grid_temp = "in_memory\\temp"#os.path.join(output_gdb, os.path.basename(before_feature) + "_temp")
            out_grid_temp = arcpy.CopyFeatures_management(polygon_grid, out_grid_temp)[0]
            out_grid_temp = extend_table(out_grid_temp)
            where_clause = None
            grid_sdf = SpatialDataFrame.from_featureclass(filename=out_grid_temp,
                                    where_clause=where_clause)
        else:
            arcpy.MakeFeatureLayer_management(out_grid, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            oids_string = str(tuple(oids))
            where_clause = 'OBJECTID IN ' + oids_string
            grid_sdf = SpatialDataFrame.from_featureclass(filename=out_grid,
                                    where_clause=where_clause)

        before_sdf = SpatialDataFrame.from_featureclass(before_feature)
        after_sdf = SpatialDataFrame.from_featureclass(after_feature)

        before_index = before_sdf.sindex
        after_index = after_sdf.sindex
        geometry_type = after_sdf.geometry_type
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            oid = row[1].OBJECTID
            #print ([idx, oid])
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids_before = list(before_index.intersect(ext))
            row_oids_after = list(after_index.intersect(ext))
            df_before = before_sdf.loc[row_oids_before]
            sq = df_before[df_before.geometry.notnull()].geometry.disjoint(geom) == False
            df_before = df_before[sq].copy()
            before_count = len(df_before)
            df_after = after_sdf.loc[row_oids_after]
            sq = df_after[df_after.geometry.notnull()].geometry.disjoint(geom) == False
            df_after = df_after[sq].copy()
            after_count = len(df_after)
            geoms_after = df_after.clip(geom.extent)
            geoms_before = df_before.clip(geom.extent)
            if geometry_type == "polygon":
                before_val = geoms_before.getArea('GEODESIC','SQUAREKILOMETERS').sum()
                after_val = geoms_after.getArea('GEODESIC','SQUAREKILOMETERS').sum()
                grid_sdf.loc[[idx], 'TDS_DENSITY'] = round(before_val,1)
                grid_sdf.loc[[idx], 'COMP_DENSITY'] = round(after_val,1)
                grid_sdf.loc[[idx], 'DIFFERENCE'] = round(before_val - after_val,1)
                if after_val > 0:
                    score = get_score(ratio=before_val/after_val,
                            baseVal=before_val,
                            inputVal=after_val)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(before_val/after_val,
                                                                          before_val, after_val)
                else:
                    score = get_score(0, before_val, after_val)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(0, before_val, after_val)
                results.append((oid, round(before_val,1), round(after_val,1), score,
                        round(before_val - after_val,1)))
            elif geometry_type == "polyline":
                before_val = geoms_before.getLength('GEODESIC','KILOMETERS').sum()
                after_val = geoms_after.getLength('GEODESIC','KILOMETERS').sum()
                grid_sdf.loc[[idx], 'TDS_DENSITY'] = round(before_val,1)
                grid_sdf.loc[[idx], 'COMP_DENSITY'] = round(after_val,1)
                grid_sdf.loc[[idx], 'DIFFERENCE'] = round(before_val - after_val,1)
                if after_val > 0:
                    score = get_score(ratio=before_val/after_val,
                            baseVal=before_val,
                            inputVal=after_val)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(ratio=before_val/after_val,
                                                                          baseVal=before_val,
                                                                          inputVal=after_val)
                else:
                    score = get_score(0, before_val, after_val)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(0, before_val, after_val)
                results.append((oid, round(before_val,1), round(after_val,1), score,
                        round(before_val - after_val,1)))
            else:
                grid_sdf.loc[[idx], 'TDS_DENSITY'] = before_count
                grid_sdf.loc[[idx], 'COMP_DENSITY'] = after_count
                grid_sdf.loc[[idx], 'DIFFERENCE'] = before_count - after_count
                if after_val > 0:
                    score = get_score(ratio=before_count/after_count,
                            baseVal=before_count,
                            inputVal=after_count)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(ratio=before_count/after_count,
                                                                          baseVal=before_count,
                                                                          inputVal=after_count)
                else:
                    score = get_score(ratio=0,
                            baseVal=before_count,
                            inputVal=after_count)
                    grid_sdf.loc[[idx], 'COMPLETENESS_VALUE'] = get_score(ratio=0,
                                                                          baseVal=before_count,
                                                                          inputVal=after_count)
                results.append((oid, before_count, after_count, score,
                        before_count - after_count))

            del sq
            del row_oids_after
            del row_oids_before
            del df_after
            del df_before
            del idx
            del row
            del geom
            del oid
            del ext
        if not out_fc_exists:
            out_grid = grid_sdf.to_featureclass(out_location=os.path.dirname(out_grid),
                                            out_name=os.path.basename(out_grid))
        else:
            extend_table(out_grid, results)

        #arcpy.SetParameterAsText(4, out_grid)
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