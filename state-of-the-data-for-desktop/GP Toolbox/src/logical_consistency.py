"""-----------------------------------------------------------------------------
Name: logical_consistency.py
Purpose: This summarizes the attribution deficiencies output from the
        Attribution Checker.
Description: This tool creates summarizes the results of the Attribution
        Assessment and statistically summarizes the attribution deficiency
        counts and types across the given grid polygon.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 7, 2015
Modified: April, 2016
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

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

sum_fields = ('OBJECTID',
              'MEAN_DEF_CNT',
              'MEDIAN_DEF_CNT',
              'MIN_DEF_CNT',
              'MAX_DEF_CNT',
              'PRI_NUM_DEF',
              'SEC_NUM_DEF',
              'PER_PRI',
              'PER_SEC',
              'PRI_ATTR_DEF',
              'SEC_ATTR_DEF',
              'PRI_ATTR_DEF_PER',
              'SEC_ATTR_DEF_PER',
              'FEATURE_CNT',
              'PRI_ATTR_DEF_CNT',
              'SEC_ATTR_DEF_CNT',
              'LABEL')

#error_field = ('DEFICIENCY','DEFICIENCY_CNT')

default = (-1,-1,-1,-1,-1,-1,-1,0,0,'N/A','N/A',0,0,0,0,0)

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
                ('MEAN_DEF_CNT', np.float64),
                ('MEDIAN_DEF_CNT', np.int32),
                ('MIN_DEF_CNT', np.int32),
                ('MAX_DEF_CNT', np.int32),
                #STandard deviation
                ('PRI_NUM_DEF', np.int32),
                ('SEC_NUM_DEF', np.int32),
                ('PER_PRI', np.float64),
                ('PER_SEC', np.float64),
                ("PRI_ATTR_DEF", '|S20'), # pri_attr
                ("SEC_ATTR_DEF", '|S20'),
                ('PRI_ATTR_DEF_PER', np.float64),
                ('SEC_ATTR_DEF_PER', np.float64),
                ('FEATURE_CNT', np.int32),
                ('PRI_ATTR_DEF_CNT', np.float64),
                ('SEC_ATTR_DEF_CNT', np.float64),
                ('LC_SCORE', np.int32)
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
#--------------------------------------------------------------------------
def most_common(lst):

    c = Counter(lst)
    mc = c.most_common(2)
    prime = mc[0]
    prime_src = prime[0]
    prime_count = prime[1]
    if len(mc) > 1:
        sec = mc[1]
        sec_src = sec[0]
        sec_count = sec[1]
    else:
        sec_src = -1
        sec_count = 0


    return prime_src, prime_count, sec_src, sec_count
#-------------------------------------------------------------------------------
#Scores logical consistency from 1 to 5
def get_lc_score(val):
    if val == 0:
        score = 5
    elif val == 1:
        score = 4
    elif val >= 2 and val <= 3:
        score = 3
    elif val >= 4 and val < 6:
        score = 2
    elif val >= 6:
        score = 1
    else:
        score = 0

    return score


#--------------------------------------------------------------------------
#Finds the most frequent source from the feature data
def get_answers(oid, err, attr, feature_count):

    count = len(err)
    if count > 0:
        mean_err = round(np.mean(err),1)
        med_err = np.median(err)
        min_err = min(err)
        max_err = max(err)
        std_err = np.std(err)
        primary, primary_count, secondary, secondary_count = most_common(err)
        lc_score = get_lc_score(primary)
        primary_percent = round(primary_count*100.0/count,1)
        secondary_percent = round(secondary_count*100.0/count,1)
        if mean_err >0:
            pri_attr, pri_attr_count, sec_attr, sec_attr_count = most_common(attr)
            pri_attr_percent = round(pri_attr_count*100.0/feature_count,1) #count
            sec_attr_percent = round(sec_attr_count*100.0/feature_count,1) #count
        else:
            pri_attr = 'N/A'
            sec_attr = 'N/A'
            pri_attr_percent = 0
            sec_attr_percent = 0
            pri_attr_count = 0
            sec_attr_count = 0
    else:
        mean_err = -1
        med_err = -1
        min_err = -1
        max_err = -1
        std_err = -1
        primary = -1
        secondary = -1
        primary_percent = 0
        secondary_percent = 0
        pri_attr = 'N/A'
        sec_attr = 'N/A'
        pri_attr_percent = 0
        sec_attr_percent = 0
        pri_attr_count = 0
        sec_attr_count = 0
        lc_score = 0
    #std_err,
    return (oid, mean_err, med_err, min_err,
            max_err, primary,
            secondary, primary_percent, secondary_percent,
            pri_attr, sec_attr, pri_attr_percent,
            sec_attr_percent, count, pri_attr_count,
            sec_attr_count, lc_score)
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        attr_features = argv[0]
        sql_clause = argv[1]
        polygon_grid = argv[2]
        error_field_count = str(argv[3]) #'NULL_COUNT'#
        error_field_def = str(argv[4]) #'NULL_COLUMNS'#
        output_fc = argv[5]
        out_fc_exists = arcpy.Exists(output_fc)

        #  Local Variable
        #
        scratchFolder = env.scratchFolder
        scratchGDB = env.scratchGDB
        results = []
        #  Logic
        #
        if not out_fc_exists:
            output_gdb = validate_workspace(os.path.dirname(output_fc))
            #  Create the grid
            #
            out_grid = arcpy.CopyFeatures_management(polygon_grid, output_fc)[0]
            out_grid = extend_table(out_grid)
            where_clause=None
        else:
            arcpy.MakeFeatureLayer_management(output_fc, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            if len(oids) >1:
                oids_string = str(tuple(oids))
            else:
                oids_string = str('('+ str(oids[0]) + ')')

            where_clause = 'OBJECTID IN ' + oids_string

        error_field = (error_field_def, error_field_count)

        #  Process the Data
        #

        poly_desc = arcpy.Describe(output_fc)
        fc_desc = arcpy.Describe(attr_features)
        if poly_desc.extent.within(fc_desc.extent):

            temp_fc = 'in_memory/clip'
            arcpy.AddMessage('Clipping features to polygon')
            arcpy.Clip_analysis(attr_features, output_fc, temp_fc)
            arcpy.AddMessage('Created in_memory fc')
            #data_sdf = geomotion.SpatialDataFrame.from_featureclass(temp_fc,
            #                                                fields=[value_field])
            if sql_clause:
                attr_sdf = SpatialDataFrame.from_featureclass(temp_fc,
                                                      fields=error_field,
                                                      where_clause=sql_clause)
            else:
                attr_sdf = SpatialDataFrame.from_featureclass(temp_fc,
                                                      fields=error_field)
            arcpy.AddMessage('features read into spatial dataframe after clipping')
        else:
            #data_sdf = geomotion.SpatialDataFrame.from_featureclass(, fields=[value_field])
            arcpy.AddMessage('features read into spatial dataframe without clipping')
            if sql_clause:
                attr_sdf = SpatialDataFrame.from_featureclass(attr_features,
                                                      fields=error_field,
                                                      where_clause=sql_clause)
            else:
                attr_sdf = SpatialDataFrame.from_featureclass(attr_features,
                                                      fields=error_field)

        grid_sdf = SpatialDataFrame.from_featureclass(filename=output_fc,
                                            where_clause=where_clause)

        index = attr_sdf.sindex
        for idx, row in enumerate(grid_sdf.iterrows()):
            errors = []
            attrs = []
            geom = row[1].SHAPE
            oid = row[1].OBJECTID
            print(str(oid))
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids = list(index.intersect(ext))
            df_current = attr_sdf.loc[row_oids]#.copy()
            sq = df_current.geometry.disjoint(geom) == False
            fcount = len(df_current[sq])  # Total Count
            q2 = df_current[error_field_count] > 0
            #& q2
            df_current = df_current[sq].copy() # Get the # of features with deficiency_cnt > 0
            #print("here")
            if fcount>0: #len(df_current) > 0:
                errors += df_current[error_field_count].tolist()
                arcpy.AddMessage(str(errors))
                def process(x):
                    print(x)
                    return [va for va in x.replace(' ', '').split('|')[-1].split(',') if len(va) > 1]
                for e in df_current[error_field_def].apply(process).tolist():
                    attrs += e
                    del e
            row = get_answers(oid=oid,
                              err=errors,
                              attr=attrs,
                              feature_count=fcount)
            results.append(row)
            if len(results) > 250:
                extend_table(table=output_fc, rows=results)
                results = []
            del idx
            del row
            del errors
            del attrs
            del geom
            del oid
            del ext
            del row_oids
            del df_current
            del sq
            del q2
        if len(results) > 0:
            extend_table(table=output_fc, rows=results)
        del index
        del results
        del grid_sdf
        del attr_sdf
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