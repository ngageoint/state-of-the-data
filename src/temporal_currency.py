"""-----------------------------------------------------------------------------
Name: temporal_currency.py
Purpose: Creates the temporal accuracy score from the feature level positional
        accuracy attribution.
Description: This tool creates a statistical summary of a source date field in
        a feature class. It identifies the different dates that features were
        collected and determines the most frequently occuring year in the data.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: December, 2015
Modified: August, 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

from __future__ import division
import os
import sys
import numpy as np
import pandas as pd
import datetime
from collections import Counter
import geodataset as geomotion
import arcpy
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping

non_std_date = '1901-1-1'
non_std_year_str = non_std_date[0:4]
non_std_year = int(non_std_date[0:4])

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
#-------------------------------------------------------------------------------
def get_currency_score(year):
    current_year = datetime.datetime.now()
    if year == non_std_year:
        score = 6
    else:
        if year >= current_year.year - 2:
            score = 5
        elif year >= current_year.year - 4:
            score = 4
        elif year >= current_year.year - 9:
            score = 3
        elif year >= current_year.year - 14:
            score = 2
        else:
            score = 1

    return score

#-------------------------------------------------------------------------------
def extend_table(rows, table):
    """
    appends the results of the array to the existing table by an objectid
    """
    try:
        dtypes = np.dtype(
            [
                ('_ID', np.int),
                ('DOM_DATE', '|S48'),
                ('DOM_DATE_CNT', np.int32),
                ('DOM_DATE_PER', np.float64),
                ('DOM_YEAR', np.int32),
                ('DOM_YEAR_CNT', np.int32),
                ('DOM_YEAR_PER', np.float64),
                ('OLDEST_DATE', '|S1024'),
                ('NEWEST_DATE', '|S1024'),
                ('NO_DATE_CNT', np.int32),
                ('NO_DATE_PER', np.float64),
                ('PCT_2_YEAR', np.float64),
                ('PCT_5_YEAR', np.float64),
                ('PCT_10_YEAR', np.float64),
                ('PCT_15_YEAR', np.float64),
                ('PCT_15_PLUS_YEAR', np.float64),
                ('FEATURE_CNT', np.int32),
                ('CURRENCY_SCORE', np.int32)
            ]
        )
        array = np.array(rows, dtypes)
        da.ExtendTable(table, "OID@", array, "_ID", False)
        return table
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
def grid_fields(grid):
    """
    Adds fields to a given polygon grid.
    """
    try:
        dtypes = np.dtype(
            [
                ('_ID', np.int),
                ('DOM_DATE', '|S48'),
                ('DOM_DATE_CNT', np.int32),
                ('DOM_DATE_PER', np.float64),
                ('DOM_YEAR', np.int32),
                ('DOM_YEAR_CNT', np.int32),
                ('DOM_YEAR_PER', np.float64),
                ('OLDEST_DATE', '|S1024'),
                ('NEWEST_DATE', '|S1024'),
                ('NO_DATE_CNT', np.int32),
                ('NO_DATE_PER', np.float64),
                ('PCT_2_YEAR', np.float64),
                ('PCT_5_YEAR', np.float64),
                ('PCT_10_YEAR', np.float64),
                ('PCT_15_YEAR', np.float64),
                ('PCT_15_PLUS_YEAR', np.float64),
                ('FEATURE_CNT', np.int32),
                ('CURRENCY_SCORE', np.int32)
            ]
        )
        array = np.array([], dtypes)
        da.ExtendTable(grid, "OID@", array, "_ID", False)
        return grid
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "grid_fields",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
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
def diff_date(date):
    """calculates the difference in days from today till the given date"""
    return float((datetime.datetime.now() - date).days)/365.25

#def date_diff(date):
#    return (datetime.datetime.now()-date).days/365.25
#-------------------------------------------------------------------------------
def get_datetime(s):
    try:
        if s:
            digits = s.split('-')
        else:
            digits=" "
        counter = 0
        if len(digits) == 3:
            if len(digits[0]) == 4:
                if digits[0]==non_std_year_str:
                    return datetime.datetime(1902,1,1,0,0)
                else:
                    counter = counter + 1
            if len(digits[1]) == 2:
                counter = counter + 1
            if len(digits[2]) == 2:
                counter = counter + 1
            if counter == 3:
                try:
                    date = datetime.datetime.strptime(s,'%Y-%m-%d')
                except:
                    date = datetime.datetime(1901,1,1,0,0)
            else:
                date = datetime.datetime(1901,1,1,0,0)
        else:
            date = datetime.datetime(1901,1,1,0,0)
        return date
    except:
        if isinstance(s, (datetime.datetime, np.datetime64)) and not s is pd.NaT:
            #arcpy.AddMessage(s)
            date = s
        else:
            #arcpy.AddMessage("Bad year")
            date = datetime.datetime(1901,1,1,0,0)
        return date

def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        features = str(argv[0])#.split(';')
        in_fields = str(argv[1])
        polygon_grid = argv[2]
        grid_polygon = argv[3]
        out_fc_exists = arcpy.Exists(grid_polygon)
        fc = features

        output_gdb = os.path.dirname(grid_polygon)
        #  Local Variables
        #
        results = []
        #fcs = []
        scratchFolder = env.scratchFolder
        scratchGDB = env.scratchGDB
        #  Logic
        #
        master_times = datetime.datetime.now()

        if not out_fc_exists:

            output_gdb = validate_workspace(wrksp=output_gdb)
            #for fc in features:
            arcpy.CopyFeatures_management(in_features=polygon_grid,
                                                     out_feature_class=grid_polygon)
        #fcs.append(grid_polygon)
            grid_polygon = grid_fields(grid=grid_polygon)
            where_clause = None
            grid_sdf = geomotion.SpatialDataFrame.from_featureclass(grid_polygon)

        else:
            arcpy.MakeFeatureLayer_management(grid_polygon, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            if len(oids) >1:
                oids_string = str(tuple(oids))
            else:
                oids_string = str('('+ str(oids[0]) + ')')

            where_clause = 'OBJECTID IN ' + oids_string
            grid_sdf = geomotion.SpatialDataFrame.from_featureclass(grid_polygon,
                                        where_clause=where_clause)


        data_sdf = geomotion.SpatialDataFrame.from_featureclass(fc)
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
                dates = df_current[in_fields].tolist()
                count = len(dates)
                date_list_strings = [d for d in dates]
                date_list = [get_datetime(d) for d in dates]
                year_list = [int(x.year) for x in date_list]
                dom_year, dom_year_count = Counter(year_list).most_common()[0]
                dom_date, dom_date_count = Counter(get_datetime_string(date_list)).most_common()[0]
                count_picket_fences = sum(non_std == datetime.datetime(1902,1,1,0,0) for non_std in date_list)
                count_non_std_dates = sum(non_std == datetime.datetime(1901,1,1,0,0) for non_std in date_list) + count_picket_fences
                date_list_minus = [x for x in date_list if (x != datetime.datetime(1901,1,1,0,0) and x != datetime.datetime(1902,1,1,0,0))]
                if len(date_list_minus)>0:
                    if dom_date == '1902-1-1' or dom_date == '1902-01-01':
                        dom_date = non_std_date
                        dom_year = non_std_year
                        sccore = 6
                        oldest = min(get_datetime_string(date_list_minus))
                        newest = max(get_datetime_string(date_list_minus))
                        change_list = [diff_date(dd) for dd in date_list_minus]
                        count_2year = sum(x <= 2 for x in change_list)
                        count_5year = sum((x <= 5 and x > 2) for x in change_list)
                        count_10year = sum((x <= 10 and x > 5) for x in change_list)
                        count_15year = sum((x <= 15 and x > 10) for x in change_list)
                        count_15year_plus = sum(x >= 15 for x in change_list)
                    elif dom_date == '1901-1-1' or dom_date == '1901-01-01':
                        dom_date = 'NoInformation'
                        dom_year = 0
                        score = 6
                        oldest = min(get_datetime_string(date_list_minus))
                        newest = max(get_datetime_string(date_list_minus))
                        change_list = [diff_date(dd) for dd in date_list_minus]
                        count_2year = sum(x <= 2 for x in change_list)
                        count_5year = sum((x <= 5 and x > 2) for x in change_list)
                        count_10year = sum((x <= 10 and x > 5) for x in change_list)
                        count_15year = sum((x <= 15 and x > 10) for x in change_list)
                        count_15year_plus = sum(x >= 15 for x in change_list)
                    else:
                        dom_date = dom_date
                        dom_year = dom_year
                        oldest = min(get_datetime_string(date_list_minus))
                        newest = max(get_datetime_string(date_list_minus))
                        change_list = [diff_date(dd) for dd in date_list_minus]
                        count_2year = sum(x <= 2 for x in change_list)
                        count_5year = sum((x <= 5 and x > 2) for x in change_list)
                        count_10year = sum((x <= 10 and x > 5) for x in change_list)
                        count_15year = sum((x <= 15 and x > 10) for x in change_list)
                        count_15year_plus = sum(x >= 15 for x in change_list)
                        score = get_currency_score(dom_year)
                else:
                    if dom_date == '1902-01-01':
                        dom_date = non_std_date
                        dom_year = non_std_year
                        oldest = non_std_date
                        newest = non_std_date
                        change_list = 0
                        count_2year = 0
                        count_5year = 0
                        count_10year = 0
                        count_15year = 0
                        count_15year_plus = 0
                        score = 6
                    else:
                        dom_date = 'NoInformation'
                        dom_year = 0
                        oldest = 'NoInformation'
                        newest = 'NoInformation'
                        change_list = 0
                        count_2year = 0
                        count_5year = 0
                        count_10year = 0
                        count_15year = 0
                        count_15year_plus = 0
                        score = 6

                r = (oid,
                        dom_date,
                        dom_date_count,
                        dom_date_count * 100.0 / count,
                        dom_year,
                        dom_year_count,
                        dom_year_count * 100.0 / count,
                        oldest,
                        newest,
                        count_non_std_dates,
                        float(count_non_std_dates) * 100.0 / count,
                        float(count_2year) * 100.0 / count,
                        float(count_5year) * 100.0 / count,
                        float(count_10year) * 100.0 / count,
                        float(count_15year) * 100.0 / count,
                        float(count_15year_plus) * 100.0 / count,
                        int(count),
                        int(score))
                results.append(r)
            else:
                results.append(
                    (oid, "None", 0,0,
                     0,0,0, "None",
                     "None",0,0,
                     0,0,0,
                     0,0,0,0))
            if len(results) > 1000:
                extend_table(rows=results, table=grid_polygon)
                results = []
            del df_current
            del ext
            del geom
            del oid
        if len(results) > 0:
            extend_table(rows=results, table=grid_polygon)
            results = []
        del fc
        del data_sdf
        del index
        #arcpy.SetParameter(4, fcs)
        arcpy.AddMessage("Total Time %s" % (datetime.datetime.now() - master_times))
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
