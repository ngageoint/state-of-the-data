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
from __future__ import print_function
import os
import sys
import numpy as np
import pandas as pd
import datetime
from collections import Counter

#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import sotd_config as config

non_std_date = '1901-1-1'
non_std_year_str = non_std_date[0:4]
non_std_year = int(non_std_date[0:4])

FIELDS = ['DOM_DATE',
        'DOM_DATE_CNT',
        'DOM_DATE_PER',
        'DOM_YEAR',
        'DOM_YEAR_CNT',
        'DOM_YEAR_PER',
        'OLDEST_DATE',
        'NEWEST_DATE',
        'NO_DATE_CNT',
        'NO_DATE_PER',
        'PCT_2_YEAR',
        'PCT_5_YEAR',
        'PCT_10_YEAR',
        'PCT_15_YEAR',
        'PCT_15_PLUS_YEAR',
        'FEATURE_CNT',
        'CURRENCY_SCORE']

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
def temporal_currency(gis, df_current, output_features, grid_filter, geom, in_fields):
    """ main driver of program """
    try:

        out_fl = FeatureLayer(gis=gis,url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
            return_all_records=True).df

        ##---cut stuff above-----
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

            out_sdf[FIELDS[0]][0]=dom_date
            out_sdf[FIELDS[1]][0]=dom_date_count
            out_sdf[FIELDS[2]][0]=round(dom_date_count * 100.0 / count,1)
            out_sdf[FIELDS[3]][0]=dom_year
            out_sdf[FIELDS[4]][0]=dom_year_count
            out_sdf[FIELDS[5]][0]=round(dom_year_count * 100.0 / count,1)
            out_sdf[FIELDS[6]][0]=oldest
            out_sdf[FIELDS[7]][0]=newest
            out_sdf[FIELDS[8]][0]=count_non_std_dates
            out_sdf[FIELDS[9]][0]=round(float(count_non_std_dates) * 100.0 / count,1)
            out_sdf[FIELDS[10]][0]=round(float(count_2year) * 100.0 / count,1)
            out_sdf[FIELDS[11]][0]=round(float(count_5year) * 100.0 / count,1)
            out_sdf[FIELDS[12]][0]=round(float(count_10year) * 100.0 / count,1)
            out_sdf[FIELDS[13]][0]=round(float(count_15year) * 100.0 / count,1)
            out_sdf[FIELDS[14]][0]=round(float(count_15year_plus) * 100.0 / count,1)
            out_sdf[FIELDS[15]][0]=int(count)
            out_sdf[FIELDS[16]][0]=int(score)

        else:
            out_sdf[FIELDS[0]][0]="None"
            out_sdf[FIELDS[1]][0]=0
            out_sdf[FIELDS[2]][0]=0
            out_sdf[FIELDS[3]][0]=0
            out_sdf[FIELDS[4]][0]=0
            out_sdf[FIELDS[5]][0]=0
            out_sdf[FIELDS[6]][0]="None"
            out_sdf[FIELDS[7]][0]="None"
            out_sdf[FIELDS[8]][0]=0
            out_sdf[FIELDS[9]][0]=0
            out_sdf[FIELDS[10]][0]=0
            out_sdf[FIELDS[11]][0]=0
            out_sdf[FIELDS[12]][0]=0
            out_sdf[FIELDS[13]][0]=0
            out_sdf[FIELDS[14]][0]=0
            out_sdf[FIELDS[15]][0]=0
            out_sdf[FIELDS[16]][0]=0

        return out_sdf, out_fl

##        out_sdf_as_featureset = out_sdf.to_featureset()
##        print(out_sdf_as_featureset)
##        out_fl.edit_features(updates=out_sdf_as_featureset)
##
##        del df_current
##        del ext
##        del geom

    except FunctionError as f_e:
        messages = f_e.args[0]
##        arcpy.AddError("error in function: %s" % messages["function"])
##        arcpy.AddError("error on line: %s" % messages["line"])
##        arcpy.AddError("error in file name: %s" % messages["filename"])
##        arcpy.AddError("with error message: %s" % messages["synerror"])
##        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
##        arcpy.AddError("error on line: %s" % line)
##        arcpy.AddError("error in file name: %s" % filename)
##        arcpy.AddError("with error message: %s" % synerror)
#--------------------------------------------------------------------------

