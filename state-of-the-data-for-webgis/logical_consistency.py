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

import os
import ast
import sys
import time
import xlrd
import numpy as np
import pandas as pd
import datetime
import platform
import itertools
import traceback
from xlrd.sheet import ctype_text
from collections import Counter

#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import SpatialDataFrame
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry
from arcgis.geometry import Polyline, Point, Polygon

import sotd_config as config
import create_selection_layers as csl

import arcpy
from arcpy import env
from arcpy import da

SUM_FIELDS = ['MEAN_DEF_CNT',
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
              'LC_SCORE']

FIELDS = ['DEFICIENCY',
            'FEATURE_CLASS',
            'SUBTYPE',
            'ORIG_OID',
            'DEFICIENCY_CNT',
            'SHAPE']


default = (-1,-1,-1,-1,-1,-1,-1,0,0,'N/A','N/A',0,0,0,0,0)
empty = (-999999, '', None, 'noInformation','None', 'Null', 'NULL', -999999.0)

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

def create_attr_dict(filename, check):
    """Creates and attribute dictionary"""
    xl_workbook = xlrd.open_workbook(filename)
    specificAttributeString = '{'
    specificAttributeDict = {}
    xl_sheet = xl_workbook.sheet_by_name(check)
    for row in range(xl_sheet.nrows):
        if row>0:
            cell = xl_sheet.cell(row,8)
            specificAttributeString += cell.value
    specificAttributeDict = ast.literal_eval(specificAttributeString[:-1] + '}')
    return specificAttributeDict, check

def get_fc_domains(gdb):
    domains = da.ListDomains(gdb)
    domain_dict = {}
    for domain in domains:
        if 'FCODE' in domain.name:
            domain_dict.update(domain.codedValues)

    return domain_dict

#-------------------------------------------------------------------------------
def get_field_alias(fc):
    fields = arcpy.ListFields(fc)

    field_dict = {}
    for field in fields:
        field_dict[field.name] = field.aliasName

    return field_dict

#--------------------------------------------------------------------------
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
##                ('MEAN_DEF_CNT', np.float64),
##                ('MEDIAN_DEF_CNT', np.int32),
##                ('MIN_DEF_CNT', np.int32),
##                ('MAX_DEF_CNT', np.int32),
##                #STandard deviation
##                ('PRI_NUM_DEF', np.int32),
##                ('SEC_NUM_DEF', np.int32),
##                ('PER_PRI', np.float64),
##                ('PER_SEC', np.float64),
##                ("PRI_ATTR_DEF", '|S20'), # pri_attr
##                ("SEC_ATTR_DEF", '|S20'),
##                ('PRI_ATTR_DEF_PER', np.float64),
##                ('SEC_ATTR_DEF_PER', np.float64),
##                ('FEATURE_CNT', np.int32),
##                ('PRI_ATTR_DEF_CNT', np.float64),
##                ('SEC_ATTR_DEF_CNT', np.float64),
##                ('LC_SCORE', np.int32)
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
def logical_consisitency(gis, template_fc, template_gdb,
            filename, tabname,
            data_sdf, input_features, output_features,
            grid_filter, geom,
            def_cnt_field, def_field):

    try:
        stList = set(data_sdf['F_CODE'].values)
        fc = input_features

        alias_table = get_field_alias(template_fc)
        fc_domain_dict = get_fc_domains(template_gdb)

        specificAttributeDict, attrCheck = create_attr_dict(filename, tabname)

        temp_result_df = pd.DataFrame(columns = FIELDS)#, dtypes=DTYPES)

        geoms=[]
        counter=0
        total_feature_count = len(data_sdf)
        for idx, row in data_sdf.iterrows():
            #print(row['F_CODE'])
            if row['F_CODE'] in stList:
                if row['F_CODE'] in specificAttributeDict:
                    vals = []
                    vals = [alias_table[i] for i in specificAttributeDict[row['F_CODE']] \
                                               if row[i] in empty]

                    line = row['SHAPE']
                    def_count = len(vals)
                    polyline = Polyline(line)
                    geoms.append(polyline)
                    if def_count > 0:
                        fs = ",".join(vals)
                        oid = row['OBJECTID']
                        ERROR = str(fc) + r" | " + str(fc_domain_dict[row['F_CODE']]) + r" | OID: " + str(oid) + r" | " + fs

                        temp_result_df.set_value(counter, FIELDS[0],fs)
                        temp_result_df.set_value(counter, FIELDS[1],fc)
                        temp_result_df.set_value(counter, FIELDS[2],(fc_domain_dict[row['F_CODE']]))
                        temp_result_df.set_value(counter, FIELDS[3],round(oid))
                        temp_result_df.set_value(counter, FIELDS[4],len(vals))


                    else:
                        temp_result_df.set_value(counter, FIELDS[0],'N/A')
                        temp_result_df.set_value(counter, FIELDS[1],fc)
                        temp_result_df.set_value(counter, FIELDS[2],(fc_domain_dict[row['F_CODE']]))
                        temp_result_df.set_value(counter, FIELDS[3],round(oid))
                        temp_result_df.set_value(counter, FIELDS[4],len(vals))
                    counter = counter+1
        assessed_feature_count = len(temp_result_df)

        attr_sdf = SpatialDataFrame(temp_result_df, geometry=geoms)

        out_fl = FeatureLayer(gis=gis,url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,return_all_records=True).df

        df_current = attr_sdf
        fcount = len(df_current)

        error_field_count = def_cnt_field
        error_field_def = def_field

        errors = []
        attrs = []
        if fcount>0: #len(df_current) > 0:
            errors += df_current[error_field_count].tolist()
            def process(x):
                #print(x)
                return [va for va in x.replace(' ', '').split('|')[-1].split(',') if len(va) > 1]
            for e in df_current[error_field_def].apply(process).tolist():
                attrs += e
                del e

        results = get_answers(0,errors,attrs, fcount)

        out_sdf[SUM_FIELDS[0]][0]=results[1]
        out_sdf[SUM_FIELDS[1]][0]=results[2]
        out_sdf[SUM_FIELDS[2]][0]=results[3]
        out_sdf[SUM_FIELDS[3]][0]=results[4]
        out_sdf[SUM_FIELDS[4]][0]=results[5]
        out_sdf[SUM_FIELDS[5]][0]=results[6]
        out_sdf[SUM_FIELDS[6]][0]=results[7]
        out_sdf[SUM_FIELDS[7]][0]=results[8]
        out_sdf[SUM_FIELDS[8]][0]=results[9]
        out_sdf[SUM_FIELDS[9]][0]=results[10]
        out_sdf[SUM_FIELDS[10]][0]=results[11]
        out_sdf[SUM_FIELDS[11]][0]=results[12]
        out_sdf[SUM_FIELDS[12]][0]=results[13]
        out_sdf[SUM_FIELDS[13]][0]=results[14]
        out_sdf[SUM_FIELDS[14]][0]=results[15]
        out_sdf[SUM_FIELDS[15]][0]=results[16]

        print(out_sdf.columns.values)

        return out_sdf, out_fl

    except arcpy.ExecuteError:
        line, filename, synerror = trace()

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