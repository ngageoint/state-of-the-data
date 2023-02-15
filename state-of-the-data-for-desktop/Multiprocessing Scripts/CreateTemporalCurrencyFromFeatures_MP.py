"""-----------------------------------------------------------------------------
Name: CreateTemporalAccuracyFromFeatures_MP.py
Purpose: Creates the temporal accuracy score from the feature level positional
        accuracy attribution. Uses multiprocessing to reduce processing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: December, 2015
Modified: August 12, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License
-----------------------------------------------------------------------------"""
from __future__ import print_function
#Native imports
import os
import sys
import time
import uuid
import logging
import datetime
import multiprocessing
from functools import partial
#3rd party imports
import arcpy
from arcpy import da
from arcpy import env
#cumtom classes
import ContentMaturity_Helpers as cm_helper
from common import FunctionError, trace
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

#Checkout spatial analyst extention (some people need this)
arcpy.CheckOutExtension('spatial')

env.overwriteOutput = True

out_fields = ('DOM_DATE', 'DOM_DATE_CNT', 'DOM_DATE_PER', 'DOM_YEAR', 'DOM_YEAR_CNT', \
            'DOM_YEAR_PER', 'FEATURE_CNT', 'OLDEST_DATE', 'NEWEST_DATE', \
            'PCT_2_YEAR', 'PCT_5_YEAR','PCT_10_YEAR', 'PCT_15_YEAR', \
            'PCT_15_PLUS_YEAR', 'NO_DATE_CNT', 'NO_DATE_PER')
in_fields = ('zi001_sdv')

default_result = str('None') + ',' + str(0) + ',' + str(0) + ',' \
            + str('None') + ','  + str(0) + ',' + str(0) + ',' \
            + str(0) + ',' + str('None') + ',' + str('None') + ',' \
            + str(0) + ',' + str(0) + ',' + str(0) + ',' + str(0) + ',' \
            + str(0) + ',' + str(0) + ',' + str(0)

def get_datetime(s):
    digits = s.split('-')
    counter = 0
    if len(digits) == 3:
        if len(digits[0]) == 4:
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

def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts

def date_diff(date):
    return (datetime.datetime.now()-date).days/365.25

def get_currency_metrics(dates):
    date_list = [get_datetime(d) for d in dates]
    count_non_std_dates = sum(non_std == datetime.datetime(1901,1,1,0,0) for non_std in date_list)
    date_list_minus = [x for x in date_list if x != datetime.datetime(1901,1,1,0,0)]
    if len(date_list_minus):
        oldest = min(get_datetime_string(date_list_minus))
        newest = max(get_datetime_string(date_list_minus))
        change_list = [date_diff(dd) for dd in date_list_minus]
        count_2year = sum(x <= 2 for x in change_list)
        count_5year = sum((x <= 5 and x > 2) for x in change_list)
        count_10year = sum((x <= 10 and x > 5) for x in change_list)
        count_15year = sum((x <= 15 and x > 10) for x in change_list)
        count_15year_plus = sum(x >= 15 for x in change_list)
    else:
        oldest = '1111-1-1'
        newest = '1111-1-1'
        change_list = 0
        count_2year = 0
        count_5year = 0
        count_10year = 0
        count_15year = 0
        count_15year_plus = 0

    return count_2year, count_5year, count_10year, count_15year, count_15year_plus, newest, oldest, count_non_std_dates

def create_fields(output_features):

    #'Adding fields to table
    fieldLength = 5
    fieldLength_48 = 48
    arcpy.AddField_management(output_features, out_fields[0], "TEXT", "", "", fieldLength_48) #'DOM_DATE'
    arcpy.AddField_management(output_features, out_fields[1], "LONG", 10, "", "", out_fields[1], "NULLABLE", "REQUIRED") # 'DOM_DATE_CNT'
    arcpy.AddField_management(output_features, out_fields[2], "DOUBLE", "", "", "") #'DOM_DATE_PER'
    arcpy.AddField_management(output_features, out_fields[3], "LONG", 10, "", "", out_fields[3], "NULLABLE", "REQUIRED") # 'DOM_YEAR'
    arcpy.AddField_management(output_features, out_fields[4], "LONG", 10, "", "", out_fields[4], "NULLABLE", "REQUIRED") # 'DOM_YEAR_CNT'
    arcpy.AddField_management(output_features, out_fields[5], "DOUBLE", "", "", "") # 'DOM_YEAR_PER'
    arcpy.AddField_management(output_features, out_fields[7], "TEXT", "", "", 1024) # 'OLDEST_DATE'
    arcpy.AddField_management(output_features, out_fields[8], "TEXT", "", "", 1024) # 'NEWEST_DATE'
    arcpy.AddField_management(output_features, out_fields[14], "LONG", 10, "", "", out_fields[14], "NULLABLE", "REQUIRED") # 'NO_DATE_CNT'
    arcpy.AddField_management(output_features, out_fields[15], "DOUBLE", "", "", "") # 'NO_DATE_PER'
    arcpy.AddField_management(output_features, out_fields[9], "DOUBLE", "", "", "") # 'PCT_2_YEAR'
    arcpy.AddField_management(output_features, out_fields[10], "DOUBLE", "", "", "") # 'PCT_5_YEAR'
    arcpy.AddField_management(output_features, out_fields[11], "DOUBLE", "", "", "") # 'PCT_10_YEAR'
    arcpy.AddField_management(output_features, out_fields[12], "DOUBLE", "", "", "") # 'PCT_15_YEAR'
    arcpy.AddField_management(output_features, out_fields[13], "DOUBLE", "", "", "") # 'PCT_15_PLUS_YEAR'
    arcpy.AddField_management(output_features, out_fields[6], "LONG", 10, "", "", out_fields[6], "NULLABLE", "REQUIRED") # 'FEATURE_CNT'

def create_output_gdb(output_features):
    outputGDB = os.path.dirname(output_features)
    log.info("Output GDB: " + outputGDB)

    #print 'Create output Grid feature class'
    if arcpy.Exists(outputGDB):
        print(outputGDB +  " already exists.")
    else:
        log.info("Creating " + os.path.split(outputGDB)[0] + " and " + os.path.basename(outputGDB))
        arcpy.CreateFileGDB_management(os.path.split(outputGDB)[0], os.path.basename(outputGDB))

def most_common(lst):
    return max(set(lst), key=lst.count), lst.count(max(set(lst), key=lst.count))

def minimun(lst):
    return min(lst), lst.count(min(lst))

def maximum(lst):
    return max(lst), lst.count(max(lst))

def unique(lst):
    return set(lst)

def get_oids(features):
    oids = []
    with da.SearchCursor(features, ('OID@')) as cursor:
        for row in cursor:
            oids.append(row[0])
    return oids

def build_oid_chunks(oids, num_chunks):
    chunk_size = 1 #(len(oids) + num_chunks - 1) / num_chunks
    chunked_list = [oids[i:i + chunk_size] for i in range(0, len(oids), chunk_size)]
    return chunked_list

def process_data(oids, output_features, fc):
    process_uuid = uuid.uuid4().hex
    features_layer = "features_" + process_uuid
    features = arcpy.MakeFeatureLayer_management(fc,features_layer,"#","#","#")

    results = {}
    #objectid in list of objectids
    query = '{0} IN ({1})'.format(arcpy.Describe(output_features).oidFieldName, ",".join(map(str, oids)))
    with da.SearchCursor(output_features, ('OID@', 'SHAPE@'), query) as cursor:
        for row in cursor:
            oid = row[0]
            geometry = row[1]

            arcpy.SelectLayerByLocation_management(features, 'INTERSECT', geometry)

            if int(arcpy.GetCount_management(features)[0]) > 0:

                date_list = []
                year_list = []
                count = 0
                with da.SearchCursor(features, in_fields) as date_cursor:
                    for date in date_cursor:
                        #print(date[0])
                        s = date[0]
                        date_list.append(s)
                        if cm_helper.represents_int(s[0:4]):
                            year_list.append(int(s[0:4]))
                        else:
                            year_list.append(-1)

                        count+=1

                if count > 0:
                    count_2year, count_5year, count_10year, count_15year, count_15year_plus, newest, oldest, non_std_date = get_currency_metrics(date_list)

                    dom_year, dom_year_count = most_common(year_list)
                    dom_date, dom_date_count = most_common(date_list)

                    result = str(dom_date) + ',' + str(dom_date_count) + ',' + str(dom_date_count*100.0/count) +\
                            ',' + str(dom_year) + ',' + str(dom_year_count) + ',' + \
                            str(dom_year_count*100.0/count) +',' + str(count) + ',' + \
                            str(oldest) + ',' + str(newest) +',' + \
                            str(count_2year*100.0/count) + ',' + str(count_5year*100.0/count) +',' + \
                            str(count_10year*100.0/count) + ',' + str(count_15year*100.0/count) +',' + str(count_15year_plus*100.0/count) +',' + \
                            str(non_std_date) + ',' + str(non_std_date*100.0/count)
                else:
                    result = default_result

            else:
                result = default_result

            results[oid] = result

    return results

def multiprocess_data(oids, output_features, topo_features):

    partial_process_temporal_currency = partial(
        process_data,
        output_features=output_features,
        fc=topo_features
    )

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_process_temporal_currency, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results


def update_oputput_fields(output_features, Results):
    with da.UpdateCursor(output_features, ['OID@'] + list(out_fields)) as cursor:
        for row in cursor:
            if row[0] in Results:
                resultString = Results[row[0]]
                result = resultString.split(',')
                row[1] = result[0]
                if cm_helper.represents_int(result[1]):
                    row[2] = int(result[1])
                else:
                    row[2] = 0
                row[3] = float(result[2])
                if cm_helper.represents_int(result[3]):
                    row[4] = int(result[3])
                else:
                    row[4] = 0
                if cm_helper.represents_int(result[4]):
                    row[5] = int(result[4])
                else:
                    row[5] = 0
                row[6] = float(result[5])
                if cm_helper.represents_int(result[6]):
                    row[7] = int(result[6])
                else:
                    row[7] = 0
                row[8] = result[7]
                row[9] = result[8]
                row[10] = float(result[9])
                row[11] = float(result[10])
                row[12] = float(result[11])
                row[13] = float(result[12])
                row[14] = float(result[13])
                row[15] = result[14]
                row[16] = float(result[15])
                cursor.updateRow(row)

def create_temporal_currency(tds_features, polygon_grid, output_features):

    #start timer
    t0 = time.time()

    log.info("Generating output file.")
    create_output_gdb(output_features)

    # Process: Copy Features
    log.info("Creating output features: " + output_features)
    arcpy.CopyFeatures_management(polygon_grid, output_features, "", "0", "0", "0")

    log.info("Creating Fields in Output Feature Class.")
    create_fields(output_features)

    oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_data(oids, output_features, tds_features)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = process_data(oids, output_features, tds_features)

    #print(Results)

    log.info("Updating output fields.")
    update_oputput_fields(output_features, results)

    t6 = (time.time() - t0)/60
    log.info("This process took " + str(t6) + " minutes to run the entire process")

    log.info('Done.')
#--------------------------------------------------------------------------
def main(*argv):
    try:
        #  Inputs
        #
        tds_features = argv[0]
        polygon_grid = argv[1]
        output_features = argv[2]
        #   Logic
        #
        create_temporal_currency(tds_features, polygon_grid, output_features)

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
        log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except:
        line, filename, synerror = trace()
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
    finally:
        logging.shutdown()
#--------------------------------------------------------------------------
if __name__ == "__main__":
    env.overwriteOutput = True
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    argv = (r'TDS\TransportationGroundCrv',
            r'TDS\ResourceSrf',
            r'Test.gdb\TransportationGroundCrv_TemporalCurrency')
    main(*argv)
