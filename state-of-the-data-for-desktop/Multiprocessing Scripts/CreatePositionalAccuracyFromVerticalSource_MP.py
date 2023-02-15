"""-----------------------------------------------------------------------------
Name: CreatePositionalAccuracyFromVerticalSource_MP.py
Purpose: Summarizes the vertical source description VSN attribute field. Uses
        multiprocessing to reduce processing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 19, 2015
Modified: August 24, 2016
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
import multiprocessing
from functools import partial
#3rd party imports
import numpy as np
import arcpy
from arcpy import da
from arcpy import env
#cumtom classes
from common import FunctionError, trace
import ContentMaturity_Helpers as cm_helper

###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

#Checkout spatial analyst extention (some people need this)
arcpy.CheckOutExtension('spatial')

env.overwriteOutput = True

out_fields = ('MEAN', 'MEDIAN',
              'MODE', 'MIN',
              'MAX', 'NO_DATE_CNT',
              'NO_DATE_PCT', 'FEATURE_CNT',
              'PA_SCORE', "TIER")
in_fields = ('zi001_vsn')

def create_fields(output_features):
    #'Adding fields to table
    arcpy.AddField_management(output_features, out_fields[0], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[1], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[2], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[3], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[4], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[5], "LONG", 2, "", "", out_fields[5], "NULLABLE", "REQUIRED") #'PRI_ATTR_DEF_CNT'
    arcpy.AddField_management(output_features, out_fields[6], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[7], "LONG", 2, "", "", out_fields[7], "NULLABLE", "REQUIRED") #'PRI_ATTR_DEF_CNT'
    arcpy.AddField_management(output_features, out_fields[8], "SHORT")
    arcpy.AddField_management(output_features, out_fields[9], "TEXT")


def create_output_gdb(output_features):
    outputGDB = os.path.dirname(output_features)
    log.info("Output GDB: " + outputGDB)

    #print 'Create output Grid feature class'
    if arcpy.Exists(outputGDB):
        log.info(outputGDB +  " already exists.")
    else:
        log.info("Creating " + os.path.split(outputGDB)[0] + " and " + os.path.basename(outputGDB))
        arcpy.CreateFileGDB_management(os.path.split(outputGDB)[0], os.path.basename(outputGDB))

def most_common(lst):
    return max(set(lst), key=lst.count), lst.count(max(set(lst), key=lst.count))

def minimun(lst):
    return min(lst), lst.count(min(lst))

def maximum(lst):
    return max(lst), lst.count(max(lst))

def mean(lst):
    return np.mean(lst)

def median(lst):
    return np.median(lst)

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

            val_list = []
            count = 0
            with da.SearchCursor(features, in_fields) as cursor:
                for field in cursor:
                    val_list.append(cm_helper.represents_float(field[0]))
                    count+=1

            if count > 0:
                num_nulls = val_list.count(-1)
                non_null_val_list = [x for x in val_list if x != -1]
                if len(non_null_val_list) > 0:
                    max_val, max_scale = maximum(non_null_val_list)
                    min_val, min_scale = minimun(non_null_val_list)
                    common, common_count = most_common(non_null_val_list)
                    med = median(non_null_val_list)
                    average = mean(non_null_val_list)
                    result = str(average) + ',' + str(med) + ',' + str(common) + ',' + str(min_val) + ',' + str(max_val) + ',' + str(num_nulls) + ',' + str(num_nulls*100.0/count) + ',' + str(count)
                else:
                    result = str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(0) + ',' + str(0) + ',' + str(0)
            else:
                result = str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(-1) + ',' + str(0) + ',' + str(0) + ',' + str(0)

            results[oid] = result

    return results

def multiprocess_data(oids, output_features, topo_features):

    partial_process_positional_accuracy = partial(
        process_data,
        output_features=output_features,
        fc=topo_features
    )

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_process_positional_accuracy, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results

def get_score(mean):
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

def update_oputput_fields(output_features, Results):
    with da.UpdateCursor(output_features,  ['OID@'] + list(out_fields)) as cursor:
        for row in cursor:
            if row[0] in Results:
                resultString = Results[row[0]]
                #print(resultString)
                result = resultString.split(',')
                row[1] = float(result[0])
                row[2] = float(result[1])
                row[3] = float(result[2])
                row[4] = float(result[3])
                row[5] = float(result[4])
                row[6] = float(result[5])
                row[7] = float(result[6])
                row[8] = float(result[7])
                score = get_score(float(result[0]))
                row[9] = score
                row[10] = get_tier(score)
                cursor.updateRow(row)

def create_vertical_source_info(tds_features, polygon_grid, output_features):

    #start timer
    t0 = time.time()

    log.info("Generating output GDB.")
    create_output_gdb(output_features)

    log.info('Creating output features.')
    arcpy.CopyFeatures_management(polygon_grid, output_features)

    log.info("Creating Fields in Output Feature Class.")
    create_fields(output_features)

    oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_data(oids, output_features, tds_features)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = process_data(oids, output_features, tds_features)

    #print(results)

    log.info("Updating output fields.")
    update_oputput_fields(output_features, results)

    t6 = (time.time() - t0)/60
    log.info("This process took " + str(t6) + " minutes to run the entire process")

    log.info('Done.')
#--------------------------------------------------------------------------
def main(*argv):
    try:
        tds_features = argv[0]
        polygon_grid = argv[1]
        output_features = argv[2]
        create_vertical_source_info(tds_features,
                                    polygon_grid,
                                    output_features)

    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
    #     log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    # except FunctionError, f_e:
    #     messages = f_e.args[0]
    #     log.error("error in function: %s" % messages["function"])
    #     log.error("error on line: %s" % messages["line"])
    #     log.error("error in file name: %s" % messages["filename"])
    #     log.error("with error message: %s" % messages["synerror"])
    #     log.error("ArcPy Error Message: %s" % messages["arc"])
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
    argv = (r'TDS\TransportationGroundCrv',
            r'TDS\ResourceSrf',
            r'Test.gdb\TransportationGroundCrv_PositionalAccFromVSN')
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    main(*argv)
