"""-----------------------------------------------------------------------------
Name: CreateLogicalConsistency_MP.py
Purpose: This summarizes the attribution deficiencies output from the
        Attribution Checker. This uses multiprocessing to reduce procesing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 7, 2015
Modified: August 12, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License
-----------------------------------------------------------------------------"""
from __future__ import print_function
#Native imports
import multiprocessing
from functools import partial
import time
import os
import sys
import math
import shutil
from collections import Counter
import uuid
import logging
#3rd party imports
from common import FunctionError, trace
import numpy as np
import arcpy
from arcpy import env
from arcpy import da
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################
#--------------------------------------------------------------------------
sum_fields = ('OBJECTID','MEAN_DEF_CNT', 'MEDIAN_DEF_CNT', 'MIN_DEF_CNT','MAX_DEF_CNT',
                'PRI_NUM_DEF', 'SEC_NUM_DEF',
                'PER_PRI', 'PER_SEC',
                'PRI_ATTR_DEF', 'SEC_ATTR_DEF',
                'PRI_ATTR_DEF_PER','SEC_ATTR_DEF_PER',
                'FEATURE_CNT',
                'PRI_ATTR_DEF_CNT', 'SEC_ATTR_DEF_CNT', 'LABEL')

error_field = ('DEFICIENCY','DEFICIENCY_CNT')

default = (-1,-1,-1,-1,-1,-1,-1,0,0,'N/A','N/A',0,0,0,0,0)

env.overwriteOutput = True
#--------------------------------------------------------------------------
def create_query_string(ftype):

    queryString = ftype
    begin = """"FEATURE_CLASS"='"""
    end = """'"""
    where_clause = begin + queryString + end

    return where_clause
#--------------------------------------------------------------------------
def get_oids(features):
    oids = []
    with da.SearchCursor(features, ('OID@')) as cursor:
        for row in cursor:
            oids.append(row[0])
    return oids
#--------------------------------------------------------------------------
def build_oid_chunks(oids, num_chunks):
    chunk_size = (len(oids) + num_chunks - 1) / num_chunks
    chunked_list = [oids[i:i + chunk_size] for i in xrange(0, len(oids), chunk_size)]
    return chunked_list
#--------------------------------------------------------------------------
def multiprocess_data(oids, output_features, topo_features):

    partial_process_logical_consistency = partial(
        process_data,
        output_features=output_features,
        fc=topo_features
    )

    # open the pipes
    pool = multiprocessing.Pool(4 * multiprocessing.cpu_count())

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    jobs = []
    #for oids in chunked_oids:
        #jobs.append(pool.apply_async(process_data,
                                     #args=(

                                         #,
                                     #))
    map_results = pool.map(partial_process_logical_consistency, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results

#--------------------------------------------------------------------------
#Processes the data to get the cumulative road length per cell
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

                errors = []
                attrs = []
                fcount = 0
                with da.SearchCursor(features, error_field) as sumCursor:
                    for attrError in sumCursor:
                        fcount+=1
                        errors.append(attrError[1])
                        if attrError[0] != 'N/A':
                            errors.append(attrError[1])
                            attr_list = list_attributes(attrError[0])
                            for element in attr_list:
                                attrs.append(element)

                if len(errors)>0:
                    result = get_answers(errors, attrs, fcount)
                else:
                    result = default
            else:
                result = default

            results[oid] = result

    return results
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
#--------------------------------------------------------------------------
def list_attributes(attr_string):
    attrs = attr_string.split('|')[-1]
    attr_list = attrs.split(',')
    valid_attr = [va for va in attr_list if len(va) > 1]
    return valid_attr
#--------------------------------------------------------------------------
def unique(lst):
    return set(lst)
#--------------------------------------------------------------------------
#Finds the most frequent source from the feature data
def get_answers(err, attr, feature_count):

    count = len(err)
    if len(err) > 0:
        mean_err = np.mean(err)
        med_err = np.median(err)
        min_err = min(err)
        max_err = max(err)
        std_err = np.std(err)
        primary, primary_count, secondary, secondary_count = most_common(err)
        primary_percent = primary_count*100.0/count
        secondary_percent = secondary_count*100.0/count
        if mean_err >0:
            pri_attr, pri_attr_count, sec_attr, sec_attr_count = most_common(attr)
            pri_attr_percent = pri_attr_count*100.0/feature_count #count
            sec_attr_percent = sec_attr_count*100.0/feature_count #count
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

    return mean_err, med_err, min_err, max_err, std_err, primary, secondary, primary_percent, secondary_percent, pri_attr, sec_attr, pri_attr_percent, sec_attr_percent, count, pri_attr_count, sec_attr_count
#--------------------------------------------------------------------------
#Updates the 3 fields in the outpur FC with the calculated values
def update_output_fields(outputFeatureClass, Results, fields):

    with da.UpdateCursor(outputFeatureClass, fields) as cursor:
        for row in cursor:
            if row[0] in Results:
                resultString = Results[row[0]]
                resultAns = resultString #resultString.split(',')
                row[1] = resultAns[0]
                row[2] = resultAns[1]
                row[3] = resultAns[2]
                row[4] = resultAns[3]
                #row[5] = resultAns[4]
                row[5] = resultAns[5]
                row[6] = resultAns[6]
                row[7] = resultAns[7]
                row[8] = resultAns[8]
                row[9] = resultAns[9]
                row[10] = resultAns[10]
                row[11] = resultAns[11]
                row[12] = resultAns[12]
                row[13] = resultAns[13]
                row[14] = resultAns[14]
                row[15] = resultAns[15]
                row[16] = str(row[5]) + ' Attribution Deficiencies'
                cursor.updateRow(row)
#--------------------------------------------------------------------------
def initialize_output(out_gdb, out_fc, grid):
    log.info('Create output Grid feature class')
    if arcpy.Exists(out_gdb):
        log.info(out_gdb +  " already exists.")
    else:
        log.info("Creating " + os.path.split(out_gdb)[0] + " and " + os.path.basename(out_gdb))
        arcpy.CreateFileGDB_management(os.path.split(out_gdb)[0], os.path.basename(out_gdb))

    if arcpy.Exists(out_fc):
        log.info('Features already exist')
    else:
        arcpy.CopyFeatures_management(grid, out_fc)
        log.info( "Adding Field to Analysis FC")
        arcpy.AddField_management(out_fc, sum_fields[5], "SHORT", 2, "", "", sum_fields[5], "NULLABLE", "REQUIRED") #'PRI_NUM_DEF'
        arcpy.AddField_management(out_fc, sum_fields[6], "SHORT", 2, "", "", sum_fields[6], "NULLABLE", "REQUIRED") #'SEC_NUM_DEF'
        arcpy.AddField_management(out_fc, sum_fields[7], "DOUBLE", "", "", "") #'PER_PRI'
        arcpy.AddField_management(out_fc, sum_fields[8], "DOUBLE", "", "", "") #'PER_SEC'
        arcpy.AddField_management(out_fc, sum_fields[1], "SHORT", 2, "", "", sum_fields[1], "NULLABLE", "REQUIRED") #'MEAN_DEF_CNT'
        arcpy.AddField_management(out_fc, sum_fields[2], "SHORT", 2, "", "", sum_fields[2], "NULLABLE", "REQUIRED") #'MEDIAN_DEF_CNT'
        arcpy.AddField_management(out_fc, sum_fields[3], "SHORT", 2, "", "", sum_fields[3], "NULLABLE", "REQUIRED") #'MIN_DEF_CNT'
        arcpy.AddField_management(out_fc, sum_fields[4], "SHORT", 2, "", "", sum_fields[4], "NULLABLE", "REQUIRED") #'MAX_DEF_CNT'
        #arcpy.AddField_management(out_fc, sum_fields[5], "DOUBLE", "", "", "")
        arcpy.AddField_management(out_fc, sum_fields[9], "TEXT", "", "", "20") #'PRI_ATTR_DEF'
        arcpy.AddField_management(out_fc, sum_fields[14], "LONG", 2, "", "", sum_fields[14], "NULLABLE", "REQUIRED") #'PRI_ATTR_DEF_CNT'
        arcpy.AddField_management(out_fc, sum_fields[11], "DOUBLE", "", "", "") #'PRI_ATTR_DEF_PER'
        arcpy.AddField_management(out_fc, sum_fields[10], "TEXT", "", "", "20") #'SEC_ATTR_DEF'
        arcpy.AddField_management(out_fc, sum_fields[15], "LONG", 2, "", "", sum_fields[15], "NULLABLE", "REQUIRED") #'SEC_ATTR_DEF_CNT'
        arcpy.AddField_management(out_fc, sum_fields[12], "DOUBLE", "", "", "") #'SEC_ATTR_DEF_PER'
        arcpy.AddField_management(out_fc, sum_fields[13], "LONG", 2, "", "", sum_fields[13], "NULLABLE", "REQUIRED") #'FEATURE_CNT'
        arcpy.AddField_management(out_fc, sum_fields[16], "TEXT", "", "", "48") #'PRI_ATTR_DEF'

def copy_selected_features(in_fc, out_gdb, out_fc, query_string):
    if arcpy.Exists(os.path.join(out_gdb, out_fc))==False:
        arcpy.FeatureClassToFeatureClass_conversion(in_fc, out_gdb, out_fc, query_string)

    features_to_analyze = os.path.join(out_gdb, out_fc)

    return features_to_analyze
#--------------------------------------------------------------------------
#Runs all the functions to create the completeness score
def create_logical_consistency(tds_features, attribution_features,
                               polygon_grid, output_features):
    #, attribution_workspace, features, Grid, outputFC, scratch_workspace):

    #start timer
    t0 = time.clock()
    features = os.path.basename(tds_features)
    ftype = features[-3:]
    where_clause = create_query_string(features)
    log.info('Looking at: ')
    log.info(where_clause)

    #global chunksize
    grid_fcount = int(arcpy.GetCount_management(polygon_grid)[0])

    log.info('Creating output data products.')
    out_gdb = os.path.dirname(output_features)
    out_fc = os.path.basename(output_features)
    initialize_output(out_gdb, output_features, polygon_grid)

    log.info('Creating features to analyze.')
    features_with_errors = copy_selected_features(attribution_features, out_gdb, out_fc+'_features', where_clause)

    oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_data(oids, output_features, features_with_errors)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = process_data(oids, output_features, features_with_errors)

    log.info('Updating the output fields')
    update_output_fields(output_features, results, sum_fields)

    t6 = (time.clock() - t0)/60
    log.info("This process took " + str(t6) + " minutes to run the entire process")

    log.info('Done.')
#--------------------------------------------------------------------------
def main(*argv):
    try:
        tds_features = argv[0]
        attr_features = argv[1]
        polygon_grid = argv[2]
        output_features = argv[3]
        #  Set the multiprocessing variable
        #
        pythonExe = os.path.join(sys.exec_prefix, 'python.exe')
        multiprocessing.set_executable(pythonExe)
        multiprocessing.freeze_support = True
        #  Run the process
        #
        create_logical_consistency(tds_features, attr_features,
                                   polygon_grid, output_features)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
        log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except FunctionError, f_e:
        messages = f_e.args[0]
        log.error("error in function: %s" % messages["function"])
        log.error("error on line: %s" % messages["line"])
        log.error("error in file name: %s" % messages["filename"])
        log.error("with error message: %s" % messages["synerror"])
        log.error("ArcPy Error Message: %s" % messages["arc"])
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
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)