"""-----------------------------------------------------------------------------
Name: CreateThematicAccuracy_MP.py
Purpose: Creates the thematic accuracy score from the feature level metadata.
        Uses multiprocessing to reduce processing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August, 2015
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
import multiprocessing
from functools import partial
#3rd party imports
import arcpy
#cumtom classes
from arcpy import env
from arcpy import da
import ContentMaturity_Helpers as cm_helper
from common import FunctionError, trace
###########################################################################
module = os.path.basename(__file__)
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

#Checkout spatial analyst extention (some people need this)
arcpy.CheckOutExtension('spatial')

env.overwriteOutput = True

out_fields = ('MISSION_PLANNING','DOM_SCALE', 'DOM_PER', 'MIN_SCALE',
            'MIN_PER', 'MAX_SCALE', 'MAX_PER',
            'LANDSCAN_SCALE', 'THEM_ACC_SCORE',
            'CNT_2500','CNT_5000','CNT_12500','CNT_25000','CNT_50000',
            'CNT_100000','CNT_250000','CNT_500000','CNT_1000000',
            'PER_2500','PER_5000','PER_12500','PER_25000','PER_50000',
            'PER_100000','PER_250000','PER_500000','PER_1000000')

#in_fields = ('zi026_ctul') #does he mean 'zi026_ctuu'?
in_fields = ('zi026_ctuu')
default_result = str(-1) + ',' + str(0) + ',' + str(-1) + ',' + str(0) + ',' + str(-1) + ',' + str(0) + ','+\
        str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)+','+str(0)

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

def get_scale_range(grid, fields):
    log.info('Creating scale range.')
    with da.UpdateCursor(grid, fields) as cursor:
        for row in cursor:
            row[1] = row[0]
            cursor.updateRow(row)

def get_thematic_accuracy_score(Grid, fields):
    log.info('Creating thematic score.')
    with da.UpdateCursor(Grid, fields) as cursor:
        for row in cursor:
            if (row[0] != None and row[1] >= 0):
                GRLS = row[0]
                domScale = row[1]
                if (domScale == 5000 and GRLS == 'G'):
                    row[2] = 5
                elif (domScale == 5000 and GRLS == 'R'):
                    row[2] = 5
                elif (domScale == 5000 and GRLS == 'L'):
                    row[2] = 5
                elif (domScale == 5000 and GRLS == 'S/U'):
                    row[2] = 5
                elif (domScale == 12500 and GRLS == 'G'):
                    row[2] = 5
                elif (domScale == 12500 and GRLS == 'R'):
                    row[2] = 5
                elif (domScale == 12500 and GRLS == 'L'):
                    row[2] = 5
                elif (domScale == 12500 and GRLS == 'S/U'):
                    row[2] = 5
                elif (domScale == 25000 and GRLS == 'G'):
                    row[2] = 5
                elif (domScale == 25000 and GRLS == 'R'):
                    row[2] = 5
                elif (domScale == 25000 and GRLS == 'L'):
                    row[2] = 5
                elif (domScale == 25000 and GRLS == 'S/U'):
                    row[2] = 5
                elif (domScale == 50000 and GRLS == 'G'):
                    row[2] = 4
                elif (domScale == 50000 and GRLS == 'R'):
                    row[2] = 4
                elif (domScale == 50000 and GRLS == 'L'):
                    row[2] = 4
                elif (domScale == 50000 and GRLS == 'S/U'):
                    row[2] = 2
                elif (domScale == 100000 and GRLS == 'G'):
                    row[2] = 3
                elif (domScale == 100000 and GRLS == 'R'):
                    row[2] = 3
                elif (domScale == 100000 and GRLS == 'L'):
                    row[2] = 2
                elif (domScale == 100000 and GRLS == 'S/U'):
                    row[2] = 1
                elif (domScale == 250000 and GRLS == 'G'):
                    row[2] = 3
                elif (domScale == 250000 and GRLS == 'R'):
                    row[2] = 3
                elif (domScale == 250000 and GRLS == 'L'):
                    row[2] = 2
                elif (domScale == 250000 and GRLS == 'S/U'):
                    row[2] = 1
                elif (domScale >= 500000 and GRLS == 'G'):
                    row[2] = 3
                elif (domScale >= 500000 and GRLS == 'R'):
                    row[2] = 2
                elif (domScale >= 500000 and GRLS == 'L'):
                    row[2] = 1
                elif (domScale >= 500000 and GRLS == 'S/U'):
                    row[2] = 1
                else:
                    row[2] = 0
            else:
                row[2] =0

            #Update Row
            cursor.updateRow(row)

    log.info('Thematic Accuracy Score Asssigned')

def create_grls(output_features, grid, landscan_gpt):
    # Local variables:
    reclass_gpt = os.path.join(env.scratchFolder, "temp_landscan.tif")
    zonal_fifteen_table = os.path.join(env.scratchGDB, 'zonalstats')#"in_memory\\table"
    if arcpy.Exists(zonal_fifteen_table):
        arcpy.Delete_management(zonal_fifteen_table)

    # Process: Copy Features
    log.info("Creating output features: " + output_features)
    arcpy.CopyFeatures_management(grid, output_features, "", "0", "0", "0")

    # Process: Reclassify
    #log.info("reclassifying " + landscan_gpt)
    #arcpy.gp.Reclassify_sa(landscan_gpt, "VALUE", "0 0;1 2;2 2;3 2;4 2;5 2;6 1;7 1;8 1;9 1;10 1", reclass_gpt, "DATA")

    # Process: Zonal Statistics as Table
    log.info("Creating zonal statistics as table: " + zonal_fifteen_table)
    arcpy.gp.ZonalStatisticsAsTable_sa(output_features, "OBJECTID", landscan_gpt, zonal_fifteen_table, "DATA", "ALL")
    zonal_oid = arcpy.Describe(zonal_fifteen_table).OIDFieldName
    # Process: Join Field
    log.info("joiing " + zonal_fifteen_table + " to " + output_features)
    arcpy.JoinField_management(output_features, "OBJECTID",zonal_fifteen_table, zonal_oid, "Count;Area;Min;Max;Range;Variety;Majority;Minority;Median;Mean;Std;Sum")

    log.info("Deleting " + reclass_gpt)
    arcpy.Delete_management(reclass_gpt)

def get_landscan_jenks_score(Grid, fields):
    log.info('Creating dimension value')
    with da.UpdateCursor(Grid, fields) as cursor:
        for row in cursor:
            if row[0] != None:
                if row[0] >= 0:
                    ratio = row[0]
                    if (ratio >= 0 and ratio <= 0.163636):
                        row[1] = "G"
                    elif (ratio > 0.163636 and ratio <= 0.491204):
                        row[1] = "R"
                    elif (ratio > 0.491204 and ratio <= 0.815789):
                        row[1] = "L"
                    elif (ratio > 0.815789 and ratio <= 2.0):
                        row[1] = "S/U"
                    else:
                        row[1] = 0
                else:
                    row[1] = 0
            else:
                row[1] = 0

            # Update the cursor with the updated list
            cursor.updateRow(row)

    log.info('Dimension values populated')

def get_landscan_brunner_score(Grid, fields):
    log.info('Creating dimension value')
    with da.UpdateCursor(Grid, fields) as cursor:
        for row in cursor:
            if row[0] != None:
                if row[0] >= 0:
                    ratio = row[0]
                    if (ratio == 0):
                        row[1] = "G"
                    elif (ratio > 0 and ratio <= 1000):
                        row[1] = "R"
                    elif (ratio > 0.5 and ratio <= 10000):
                        row[1] = "L"
                    elif (ratio > 1.0 and ratio <= 100000):
                        row[1] = "S/U"
                    else:
                        row[1] = 0
                else:
                    row[1] = 0
            else:
                row[1] = 0

            # Update the cursor with the updated list
            cursor.updateRow(row)

    log.info('Dimension values populated')

def get_landscan_custom_score(Grid, fields):
    log.info('Creating dimension value')
    with da.UpdateCursor(Grid, fields) as cursor:
        for row in cursor:
            if row[0] != None:
                if row[0] >= 0:
                    ratio = row[0]
                    if (ratio >= 0 and ratio <= 0.5):
                        row[1] = "G"
                    elif (ratio > 0.5 and ratio <= 0.75):
                        row[1] = "R"
                    elif (ratio > 0.75 and ratio <= 1.0):
                        row[1] = "L"
                    elif (ratio > 1.0 and ratio <= 2.0):
                        row[1] = "S/U"
                    else:
                        row[1] = 0
                else:
                    row[1] = 0
            else:
                row[1] = 0

            # Update the cursor with the updated list
            cursor.updateRow(row)

    log.info('Dimension values populated')


def get_landscan_equal_breaks_score(Grid, fields):
    log.info('Creating dimension value')
    with da.UpdateCursor(Grid, fields) as cursor:
        for row in cursor:
            if row[0] != None:
                if row[0] >= 0:
                    ratio = row[0]
                    if (ratio >= 0 and ratio <= 100):
                        row[1] = "G"
                    elif (ratio > 100 and ratio <= 1000):
                        row[1] = "R"
                    elif (ratio > 1000 and ratio <= 10000):
                        row[1] = "L"
                    elif (ratio > 10000 and ratio <= 200000):
                        row[1] = "S/U"
                    else:
                        row[1] = 0
                else:
                    row[1] = 0
            else:
                row[1] = 0

            # Update the cursor with the updated list
            cursor.updateRow(row)

    log.info('Dimension values populated')

def create_fields(output_features):
    #'Adding fields to table
    fieldLength = 5
    fieldLength_48 = 48
    arcpy.AddField_management(output_features, 'BRUNNER', "TEXT", "", "", fieldLength)
    arcpy.AddField_management(output_features, 'JENKS', "TEXT", "", "", fieldLength)
    arcpy.AddField_management(output_features, 'CUSTOM', "TEXT", "", "", fieldLength)
    arcpy.AddField_management(output_features, 'EQUAL', "TEXT", "", "", fieldLength)
    arcpy.AddField_management(output_features, out_fields[0], "TEXT", "", "", fieldLength_48)
    arcpy.AddField_management(output_features, out_fields[1], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[2], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[3], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[4], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[5], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[6], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[7], "TEXT", "", "", fieldLength)
    arcpy.AddField_management(output_features, out_fields[8], "SHORT", "", "", "")
    arcpy.AddField_management(output_features, out_fields[9], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[10], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[11], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[12], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[13], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[14], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[15], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[16], "LONG", "", "", "")
    arcpy.AddField_management(output_features, out_fields[17], "LONG", "", "", "")

    arcpy.AddField_management(output_features, out_fields[18], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[19], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[20], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[21], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[22], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[23], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[24], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[25], "DOUBLE", "", "", "")
    arcpy.AddField_management(output_features, out_fields[26], "DOUBLE", "", "", "")

def get_scores(output_features):
    log.info('Creating BRUNNER Score')
    get_landscan_brunner_score(output_features, ['MEAN', 'BRUNNER'])
    log.info('Creating JENKS Score')
    get_landscan_jenks_score(output_features, ['MEAN','JENKS'])
    log.info('Creating CUSTOM Score')
    get_landscan_custom_score(output_features, ['MEAN','CUSTOM'])
    log.info('Creating EQUAL Score')
    get_landscan_equal_breaks_score(output_features, ['MEAN','EQUAL'])

def create_output_gdb(output_features):
    outputGDB = os.path.dirname(output_features)
    log.info("Output GDB: " + outputGDB)
    if arcpy.Exists(outputGDB):
        arcpy.AddMessage(outputGDB +  " already exists.")
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

    desc = arcpy.Describe(fc)
    ftype = desc.shapeType

    results = {}
    #objectid in list of objectids
    query = '{0} IN ({1})'.format(arcpy.Describe(output_features).oidFieldName, ",".join(map(str, oids)))
    with da.SearchCursor(output_features, ('OID@', 'SHAPE@'), query) as cursor:
        for row in cursor:
            oid = row[0]
            geometry = row[1]
            arcpy.SelectLayerByLocation_management(features, 'INTERSECT', geometry)
            if int(arcpy.GetCount_management(features)[0]) > 0:

                scale_list = []
                count = 0
                with da.SearchCursor(features, in_fields) as sumCursor:
                    for scamin in sumCursor:
                        scale_list.append(scamin[0])
                        count+=1

                if count > 0:
                    max_val, max_scale = maximum(scale_list)
                    min_val, min_scale = minimun(scale_list)
                    common, common_count = most_common(scale_list)
                    count_2500 = scale_list.count(2500)
                    count_5000 = scale_list.count(5000)
                    count_12500 = scale_list.count(12500)
                    count_25000 = scale_list.count(25000)
                    count_50000 = scale_list.count(50000)
                    count_100000 = scale_list.count(100000)
                    count_250000 = scale_list.count(250000)
                    count_500000 = scale_list.count(500000)
                    count_1000000 = scale_list.count(1000000)
                    result = str(common) + ',' + str(common_count*100.0/count) + ',' + str(min_val) + ',' + str(min_scale*100.0/count) + ',' + str(max_val) + ',' + str(max_scale*100.0/count)+','+\
                        str(count_2500)+','+str(count_5000)+','+str(count_12500)+','+\
                        str(count_25000)+','+str(count_50000)+','+str(count_100000)+','+\
                        str(count_250000)+','+str(count_500000)+','+str(count_1000000)+','+\
                        str(count)
                else:
                    result = default_result

            else:
                result = default_result
            results[oid] = result
    return results

def multiprocess_data(oids, output_features, topo_features):

    partial_process_thematic_accuracy = partial(
        process_data,
        output_features=output_features,
        fc=topo_features
    )

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_process_thematic_accuracy, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results

def update_oputput_fields(output_features, Results):
    with da.UpdateCursor(output_features,  ['OID@'] + list(out_fields)) as cursor:
        for row in cursor:
            if row[0] in Results:
                resultString = Results[row[0]]
                result = resultString.split(',')
                row[1] = get_msp(int(result[0]))
                row[2] = float(result[0])
                row[3] = float(result[1])
                row[4] = float(result[2])
                row[5] = float(result[3])
                row[6] = float(result[4])
                row[7] = float(result[5])

                row[10] = int(result[6])
                row[11] = int(result[7])
                row[12] = int(result[8])
                row[13] = int(result[9])
                row[14] = int(result[10])
                row[15] = int(result[11])
                row[16] = int(result[12])
                row[17] = int(result[13])
                row[18] = int(result[14])
                if float(result[15])>0:
                    row[19] = float(result[6])/float(result[15])
                    row[20] = float(result[7])/float(result[15])
                    row[21] = float(result[8])/float(result[15])
                    row[22] = float(result[9])/float(result[15])
                    row[23] = float(result[10])/float(result[15])
                    row[24] = float(result[11])/float(result[15])
                    row[25] = float(result[12])/float(result[15])
                    row[26] = float(result[13])/float(result[15])
                    row[27] = float(result[14])/float(result[15])
                else:
                    row[19] = 0
                    row[20] = 0
                    row[21] = 0
                    row[22] = 0
                    row[23] = 0
                    row[24] = 0
                    row[25] = 0
                    row[26] = 0
                    row[27] = 0

            cursor.updateRow(row)

def create_thematic_accuracy(landscan_gpt, tds_features, polygon_grid, output_features):

    #start timer
    t0 = time.time()

    ftype = tds_features[-3:]

    log.info("Generating output file.")
    create_output_gdb(output_features)

    log.info("Creating GLRS from Landscan.")
    create_grls(output_features, polygon_grid, landscan_gpt)

    log.info("Creating Fields in Output Feature Class.")
    create_fields(output_features)

    log.info("Ranking Landscan Data.")
    get_scores(output_features)

    oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_data(oids, output_features, tds_features)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = process_data(oids, output_features, tds_features)

    log.info("Updating output fields.")
    update_oputput_fields(output_features, results)

    get_scale_range(output_features, ['EQUAL', out_fields[7]])
    get_thematic_accuracy_score(output_features, ['EQUAL', out_fields[1], out_fields[8]])

    fields_to_keep = list(out_fields)
    lines_to_keep = cm_helper.make_delete_polygons_list(arcpy.ListFields(output_features), fields_to_keep)
    arcpy.DeleteField_management(output_features, lines_to_keep)

    t6 = (time.time() - t0)/60
    log.info("This process took " + str(t6) + " minutes to run the entire process")

    log.info('Done.')
#--------------------------------------------------------------------------
def main(*argv):
    #try:
        #  Inputs
        #
    landscan_gpt = argv[0]
    tds_features = argv[1]
    polygon_grid = argv[2]
    output_features = argv[3]
        #   Logic
        #
    create_thematic_accuracy(landscan_gpt, tds_features,
                                 polygon_grid, output_features)
    # except arcpy.ExecuteError:
    #     line, filename, synerror = trace()
    #     log.error("error on line: %s" % line)
    #     log.error("error in file name: %s" % __file__)
    #     log.error("with error message: %s" % synerror)
    #     log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    # except:
    #     line, filename, synerror = trace()
    #     log.error("error on line: %s" % line)
    #     log.error("error in file name: %s" % __file__)
    #     log.error("with error message: %s" % synerror)
    # finally:
    #     logging.shutdown()
#--------------------------------------------------------------------------
if __name__ == "__main__":
    env.overwriteOutput = True
    argv = (
        r'Population\lspop2013',
        r'TDS\TransportationGroundCrv',
        r'TDS\ResourceSrf',
        r'Test.gdb\TransportationGroundCrv_ThematicAcuracy'
    )
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    main(*argv)
