"""-----------------------------------------------------------------------------
Name: CreateSourceLineage_MP.py
Purpose: Creates the source lineage quality element from TDS 6.1 features.
        Uses multiprocessing to reduce processing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: October, 2015
Modified: July, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License
-----------------------------------------------------------------------------"""
from __future__ import print_function
# native imports
from collections import Counter
import multiprocessing
import os
import sys
from functools import partial
import time
import uuid
import logging
# 3rd party imports
import arcpy
from arcpy import env
from arcpy import da

from common import FunctionError, trace
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################
env.overwriteOutput = True

SUMMARIZE_ALL = True
source_fields = ('zi001_sdp', 'zi001_sps')
base_fields =(('SOURCE_LIST',  'TEXT',   '', '', 1024, 'SOURCE_LIST',  '',         ''),
    ('PRI_SOURCE', 'TEXT',   '', '', 256,  'PRI_SOURCE', '',         ''),
    ('PRI_SOURCE_CNT', 'LONG',   2,  '', '',   'PRI_SOURCE_CNT', 'NULLABLE', 'REQUIRED'),
    ('PRI_SOURCE_PER', 'DOUBLE', '', '', '',   'PRI_SOURCE_PER', '',          ''),
    ('SEC_SOURCE',   'TEXT',   '', '', 256,  'SEC_SOURCE',   '',          ''),
    ('SEC_SOURCE_CNT',   'LONG',   2,  '', '',   'SEC_SOURCE_CNT',   'NULLABLE', 'REQUIRED'),
    ('SEC_SOURCE_PER',   'DOUBLE', '', '', '',   'SEC_SOURCE_PER',   '',         ''))
fields = []
for prefix in ['']:
#['PNT', 'CRV', 'SRF']:
    for field in base_fields:
        fields.append(list(field))
        fields[-1][0] = prefix + field[0]
        fields[-1][5] = prefix + field[5]

def main_task(source_features, source_grid, output_features):
    # start timer
    t0 = time.time()

    # initialize source paths
    point_source_features = build_source_feature_path(source_features, 'Pnt')
    line_source_features = build_source_feature_path(source_features, 'Crv')
    polygon_source_features = build_source_feature_path(source_features, 'Srf')

    # count features
    pnt_count = get_feature_count(point_source_features)
    log.info('Number of Point Features: {0}'.format(pnt_count))
    crv_count = get_feature_count(line_source_features)
    log.info('Number of Line Features: {0}'.format(crv_count))
    srf_count = get_feature_count(polygon_source_features)
    log.info('Number of Polygon Features: {0}'.format(srf_count))

    # create output
    log.info('Creating output feature class...')
    create_output_features(source_grid, output_features)

    # log feature creation process
    t1 = time.time()
    log.info('This process took {0} minutes to run.'.format((t1 - t0) / 60))

    log.info('Performing source lineage calculations...')
    oids = get_oids(output_features + '_Crv')
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_source_lineage(oids, output_features+'_Crv', point_source_features, line_source_features, polygon_source_features)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = process_source_lineage(oids, output_features+'_Crv', point_source_features, line_source_features, polygon_source_features)

    # update features with results
    global fields
    field_names = [field[0] for field in fields]
    #print(results)
    update_with_results(output_features + '_Pnt', field_names, results, 0)
    update_with_results(output_features + '_Crv', field_names, results, 1)
    update_with_results(output_features + '_Srf', field_names, results, 2)

    t2 = time.time()
    log.info('This process took {0} minutes to run the entire process'.format((t2 - t0) / 60))


def build_source_feature_path(source_features, suffix):
    return source_features[:-3] + suffix


def get_feature_count(features):
    return int(arcpy.GetCount_management(features).getOutput(0))


def get_oids(features):
    oids = []
    with da.SearchCursor(features, ('OID@')) as cursor:
        for row in cursor:
            oids.append(row[0])
    return oids


def build_oid_chunks(oids, num_chunks):
    chunk_size = 1#(len(oids) + num_chunks - 1) / num_chunks
    chunked_list = [oids[i:i + chunk_size] for i in range(0, len(oids), chunk_size)]
    return chunked_list


def create_output_features(source_grid, output_features):
    # create output gdb
    crv_features = output_features + '_Crv'
    pnt_features = output_features + '_Pnt'
    srf_features = output_features + '_Srf'

    output_gdb = os.path.dirname(output_features)
    if not arcpy.Exists(output_gdb):
        gdb_location = os.path.split(output_gdb)[0]
        gdb_name = os.path.basename(output_gdb)

        log.info('Creating {0} and {1}'.format(gdb_location, gdb_name))
        arcpy.CreateFileGDB_management(gdb_location, gdb_name)

    if not arcpy.Exists(crv_features):
        arcpy.CopyFeatures_management(source_grid, crv_features)
    if not arcpy.Exists(pnt_features):
        arcpy.CopyFeatures_management(source_grid, pnt_features)
    if not arcpy.Exists(srf_features):
        arcpy.CopyFeatures_management(source_grid, srf_features)

    for field in fields:
        if not arcpy.ListFields(pnt_features, field[0]):
            arcpy.AddField_management(pnt_features, *field)

    for field in fields:
        if not arcpy.ListFields(crv_features, field[0]):
            arcpy.AddField_management(crv_features, *field)

    for field in fields:
        if not arcpy.ListFields(srf_features, field[0]):
            arcpy.AddField_management(srf_features, *field)


def multiprocess_source_lineage(oids, output_features, point_features, line_features, polygon_features):
    # build function with inputs
    partial_process_source_lineage = partial(
        process_source_lineage,
        output_features=output_features,
        point_features=point_features,
        line_features=line_features,
        polygon_features=polygon_features
    )

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_process_source_lineage, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results


def process_source_lineage(oids, output_features, point_features=None, line_features=None, polygon_features=None):
    process_uuid = uuid.uuid4().hex
    point_layer = 'point_layer_' + process_uuid
    if point_features:
        arcpy.MakeFeatureLayer_management(point_features, point_layer)

    line_layer = 'line_layer_' + process_uuid
    if line_features:
        arcpy.MakeFeatureLayer_management(line_features, line_layer)

    polygon_layer = 'polygon_layer_' + process_uuid
    if polygon_features:
        arcpy.MakeFeatureLayer_management(polygon_features, polygon_layer)

    results = {}
    #objectid in list of objectids
    query = '{0} IN ({1})'.format(arcpy.Describe(output_features).oidFieldName, ",".join(map(str, oids)))
    #print(query)
    with da.SearchCursor(output_features, ('OID@', 'SHAPE@'), query) as cursor:
        for row in cursor:
            oid = row[0]
            geometry = row[1]

            result = []
            if point_features:
                result += source_lineage(geometry, point_layer)
            else:
                result += ['None', 'None', 0, 0, 'None', 0, 0]

            if line_features:
                result += source_lineage(geometry, line_layer)
            else:
                result += ['None', 'None', 0, 0, 'None', 0, 0]

            if polygon_features:
                result += source_lineage(geometry, polygon_layer)
            else:
                result += ['None', 'None', 0, 0, 'None', 0, 0]

            # add to results
            results[oid] = result

    return results


def source_lineage(geometry, source_layer):
    # select intersecting features
    arcpy.SelectLayerByLocation_management(source_layer, 'INTERSECT', geometry)

    # gather values
    srts = []
    sdps = []
    total_count = 0
    with da.SearchCursor(source_layer, source_fields) as rows:
        for row in rows:
            total_count += 1
            if row[1] == 1001:
                sdps.append(row[0])

##    # get srt most common value
##    if len(srts) > 0:
##        srt_counter = Counter(srts)
##        primary_srt_val = srt_counter.most_common(1)[0][0]
##        srt_list = ','.join(set(srts))
##    else:
##        primary_srt_val = 'None'
##        srt_list = 'None'

    # get sdp most common values
    if len(sdps) > 0:
        sdp_list = ','.join(set(sdps))
        sdp_counter = Counter(sdps)
        sdp_most_common = sdp_counter.most_common(2)
        primary_sdp_val = sdp_most_common[0][0]
        primary_sdp_count = sdp_most_common[0][1]
        primary_sdp_perc = primary_sdp_count * 100.0 / total_count
        if len(sdp_most_common) > 1:
            secondary_sdp_val = sdp_most_common[1][0]
            secondary_sdp_count = sdp_most_common[1][1]
            secondary_sdp_perc = secondary_sdp_count * 100.0 / total_count
        else:
            secondary_sdp_val = 'None'
            secondary_sdp_count = 0
            secondary_sdp_perc = 0
    else:
        sdp_list = 'None'
        primary_sdp_val = 'None'
        primary_sdp_count = 0
        primary_sdp_perc = 0
        secondary_sdp_val = 'None'
        secondary_sdp_count = 0
        secondary_sdp_perc = 0

    return [sdp_list, primary_sdp_val, primary_sdp_count, primary_sdp_perc, secondary_sdp_val, secondary_sdp_count, secondary_sdp_perc]


def update_with_results(features, fields, results, start_ind):
    with da.UpdateCursor(features, ['OID@'] + list(fields)) as cursor:
        for row in cursor:
            if row[0] in results:
                for i in range(0, len(fields)):
                #for i in xrange(6*start_ind,6*(start_ind+1)):
                    #log.info('i = ' + str(i))
                    #print(results[row[0]][i])
                    row[i + 1] = results[row[0]][i+start_ind*7]
                cursor.updateRow(row)
#--------------------------------------------------------------------------
def main(*argv):
    try:
        #  Inputs
        #
        tds_features = argv[0]
        polygon_grid =  argv[1]
        output_features = argv[2]
        #   Logic
        #
        main_task(tds_features, polygon_grid, output_features)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
        log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
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
            r'Test.gdb\TransportationGroundCrv_SourceLineage')

    main(*argv)
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    #main(*argv)