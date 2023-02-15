"""-----------------------------------------------------------------------------
Name: CreatePositionalOffset_MP.py
Purpose: Determines the positional offset of TDS line features using another
        set of line features as the baseline. This tool also summarizes those results
        at a grid cell level. Uses multiprocessing to improve processing speed.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: September, 2015
Modified: July, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License
-----------------------------------------------------------------------------"""
from __future__ import print_function
# native imports
import os
import sys
import time
import uuid
import shutil
import logging
import multiprocessing
from functools import partial
# 3rd party imports
import arcpy
from arcpy import da
from arcpy import env
import numpy as np
from common import trace, FunctionError, unique_values
import ContentMaturity_Helpers as cm_helper

arcpy.CheckOutExtension('GeoStats')

###########################################################################
module = "CreatePositionalOffset_MP.py"
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

env.overwriteOutput = True

sr = arcpy.SpatialReference(3857)

in_fields = 'zi001_vsn'

def main_task(source_features, comparative_features, source_grid, output_grid, output_features, distance, sample_size):
    # begin timer
    t0 = time.time()

    # setup workspace
    scratch_workspace = env.scratchFolder
    if not os.listdir(scratch_workspace) == '':
        log.info('Cleaning scratch workspace...')
        shutil.rmtree(scratch_workspace)
    if not os.path.exists(scratch_workspace):
        os.mkdir(scratch_workspace)

    #
    #proj_output_features = output_features
    #
# ----------------------
    log.info('Creating output feature class...')
    output_gdb, output_fc_name = create_gdb(output_features)
    create_result_features(source_grid, output_grid)

    # select sample features
    t1 = time.time()
    log.info('Selecting samples...')
    sample_source_features = 'in_memory/sample_features'
    arcpy.SubsetFeatures_ga(source_features,sample_source_features, '', sample_size, "PERCENTAGE_OF_INPUT")
    #sample_source_features = select_feature_sample(source_features, comparative_features, distance, sample_size)
    log.info('Selected {0} sample features...'.format(int(arcpy.GetCount_management(sample_source_features).getOutput(0))))
    log.info('Selection took {0} minutes to run'.format((time.time() - t1) / 60))

    # project features
    log.info('Projecting data to common coordinate system...')
    proj_comparative_features = os.path.join(arcpy.env.scratchGDB, 'proj_comparative_features'+ str(uuid.uuid4().int)[:10])
    print(proj_comparative_features)
    print(output_features)
    arcpy.Project_management(sample_source_features, output_features, sr)
    arcpy.Project_management(comparative_features, proj_comparative_features, sr)

    # # add output fields
    add_fields = [['MEAN_CE90', 'DOUBLE'],['OFFSET_METERS', 'DOUBLE'],['OSM_MATCH', 'TEXT']]
    for field in add_fields:
        if not arcpy.ListFields(output_features, field[0]):
            arcpy.AddField_management(output_features, field[0], field[1])

    # perform comparison
    log.info('Performing positional offset calculations...')
    t2 = time.time()
    sample_oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_positional_offset(sample_oids, output_features, proj_comparative_features, distance)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = positional_offset(sample_oids, output_features, proj_comparative_features, distance)

    # update features with results
    update_with_results(output_features, ['OFFSET_METERS', 'OSM_MATCH'], results)
    log.info('Positional offset calculations took {0} minutes to run'.format((time.time() - t2) / 60))

    # cleanup extra fields
    log.info('Cleaning up output fields...')
    fields_to_keep = [field[0] for field in add_fields] + [in_fields, in_fields.upper()]
    fields_to_delete = cm_helper.make_delete_lines_list(arcpy.ListFields(output_features), fields_to_keep)
    if len(fields_to_delete):
        arcpy.DeleteField_management(output_features, fields_to_delete)

    # project to Equidistcan Cylidrical (world)
    proj_output_features = '{0}/{1}'.format(env.scratchGDB, 'proj_output_features')
    arcpy.Project_management(output_features, proj_output_features, arcpy.SpatialReference(54002))
#----------------------------------------


    # perform grid scoring
    log.info('Performing grid scoring...')
    t3 = time.time()
    grid_oids = get_oids(output_grid)
    if multiprocessing.cpu_count() > 1:
        log.info('Using {0} cores.'.format(multiprocessing.cpu_count()))
        results = multiprocess_score(grid_oids, output_grid, proj_output_features)
    else:
        log.info('Only 1 core available. Not using multiprocessing.')
        results = score(grid_oids, output_grid, proj_output_features)

    # update features with results
    update_with_results(output_grid, ['MEAN_OFFSET_METERS', 'PO_SCORE', 'TIER'], results)
    log.info('Grid scoring took {0} minutes to run'.format((time.time() - t3) / 60))

    # cleanup
    try:
        arcpy.Delete_management(proj_comparative_features)
        arcpy.Delete_management(proj_output_features)
        copy_ce(output_features)
    except:
        print("Cannot delete features")

    # stop timer
    t4 = (time.time() - t0)/60
    log.info('This process took ' + str(t4) + ' minutes to run the entire process')

def copy_ce(out_grid):
    arcpy.CalculateField_management(out_grid, field="MEAN_CE90",
                                    expression="get_ce90(!ZI001_VSN!)",
                                    expression_type="PYTHON",
                                    code_block="def get_ce90(val):\n    try:\n        num = float(val)\n    except:\n        num = -999999\n    return num")
    arcpy.DeleteField_management(out_grid, in_fields)

def create_gdb(output_features):
    output_gdb = os.path.dirname(output_features)
    log.info('Output GDB: ' + output_gdb)
    output_fc_name = os.path.basename(output_features)
    log.info('Output Feature Class: ' + output_fc_name)

    if not arcpy.Exists(output_gdb):
        gdb_location = os.path.split(output_gdb)[0]
        gdb_name = os.path.basename(output_gdb)

        log.info('Creating {0} and {1}'.format(gdb_location, gdb_name))
        arcpy.CreateFileGDB_management(gdb_location, gdb_name)

    return output_gdb, output_fc_name


def create_result_features(source_grid, output_grid):
    arcpy.CopyFeatures_management(source_grid, output_grid)
    arcpy.AddField_management(output_grid, 'MEAN_OFFSET_METERS', 'DOUBLE')
    arcpy.AddField_management(output_grid, 'PO_SCORE', 'SHORT')
    arcpy.AddField_management(output_grid, 'TIER', 'TEXT')


##def get_sample_size(features, sample_size):
##    size = sample_size
##    if size <= 100:
##        feature_count = int(arcpy.GetCount_management(features).getOutput(0))
##        size = round((sample_size / 100.0) * feature_count)
##    return size


##def get_sample_oids(layer, sample_size):
##    oids = get_oids(layer)
##
##    if sample_size < len(oids):
##        return np.random.choice(oids, sample_size)
##    return oids


def get_oids(features):
    oids = []
    with da.SearchCursor(features, ("OID@")) as cursor:
        for row in cursor:
            oids.append(row[0])
    return oids


##def select_feature_sample(source_features, comparative_features,
##                          distance, sample_size):
##    try:
##        # calc sample size
##        sample_size = get_sample_size(source_features, sample_size)
##        # select features within tolerance
##        source_layer = 'source_layer'
##        source_layer = arcpy.MakeFeatureLayer_management(source_features,
##                                                         source_layer)[0]
##        bufferFC = arcpy.Buffer_analysis(comparative_features,
##                                         out_feature_class=r"in_memory\buffer",
##                                         buffer_distance_or_field='{0} meters'.format(distance))[0]
##        arcpy.SelectLayerByLocation_management(source_layer,
##                                               'INTERSECT',
##                                               bufferFC)
##        source_layer = arcpy.CopyFeatures_management(in_features=source_layer,
##                                                     out_feature_class=os.path.join(env.scratchGDB, "seldsd")
##                                                     )[0]
##        # choose samples
##        oids = unique_values(table=source_layer,
##                             field="OID@")
##        if sample_size < len(oids):
##            oids = np.random.choice(oids, sample_size)
##        else:
##            oids = np.array(oids)
##        oid_list = np.sort(oids)
##        query = '{0} IN {1}'.format(arcpy.Describe(source_features).oidFieldName, str(tuple(oid_list)))
##        source_layer = arcpy.MakeFeatureLayer_management(in_features=source_layer,
##                                                         out_layer="a" + uuid.uuid4().get_hex()[:6],
##                                                         where_clause=query)[0]
##        # copy selected sample features
##        sample_features = os.path.join(env.scratchGDB,
##                                       "a" + uuid.uuid4().get_hex()[:15])
##        fc = arcpy.CopyFeatures_management(source_layer,
##                                           sample_features)[0]
##        return fc
##    except:
##        arcpy.AddError([trace(), arcpy.GetMessages(2)])


def build_oid_chunks(oids, num_chunks):
    chunk_size = 1 #(len(oids) + num_chunks - 1) / num_chunks
    chunked_list = [oids[i:i + chunk_size] for i in range(0, len(oids), chunk_size)]
    return chunked_list

def update_with_results(features, fields, results):
    with da.UpdateCursor(features, ['OID@'] + list(fields)) as cursor:
        for row in cursor:
            if row[0] in results:
                for i in range(0, len(fields)):
                    row[i + 1] = results[row[0]][i]
            cursor.updateRow(row)


def multiprocess_positional_offset(oids, source_features, comparative_features, distance):
    # build function with inputs
    partial_positional_offset = partial(positional_offset, source_features=source_features, comparative_features=comparative_features, distance=distance)

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_positional_offset, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results


def positional_offset(oids, source_features, comparative_features, distance):
    comparative_layer = 'comparative_layer_' + str(uuid.uuid4().int)[:10]
    arcpy.MakeFeatureLayer_management(comparative_features, comparative_layer)

    #count = 0
    results = {}
    query = '{0} IN ({1})'.format(arcpy.Describe(source_features).oidFieldName, ",".join(map(str, oids)))
    #query = '{0} IN {1}'.format(arcpy.Describe(source_features).oidFieldName, str(tuple(oids)))
    #print(query)
    with da.SearchCursor(source_features, ('OID@', 'SHAPE@'), query) as cursor:
        # for each source line
        for row in cursor:
            #count = count+1

            oid = row[0]
            if int(oid)%100 == 0:
                arcpy.AddMessage(str(oid))
            source_geometry = row[1]

            try:

                # find its direction
                first_point = arcpy.PointGeometry(source_geometry.firstPoint, sr)
                last_point = arcpy.PointGeometry(source_geometry.lastPoint, sr)
                source_angle = (first_point.angleAndDistanceTo(last_point)[0] + 360) % 360

                # buffer
                source_buffer = source_geometry.buffer(distance)

                # select comparative features
                arcpy.SelectLayerByLocation_management(comparative_layer, 'INTERSECT', source_buffer)

                # perform comparison
                offset_meters = -1
                near_osm = "NO"#0
                with da.SearchCursor(comparative_layer, ("SHAPE@")) as comparative_cursor:
                    for comparative_row in comparative_cursor:
                        comparative_geometry = comparative_row[0]

                        # clip
                        clipped_comparative_geometry = source_buffer.intersect(comparative_geometry, 2)

                        # find direction
                        first_point = arcpy.PointGeometry(clipped_comparative_geometry.firstPoint, sr)
                        last_point = arcpy.PointGeometry(clipped_comparative_geometry.lastPoint, sr)
                        comparative_angle = (first_point.angleAndDistanceTo(last_point)[0] + 360) % 360

                        # test for parallelism
                        if (
                            (comparative_angle <= source_angle + 7.5 and comparative_angle >= source_angle - 7.5) or
                            (comparative_angle <= source_angle + 180 + 7.5 and comparative_angle >= source_angle + 180 - 7.5) or
                            (comparative_angle <= source_angle - 180 + 7.5 and comparative_angle >= source_angle - 180 - 7.5)
                        ):
                            near_osm = "YES"#1
                            near_distance = source_geometry.distanceTo(clipped_comparative_geometry)
                            if offset_meters < 0 or near_distance < offset_meters:
                                offset_meters = near_distance

                # add to results
                results[oid] = [offset_meters, near_osm]
            except:
                results[oid] = [-1, "YES"]

    return results


def multiprocess_score(oids, grid_features, line_features):
    # build function with inputs
    partial_score = partial(score, grid_features=grid_features, line_features=line_features)

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    print(chunked_oids)
    print(partial_score)
    map_results = pool.map(partial_score, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    results = {}
    for result in map_results:
        results.update(result)
    return results


def score(oids, grid_features, line_features):
    line_layer = 'line_layer_' + str(uuid.uuid4().int)[:10]
    arcpy.MakeFeatureLayer_management(line_features, line_layer)

    results = {}
    query = '{0} IN ({1})'.format(arcpy.Describe(grid_features).oidFieldName, ",".join(map(str, oids)))
    with da.SearchCursor(grid_features, ('OID@', 'SHAPE@'), query) as cursor:
        for row in cursor:
            oid = row[0]
            grid_geometry = row[1]

            # select intersecting lines
            arcpy.SelectLayerByLocation_management(line_layer, 'INTERSECT', grid_geometry)

            # build stats
            stats = []
            has_match = False
            with da.SearchCursor(line_layer, ('OFFSET_METERS')) as line_cursor:
                for line_row in line_cursor:
                    has_match = True
                    if line_row[0] > 0:
                        stats.append(line_row[0])

            # calc mean
            if len(stats) > 0:
                mean = np.mean(stats)
            elif has_match:
                mean = -2
            else:
                mean = -1

            # assign score
            if mean > 0:
                if mean >= 0 and mean < 15: # ranges
                    value = 5
                    cat = "Platinum"
                elif mean >= 15 and mean <= 25:
                    value = 4
                    cat = "Gold"
                elif mean > 25 and mean <= 50:
                    value = 3
                    cat = 'Silver'
                elif mean > 50 and mean <= 100:
                    value = 2
                    cat = "Bronze"
                else:
                    value = 1 # TIN
                    cat = "Tin"
            elif mean == -1:
                # no samples
                value = 0
                cat = "No Sample"
            elif mean == -2:
                # no parallel matches
                value = 6
                cat = "No Match"

            results[oid] = [mean, value, cat]
    return results
#--------------------------------------------------------------------------
def main(*argv):
    #try:
        #  Inputs
        #
    comparative_features = argv[0]
    tds_features = argv[1]
    polygon_grid = argv[2]
    output_grid = argv[3]
    #  Local Variables
    #
    output_features = output_grid + "_lines"
    distanceTolerance = 25 #assumes meters
    sample_size = 1 #if 100 or less, is a percent. if 101 or greater, is a number of features
    #   Logic
    #
    pythonExe = os.path.join(sys.exec_prefix, 'python.exe')
    multiprocessing.set_executable(pythonExe)
    multiprocessing.freeze_support = True
    main_task(tds_features, comparative_features,
              polygon_grid, output_grid,
              output_features, distanceTolerance,
              sample_size)

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
    argv = (r'CompFeatures.gdb\TransportationGroundCurves',
        r'TDS\TransportationGroundCrv',
        r'TDS\ResourceSrf',
        r'Test.gdb\TransportationGroundCrv_PositionalOffset')
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    main(*argv)
