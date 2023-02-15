"""-----------------------------------------------------------------------------
Name: CreateCompleteness_MP.py
Purpose: Creates the completness indicator from two caomparable feature
        classes. Uses multiprocessing to reduce the processing time.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 8, 2015
Modified: July, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License

-----------------------------------------------------------------------------"""
from __future__ import print_function
from __future__ import division
import six
#Native imports
import multiprocessing
import logging
from common import FunctionError, trace
from functools import partial
import time
import os
import sys
import uuid
import math

#3rd party imports
import arcpy
from arcpy import env
from arcpy import da
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################
env.overwriteOutput = True
def get_score(baseVal, inputVal):
    if inputVal > 0:
        ratio = baseVal/inputVal
        if (ratio >= 0 and ratio <= 0.5):
            result = 1
        elif (ratio > 0.5 and ratio <= 0.75):
            result = 2
        elif (ratio > 0.75 and ratio <= 1.25):
            result = 3
        elif (ratio > 1.25 and ratio <= 1.5):
            result = 4
        elif (ratio > 1.5):
            result = 5
        else:
            result = 0
    else:
        if baseVal > 0:
            result = 5
        else:
            result = 0

    return result

def get_oids(features):
    oids = []
    with da.SearchCursor(features, ('OID@')) as cursor:
        for row in cursor:
            oids.append(row[0])
    return oids

def build_oid_chunks(oids, num_chunks):
    chunk_size = int(math.floor((len(oids) + num_chunks - 1) / num_chunks))
    chunked_list = [oids[i:i + chunk_size] for i in six.moves.range(0, len(oids), chunk_size)]
    return chunked_list

#Processes the data to get the cumulative road length per cell
def process_data(oids, output_features, fc):
    if sys.version_info.major == 3:
        process_uuid = uuid.uuid4().hex
    else:
        process_uuid = uuid.uuid4().get_hex()
    features_layer = "features_" + process_uuid
    features = arcpy.MakeFeatureLayer_management(fc,features_layer,"#","#","#")

    #Get feature type
    desc = arcpy.Describe(fc)
    ftype = desc.shapeType
    #print(ftype)

    if ftype == 'Point':
        feature_type = "POINT"
        sr =  arcpy.SpatialReference(4326) #WGS84 for points
    if ftype == 'Polyline':
        feature_type = "POLYLINE"
        field = 'SHAPE@LENGTH'
        sr =  arcpy.SpatialReference(54002) #Equidistcan Cylidrical (world)
    if ftype == 'Polygon':
        feature_type = "POLYGON"
        field = 'SHAPE@AREA'
        sr =  arcpy.SpatialReference(54034) #Cylindrical Equal Area (world) meters

    results = {}
    #objectid in list of objectids
    query = '{0} IN ({1})'.format(arcpy.Describe(output_features).oidFieldName, ",".join(map(str, oids)))
    with da.SearchCursor(output_features, ('OID@', 'SHAPE@'), query) as cursor:
        for row in cursor:
            oid = row[0]
            geometry = row[1]
            #print(oid)

            try:
                arcpy.Clip_analysis(features, geometry, "in_memory\\output_" + process_uuid)
            except:
                result = str(oid) + ", " + str(0.0)
                results[oid] = result
                return results

            num_features = arcpy.GetCount_management("in_memory\\output_" + process_uuid)[0]
            if ftype != 'Point':
                #print('Point')
                if int(num_features) > 0:
                    count = 0.0
                    with da.SearchCursor("in_memory\\output_" + process_uuid,'SHAPE@', spatial_reference=sr) as cursorRoad:
                        for rowRoad in cursorRoad:
                            geometry = cursorRoad[0]
                            if geometry:
                            #print(rowRoad[0])
                                if ftype == 'Polyline':
                                    count = count + geometry.getLength('PLANAR','KILOMETERS') #rowRoad[0]/1000
                                    #print(count)
                                if ftype == 'Polygon':
                                    count = count + geometry.getArea('PLANAR','SQUAREKILOMETERS') #rowRoad[0]/1000000
                                    #print(count)

                    result = str(oid) + ", " + str(count)

                else:
                    count = 0.0
                    result = str(oid) + ", " + str(count)
            else:
                count = 0.0
                result = str(oid) + ", " + str(num_features)

            #print(result)
            arcpy.Delete_management("in_memory\\output_" + process_uuid)
            results[oid] = result

    return results

#Opens the multiprocessing pool
#Multiprocesses the data
def multiprocess_data(oids, output_features, topo_features):

    partial_process_completeness = partial(
        process_data,
        output_features=output_features,
        fc=topo_features
    )

    # open the pipes
    pool = multiprocessing.Pool()

    # start multiprocessing
    num_chunks = 4 * multiprocessing.cpu_count()
    chunked_oids = build_oid_chunks(oids, num_chunks)
    map_results = pool.map(partial_process_completeness, chunked_oids)

    # close the pool
    pool.close()
    pool.join()

    # flatten results
    results = {}
    for result in map_results:
        results.update(result)
    return results

#Creates the GDB, FC, and Fields in the FC for the output data
def create_output_features(grid, out_fc, fields):
    #Creating output GDB and feature class
    outputGDB = os.path.dirname(out_fc)
    print("Output GDB: " + outputGDB)
    outputFC = os.path.basename(out_fc)
    print("Output Feature Class: " + outputFC)
    outputGrid = os.path.join(outputGDB, outputFC)

    print('Create output Grid feature class')
    if arcpy.Exists(outputGDB):
        print(outputGDB +  " already exists.")
    else:
        print("Creating " + os.path.split(outputGDB)[0] + " and " + os.path.basename(outputGDB))
        arcpy.CreateFileGDB_management(os.path.split(outputGDB)[0], os.path.basename(outputGDB))

    if arcpy.Exists(outputGDB + "\\" + outputFC):
        print(outputGDB + "\\" + outputFC + " feature class already exists.")
    else:
        print('Creating ' + outputGrid)
        arcpy.CopyFeatures_management(grid, out_fc)
        print('Creating few fields: ' + fields[0] + ', ' + fields[1] + ', ' + fields[2] + ', ' + fields[4])
        arcpy.AddField_management(out_fc, fields[0], "DOUBLE", "", "", "")
        arcpy.AddField_management(out_fc, fields[1], "DOUBLE", "", "", "")
        arcpy.AddField_management(out_fc, fields[4], "DOUBLE", "", "", "")
        arcpy.AddField_management(out_fc, fields[2], "SHORT", "", "", "")

#Updates the 3 fields in the outpur FC with the calculated values
def update_output_fields(outputFeatureClass, baselineResults, inputResults, fields):
    with da.UpdateCursor(outputFeatureClass, fields[0:5]) as cursor:
        for row in cursor:
            if row[3] in baselineResults:
                baselineString = baselineResults[row[3]]
                baselineAns = baselineString.split(',')
                inputString = inputResults[row[3]]
                inputAns = inputString.split(',')
                row[0] = float(inputAns[1])
                row[1] = float(baselineAns[1])
                row[2] = get_score(row[0], row[1])
                row[4] = float(inputAns[1])-float(baselineAns[1])
                cursor.updateRow(row)

#Runs all the functions to create the completeness score
#comparative_features, tds_features, grid_polygons, output_features
def create_completeness(comparative_features, tds_features, grid_polygons, output_features):

    fields = ('TDS_DENSITY','COMP_DENSITY','COMPLETENESS_VALUE', 'OBJECTID', 'DIFFERENCE')

    #Redefine this
    country = os.path.basename(grid_polygons)

    #Creates the output features
    print('Creating output features')
    create_output_features(grid_polygons, output_features, fields)

    #start timer
    t0 = time.time()

    oids = get_oids(output_features)
    if multiprocessing.cpu_count() > 1:
        print('Using {0} cores.'.format(multiprocessing.cpu_count()))
        print('Processing comparative features.')
        comparative_results = multiprocess_data(oids, output_features, comparative_features)
        print('Processing TDS features.')
        your_results = multiprocess_data(oids, output_features, tds_features)
    else:
        print('Processing comparative features.')
        comparative_results = process_data(oids, output_features, comparative_features)
        print('Processing TDS features.')
        your_results = process_data(oids, output_features, tds_features)

    print("Updating output fields.")

    #print(comparative_results)
    #print("-----------")
    #print(your_results)

    #End timer
    t1 = (time.time() - t0)/60
    print("This process took " + str(t1) + " minutes to run")

    #Updates the output fields
    print('Updating the output fields')
    update_output_fields(output_features, comparative_results, your_results, fields)

    print('Done.')
if __name__ == '__main__':
    try:
        comparative_features = r'CompFeatures.gdb\TransportationGroundCurves'
        tds_features = r'TDS\TransportationGroundCrv'
        polygon_grid = r'TDS\ResourceSrf'
        output_features = r'Test.gdb\TransportationGroundCrv_Completeness'
            #comparative_features = arcpy.GetParameterAsText(0)
            #tds_features = arcpy.GetParameterAsText(1)
            #polygon_grid = arcpy.GetParameterAsText(2)
            #output_features = arcpy.GetParameterAsText(3)
        create_completeness(comparative_features, tds_features,
                                polygon_grid, output_features)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        msg = "error on line: %s" % line
        msg += "\n error in file name: %s" % filename
        msg += "\n with error message: %s" % synerror
        msg += "\n ArcPy Error Message: %s" % arcpy.GetMessages(2)
        log.error(msg)
        arcpy.AddError(msg)
    except FunctionError as f_e:
        messages = f_e.args[0]
        msg = "error in function: %s" % messages["function"]
        msg += "\n error on line: %s" % messages["line"]
        msg += "\n error in file name: %s" % messages["filename"]
        msg += "\n with error message: %s" % messages["synerror"]
        msg += "\n ArcPy Error Message: %s" % messages["arc"]
        log.error(msg)
        arcpy.AddError(msg)
    except:
        line, filename, synerror = trace()
        msg = "error on line: %s" % line
        msg += "\n error in file name: %s" % filename
        msg += "\n with error message: %s" % synerror
        log.error(msg)
        arcpy.AddError(msg)
    finally:
        logging.shutdown()
