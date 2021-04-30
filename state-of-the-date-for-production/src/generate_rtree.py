import arcpy
#import numpy as np
from arcpy import da
import datetime
from libtools import match_fields
from rtree import Rtree, index
from itertools import repeat
#from ast import literal_eval
import os
import sys
import re
import logging

def feature_class_stream(idx, feature_class, fields):
    with da.SearchCursor(feature_class, field_names=fields) as rows:
        for row in rows:
            #try:
            extent = row[0].extent
            oid = row[1]
            geoms = (extent.XMin, extent.XMax, extent.YMin, extent.YMax)
            fields = list(map(str.lower, fields))
            item = dict(zip(fields[2::], row[2::]))
            # print(f"objectid: {oid}, feature geoms {extent.XMin,extent.XMax,extent.YMin,extent.YMax}")
            idx.insert(oid, tuple(map(round, geoms, repeat(12))), item)
            #except:
            #    print('NULL Geometry.')


def main():
    arcpy.env.overwriteOutput = True

    start = datetime.datetime.now()
    print(f"starting at:{start}")
    # can replace with sys.argv[1]

    #Common
    workspace =         sys.argv[1]
    feature_arg =       sys.argv[2]
    rtree_location =    sys.argv[3]
    try:
        sotd_cells =        sys.argv[4]
        resource_srf = sotd_cells
    except:
        resource_srf = None

    # Hard Coded
    fields_to_extract = ['objectid', 'ZI026_CTUU', 'ZI026_CTUC', 'ZI001_SDV', 'ZI001_SDP', 'ZI001_SPS', 'AHA',
                         'ZSAX_RS0', 'ZSAX_RX0']

    log = f"gen_rtree_{feature_arg}.log"
    print(log)
    logging.basicConfig(filename=os.path.join(rtree_location, log), level=logging.INFO)
    logging.info(f"start: {start}")

    # Derived
    rtree_file = os.path.join(rtree_location, feature_arg)
    if not os.path.exists(rtree_location):
        os.makedirs(rtree_location)

    temp_gbd_name = "Extracted.gdb"
    temp_gdb = os.path.join(rtree_location, temp_gbd_name)
    if arcpy.Exists(temp_gdb) == False:
        arcpy.CreateFileGDB_management(rtree_location, temp_gbd_name)



    arcpy.env.workspace = workspace
    arcpy.env.workspace = arcpy.ListDatasets('*TDS')[0]
    print(arcpy.env.workspace)
    print(feature_arg)
    logging.info(arcpy.env.workspace)
    logging.info(feature_arg)

    feature_list = arcpy.ListFeatureClasses("*" + feature_arg + "*")
    print(feature_list)
    logging.info(str(feature_list))
    if len(feature_list) > 1:
        for pattern in feature_list:
            # print(pattern)
            f_name = re.search(feature_arg, pattern)
            if f_name:
                feature_name = pattern
    else:
        feature_name = feature_list[0]

    print(feature_name)
    logging.info(feature_name)
    fields = match_fields(feature_name, fields_to_extract)
    shape = ['SHAPE@']
    cursor_fields = shape + fields

    # Replace a layer/table view name with a path to a dataset (which can be a layer file) or create the layer/table view within the script
    # The following inputs are layers or table views: "TransportationGroundCurves"
    print("Extracting temporary GDB")
    temp_fc_name = os.path.join(temp_gdb, feature_arg)
    logging.info("Extracting features.")
    now = datetime.datetime.now()
    logging.info(f"started at: {now}")

    create_cache = False
    if create_cache:
        try:
            logging.info("Extraction features to temporary feature class in geodatabase.")
            arcpy.FeatureClassToFeatureClass_conversion(in_features=feature_name,
                                                    out_path=temp_gdb,
                                                    out_name=feature_arg,
                                                    field_mapping='zi026_ctuc "Feature Metadata : Cartographic Usability Range <interval closure>" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi026_ctuc,-1,-1;zi026_ctuu "Feature Metadata : Cartographic Usability Range <upper value>" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi026_ctuu,-1,-1;aha "Absolute Horizontal Accuracy (90%)" true true false 8 Double 8 38 ,First,#,TransportationGroundCurves,aha,-1,-1;zi001_sdv "Source Information : Source Date and Time" true true false 20 Text 0 0 ,First,#,TransportationGroundCurves,zi001_sdv,-1,-1;zi001_sdp "Source Information : Source Description" true true false 254 Text 0 0 ,First,#,TransportationGroundCurves,zi001_sdp,-1,-1;zi001_sps "Source Information : Spatial Source" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi001_sps,-1,-1;zsax_rs0 "Restriction Information : Security Attributes Group <resource classification>" true true false 14 Text 0 0 ,First,#,TransportationGroundCurves,zsax_rs0,-1,-1;zsax_rx0 "Restriction Information : Security Attributes Group <resource dissemination controls>" true true false 254 Text 0 0 ,First,#,TransportationGroundCurves,zsax_rx0,-1,-1;st_length_shape_ "st_length_shape_" false true true 0 Double 0 0 ,First,#,TransportationGroundCurves,st_length(shape),-1,-1',
                                                    where_clause="",
                                                    config_keyword="")
                                                    #field_mapping='zi026_ctuc "Feature Metadata : Cartographic Usability Range <interval closure>" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi026_ctuc,-1,-1;zi026_ctuu "Feature Metadata : Cartographic Usability Range <upper value>" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi026_ctuu,-1,-1;aha "Absolute Horizontal Accuracy (90%)" true true false 8 Double 8 38 ,First,#,TransportationGroundCurves,aha,-1,-1;zi001_sdv "Source Information : Source Date and Time" true true false 20 Text 0 0 ,First,#,TransportationGroundCurves,zi001_sdv,-1,-1;zi001_sdp "Source Information : Source Description" true true false 254 Text 0 0 ,First,#,TransportationGroundCurves,zi001_sdp,-1,-1;zi001_sps "Source Information : Spatial Source" true true false 4 Long 0 10 ,First,#,TransportationGroundCurves,zi001_sps,-1,-1;zsax_rs0 "Restriction Information : Security Attributes Group <resource classification>" true true false 14 Text 0 0 ,First,#,TransportationGroundCurves,zsax_rs0,-1,-1;zsax_rx0 "Restriction Information : Security Attributes Group <resource dissemination controls>" true true false 254 Text 0 0 ,First,#,TransportationGroundCurves,zsax_rx0,-1,-1;st_length_shape_ "st_length_shape_" false true true 0 Double 0 0 ,First,#,TransportationGroundCurves,st_length(shape),-1,-1',
        except:
            logging.info("Extraction failed. Generating RTree directly from Enterprise GDB.")
            temp_fc_name = feature_name
    else:
        temp_fc_name = feature_name

    logging.info("Extracting features.")
    now = datetime.datetime.now()
    logging.info(f"finished at: {now}")

    print("Creating RTree.")
    logging.info("Creating RTree.")
    this = Rtree(rtree_file, interleaved=False)

    if resource_srf:
        print("Making feature layer from selecting features.")
        logging.info("Making feature layer from selecting features.")
        selected_resource_srfs = arcpy.MakeFeatureLayer_management(resource_srf, 'sel_fl')
        analysis_feature_layer = arcpy.MakeFeatureLayer_management(temp_fc_name, 'analysis_fl')

        selected_features = arcpy.SelectLayerByLocation_management(analysis_feature_layer, 'INTERSECT',
                                                                   selected_resource_srfs)
        print("Generating Index.")
        logging.info("Generating Index.")
        feature_class_stream(this, selected_features, cursor_fields)

    else:
        print("Generating Index.")
        logging.info("Generating Index")
        feature_class_stream(this, temp_fc_name, cursor_fields)

    try:
        arcpy.Delete_management(temp_fc_name)
        print(f"Deleted: {temp_fc_name}")
    except:
        logging.info("Delete Failed")

    finish = datetime.datetime.now()
    print(f"finished at: {finish}")
    logging.info(f"finished at: {finish}")
    print(f"runtime: {finish - start}")
    logging.info(f"runtime: {finish - start}")
    logging.shutdown()


if __name__ == '__main__':
    main()
