#import numpy as np
import pandas as pd
import arcpy
import os
import sys
#import datetime
from collections import Counter
from arcgis import GeoAccessor as ga
from libtools import *
from rtree import Rtree
import utilities
import logging
import pickle


def represents_float(s):
    try:
        return float(s)
    except ValueError:
        return -1

def intersect(bbox, tree, column):
    xmin,ymin,xmax,ymax = bbox
    interleaved = (xmin,xmax,ymin,ymax)
    try:
        bucket = list(tree.intersection((bbox), objects='raw'))
    except Exception as e:
        bucket = list(tree.intersection((interleaved), objects="raw"))
    return [v for v in bucket for k, v in v.items() if k == column]


def get_lc_answers(err, attr):
    count = len(err)
    if count > 0:
        mean_err = round(np.mean(err),1)
        med_err = np.median(err)
        min_err = np.min(err)
        max_err = np.max(err)
        primary, primary_count, secondary, secondary_count = most_common_lc_val(err)
        if secondary == 'N/A':
            secondary = -1
        lc_score = get_lc_score(primary)
        #primary_percent = round(primary_count*100.0/count,1)
        #secondary_percent = round(secondary_count*100.0/count,1)
        if mean_err >0:
            pri_attr, pri_attr_count, sec_attr, sec_attr_count = most_common_lc_val(attr)
        else:
            pri_attr = 'N/A'
            sec_attr = 'N/A'
            pri_attr_count = 0
            sec_attr_count = 0
            #primary_count = 0
            #secondary_count = 0
    else:
        mean_err = -1
        med_err = -1
        min_err = -1
        max_err = -1
        primary = -1
        secondary = -1
        pri_attr = 'N/A'
        sec_attr = 'N/A'
        pri_attr_count = 0
        sec_attr_count = 0
        primary_count = 0
        secondary_count = 0
        lc_score = 0

    return mean_err, med_err, min_err, max_err, \
           primary, secondary, \
           primary_count, secondary_count,\
           pri_attr, sec_attr, \
           pri_attr_count, sec_attr_count, \
           lc_score


def most_common_lc_val(lst):

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
        sec_src = 'N/A'
        sec_count = 0

    return prime_src, prime_count, sec_src, sec_count


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


def lc_stats(bbox, tree, val_counts, field_vals):
    all_val_counts = [represents_float(v) for v in intersect(bbox, tree, val_counts)]
    all_field_vals = [v for v in intersect(bbox, tree, field_vals)]
    sep_field_vals = ','.join([str(elem) for elem in all_field_vals])
    sep_field_vals = sep_field_vals.split(',')

    mean_err, med_err, min_err, max_err, \
        primary, secondary, \
        primary_count, secondary_count, \
        pri_attr, sec_attr, \
        pri_attr_count, sec_attr_count, \
        lc_score = get_lc_answers(all_val_counts, sep_field_vals)

    return mean_err, med_err, min_err, max_err, \
            primary, secondary, \
            primary_count, secondary_count, \
            pri_attr, sec_attr, \
            pri_attr_count, sec_attr_count, \
            lc_score

def main():
    """
    :usage given a tree on disk aggregate values using pandas
    verify column casing in the rtree was loaded as upper or lower or as-is from the feature class
    from pwd build rtree, and load rtree
    """
    pd.set_option('display.max_columns', None)
    # TODO : add logging
    start = datetime.datetime.now()
    # feature = sys.argv[1]

    #Common
    workspace = sys.argv[1] #r'editor.sde'
    features = sys.argv[2] #"TransportationGroundCrv"
    idx_location = sys.argv[3] #r"RESULTS\RTREE"
    out_location = sys.argv[4] #r"StateOfTheData.gdb"
    try:
        update_flag = bool(sys.argv[5])
        update_status = "Update"
    except:
        update_flag = False
        update_status = "Initial"


    log = f"proc_lc_rtree_{features}.log"
    print(os.path.join(idx_location, log))
    print(f"start: {start}")
    logging.basicConfig(filename=os.path.join(idx_location, log), level=logging.INFO)
    logging.info(f"start: {start}")
    logging.info(log)

    if arcpy.Exists(out_location):
        print(out_location +  " already exists.")
        logging.info(out_location +  " already exists.")
    else:
        print("Creating " + os.path.split(out_location)[0] + " and " + os.path.basename(out_location))
        logging.info("Creating " + os.path.split(out_location)[0] + " and " + os.path.basename(out_location))
        arcpy.CreateFileGDB_management(os.path.split(out_location)[0], os.path.basename(out_location))

    #Hard coded
    checklist = ['PSG', 'HADR']#, 'MTM', 'Routing', 'LOC']

    classification_fc_name = features + '_' + 'Classification'
    classification_fc = os.path.join(out_location, classification_fc_name)
    classification_fields =  ['classification', 'caveat'] # ['objectid', 'classification', 'caveat']
    classification_df = ga.from_featureclass(classification_fc, fields=classification_fields)
    #print(classification_df.head())

    # tree name ? -> just use feature as name ?
    # workspace code is awful
    # provided a path to a gdb and the expectation that it is written as a dataset that
    # is within the gdb i.e. 1 level deep, the following will work.
    default_fields = ['globalid', 'f_code', 'OBJECTID']
    # bin feature class for aggregating vector into [][] planar shapes

    filename = 'ResourceSrf.p'
    resource_cache = os.path.join(idx_location, filename)
    if os.path.isfile(resource_cache):
        print("Reading ResourceSrf from Cache")
        logging.info("Reading ResourceSrf from Cache")
        resourcesrf_df = pickle.load(open(resource_cache, "rb"))
    else:
        if update_flag:
            arcpy.env.workspace = out_location
            print(arcpy.env.workspace)
            logging.info(arcpy.env.workspace)
            resource_arg = arcpy.ListFeatureClasses('*cells_to_update*')[0]
        else:
            arcpy.env.workspace = workspace
            arcpy.env.workspace = arcpy.ListDatasets('*TDS')[0]
            print(arcpy.env.workspace)
            logging.info(arcpy.env.workspace)
            resource_arg = arcpy.ListFeatureClasses("*Resource*")[0]

        print(resource_arg)
        logging.info(resource_arg)

        # match fields takes 2 args :list type expected and matches the elements of the list from the second
        # arg that best match the first arg
        fields = match_fields(resource_arg, default_fields)
        logging.info(f"fields used in the dataframe: {fields}")

        print("Reading ResourceSrf from DB")
        logging.info("Reading ResourceSrf from DB")
        resourcesrf_df = ga.from_featureclass(resource_arg, fields=fields)
        print("Writing ResourceSrf to Cache")
        logging.info("Writing ResourceSrf to Cache")
        pickle.dump(resourcesrf_df, open(resource_cache, "wb"))

    #print(resourcesrf_df.head())

    # Rtree load, must exist on file if it doesnt it could make an empty file and fail later.
    # add configuration for sys.argv[2] for .bat to load various rtree indexs

    print('Merging in Classification.')
    logging.info('Merging in Classification.')
    try:
        classification_df = classification_df[classification_df.columns.difference(['SHAPE'])]
    except:
        classification_df = classification_df[classification_df.columns.difference(['shape'])]

    try:
        init_df = pd.merge(classification_df, resourcesrf_df, left_index=True, right_index=True)
        # init_df = pd.merge(classification_df, resourcesrf_df, on='objectid',  how='inner')
    except:
        init_df = pd.merge(classification_df, resourcesrf_df, on='objectid', how='inner')

    print("Updating dataframe.")
    logging.info("Updating dataframe.")
    #df.update(full_df)
    #print(init_df.head())

    #Derived
    for handle in checklist:
        df = init_df.copy()
        #handle = 'HADR'
        handle = handle.lower().replace(' ', '_').replace('-', '_')
        index_file = os.path.join(idx_location, features + "_" +handle + "_Index")


        lc_fc_name = features+'_'+handle
        lc_fc = os.path.join(out_location, lc_fc_name)

        tree = Rtree(index_file, objects="raw")
        print('Rtree loaded.')
        logging.info('Rtree loaded.')

        print("Generating Extent.")
        logging.info("Generating Extent.")
        df['extent'] = df.apply(lambda x: x['SHAPE'].extent, axis=1)
        #df['extent'] = df.apply(lambda x: utilities.custom_buf(x['SHAPE']), axis=1)
        df['extent'].head()

        print("Counting features per cell.")
        logging.info("Counting features per cell.")
        df['feature_cnt'] = df.apply(lambda x: tree.count(x['extent']), axis=1)
        finish = datetime.datetime.now()
        print(f"Finished counting features at: {finish}")
        logging.info(f"Finished counting features at: {finish}")



        proc_lc = True
        if proc_lc:
            print("Processing " + handle + " Logical Consistency.")
            logging.info("Processing " + handle + " Logical Consistency.")

            utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', lc_fc_name, update_status,
                                          "Started")

            lc_fields = ['field_list', 'deficiency_cnt']

            df['mean_def_cnt'], df['median_def_cnt'], df['min_def_cnt'], df['max_def_cnt'], \
                df['pri_num_def'], df['sec_num_def'], \
                df['pri_num_def_cnt'], df['sec_num_def_cnt'],\
                df['pri_attr_def'], df['sec_attr_def'], \
                df['pri_attr_def_cnt'], df['sec_attr_def_cnt'], \
                df['lc_score']= zip(*df.apply(lambda x: lc_stats(x['extent'], tree, lc_fields[1], lc_fields[0]), axis=1))

            df['per_pri'] = (((df['pri_num_def_cnt'] * 100 / df['feature_cnt']).fillna(0)).round(2)).replace(np.inf,
                                                                                                                   0).replace(
                -np.inf, 0)
            df['per_sec'] = (((df['sec_num_def_cnt'] * 100 / df['feature_cnt']).fillna(0)).round(2)).replace(np.inf,
                                                                                                                   0).replace(
                -np.inf, 0)
            df['pri_attr_def_per'] = ((df['pri_attr_def_cnt'] * 100 / df['feature_cnt']).fillna(0)).round(
                2).replace(np.inf, 0).replace(-np.inf, 0)
            df['sec_attr_def_per'] = ((df['sec_attr_def_cnt'] * 100 / df['feature_cnt']).fillna(0)).round(
                2).replace(np.inf, 0).replace(-np.inf, 0)


        df = df[df.columns.difference(['extent'])]

        logging.info(f"Logical Consistency output {df.head(3)}")
        processing = datetime.datetime.now()
        logging.info(f"processor time: {processing - start}")
        print(f"process time for data: {processing - start}")
        print("writing feature")
        # should we put things where we found them ?
        #df.spatial.to_featureclass(lc_fc, overwrite=True)
        if update_flag:
            logging.info("Inserting new logical consistency features.")
            print(df.head())
            lc_fc = utilities.insert_new_features(df, lc_fc, out_location, 'logc')
            print(lc_fc)
            logging.info(lc_fc)
        else:
            logging.info("Overwriting logical consistency features.")
            df.spatial.to_featureclass(lc_fc, overwrite=True)
            print(lc_fc)
            logging.info(lc_fc)

        del df

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', lc_fc_name, update_status,
                                      "Completed")

    finish = datetime.datetime.now()
    logging.info(f"finish: {finish}, runtime: {finish-start} ")
    print(f"finish: {finish}")
    print(f"runtime: {finish - start}")
    logging.shutdown()


if __name__ == '__main__':
    main()
