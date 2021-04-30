#import numpy as np
import pandas as pd
import arcpy
#from arcgis.features import SpatialDataFrame
import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
#from dateutil.tz import tzutc
from collections import Counter
from arcgis import GeoAccessor as ga
from libtools import *
from rtree import Rtree
from itertools import repeat
import logging
#import sys
import os
import sys
import temporal_accuracy as ta
import utilities
import pickle

arcpy.env.overwriteOutput = True

empty = (-999999, '', None, 'noInformation','None', 'Null', 'NULL', -999999.0)
default_date = '1901-1-1'
class_codes = {
    "class_dict":{
        "U": 1,
        "S":2,
        "TS":3
    },
    "caveat_dict":{
        "noInformation": 1,
        "FO": 2,
        "DS": 3
    }
}


def temporal_accuracy(change_features, curr_features, output_features, years):
    print('Running Temporal Accuracy from Currency FC')
    feature_class = curr_features

    temp_acc_calc = ta.TemporalAccuracy(change_features, feature_class, output_features, years)

    temp_acc_calc.create_temporal_accuracy()

def intersect(bbox, tree, column):
    xmin,ymin,xmax,ymax = bbox
    interleaved = (xmin,xmax,ymin,ymax)
    try:
        bucket = list(tree.intersection((bbox), objects='raw'))
    except Exception as e:
        bucket = list(tree.intersection((interleaved), objects="raw"))
    return [v for v in bucket for k, v in v.items() if k == column]


def represents_float(s):
    try:
        return float(s)
    except ValueError:
        return -1

def represents_int(s):
    try:
        return int(s)
    except ValueError:
        return -1

def stats(bbox, tree, column):
    all_values = [represents_float(v) for v in intersect(bbox, tree, column)]

    null_count = all_values.count(-1)
    values = [x for x in all_values if x != -1]

    mean = np.mean(values) if len(values) > 0 else 0
    median = np.median(values) if len(values) > 0 else 0
    mode = [m for m, o in Counter(values).most_common(1)][0] if len(values) > 0 else 0
    min_val = np.min(values) if len(values) > 0 else 0
    max_val = np.max(values) if len(values) > 0 else 0
    return mean, median, str(mode), min_val, max_val, null_count


def convert_date(string):
    try:
        return parse(string)
    except:
        return parse('1/1/1901')

def coerce_dates(bbox, tree, column):
    default = datetime.datetime(1901,1,1,0,0)#datetime.datetime.now() - relativedelta(years=50)
    values = intersect(bbox, tree, column)
    if len(values) > 0:
        return list(map(convert_date, values))
    else:
        return [default]


def percent_years(count, bbox, tree, column):
    vals = [val.replace(tzinfo=None) for val in coerce_dates(bbox, tree, column)]
    fifteen_y = datetime.datetime.now() - relativedelta(years=15)
    ten_y = datetime.datetime.now() - relativedelta(years=10)
    five_y = datetime.datetime.now() - relativedelta(years=5)
    two_y = datetime.datetime.now() - relativedelta(years=2)
    perc_two = (np.divide(len([x for x in vals if x >= two_y]), count) if count > 0 else 0) * 100
    perc_five = (np.divide(len([x for x in vals if five_y < x <= two_y]), count) if count > 0 else 0) * 100
    perc_ten = (np.divide(len([x for x in vals if ten_y < x <= five_y]), count) if count > 0 else 0) * 100
    perc_fifteen = (np.divide(len([x for x in vals if fifteen_y < x <= ten_y]), count) if count > 0 else 0) * 100
    over_fifteen = (np.divide(len([x for x in vals if x < fifteen_y]), count) if count > 0 else 0) * 100
    if len(vals) == 1 and vals[0] == datetime.datetime(1901,1,1,0,0):
        no_values = 0
    else:
        no_values = no_date(count, vals)

    return perc_two, perc_five, perc_ten, perc_fifteen, over_fifteen, no_values


def no_date(count, values):
    nodate = parse('1-1-1111')
    no_values = [v for v in values if (v == nodate or v == datetime.datetime(1901,1,1,0,0))]
    return len(no_values)


# can use ['key'] to get count of specific date when using Counter objects
def mode_dates(bbox, tree, column):
    values = coerce_dates(bbox, tree, column)
    stripped = list(map(str, list(map(datetime.datetime.strftime, values, repeat("%Y-%m-%d")))))
    date_mode = Counter(stripped).most_common(1)
    date_m = date_mode[0][0] if len(date_mode) > 0 else 0
    date_count = date_mode[0][1] if len(date_mode) > 0 else 0

    year_stripped = list(map(str, list(map(datetime.datetime.strftime, values, repeat("%Y")))))
    year_mode = Counter(year_stripped).most_common(1)
    year_m = year_mode[0][0] if len(year_mode) > 0 else 0
    year_count = year_mode[0][1] if len(year_mode) > 0 else 0
    if year_m == '1901':
        year_m = 'None'
        date_m = 'None'
        date_count = 0
        year_count = 0
    #if date_m == '1901-01-01':


    #mode = [m for m, o in ][0]
    return date_m, date_count, year_m, year_count


def bounding_dates(bbox, tree, column):
    dates = [val.replace(tzinfo=None) for val in coerce_dates(bbox, tree, column)]
    minimum_date = min(dates)
    maximum_date = max(dates)
    min_date = minimum_date.strftime("%Y-%m-%d")
    max_date = maximum_date.strftime("%Y-%m-%d")
    if min_date == '1901-01-01':
        min_date = 'None'
    if max_date == '1901-01-01':
        max_date = 'None'

    return min_date, max_date  #, str(date_delta)


def source_lineage(sdp, sps, bbox, tree):
    sdp_values = [v for v in intersect(bbox, tree, sdp)]
    sps_values = [v for v in intersect(bbox, tree, sps)]
    things = []
    for source, boolean in zip(sdp_values, sps_values):
        if boolean == 1001:
            things.append(source)
    unique_things = list(set(things))
    source_list = ",".join(unique_things) if len(unique_things) > 0 else str(['None'])
    if len(things) > 0:
        if len(unique_things) > 1:
            #print(things)
            pri, sec = Counter(things).most_common(2)
            pri_source = str(pri[0])
            sec_source = str(sec[0])
            pri_source_count = int(pri[1])
            sec_source_count = int(sec[1])
        if len(unique_things) == 1:
            pri = Counter(things).most_common(1)
            pri_source = pri[0][0]
            sec_source = 'None'
            pri_source_count = int(pri[0][1])
            sec_source_count = int(0)
    else:
        source_list = 'None'
        pri_source = 'None'
        sec_source = 'None'
        pri_source_count = int(0)
        sec_source_count = int(0)

    #'SOURCE_LIST', '|S4096'
    #'PRI_SOURCE', '|S256'),
    #'PRI_SOURCE_CNT', np.int32), \
    #'PRI_SOURCE_PER', np.float64), \
    #'SEC_SOURCE', '|S256'), \
    #'SEC_SOURCE_CNT', np.int32), \
    #'SEC_SOURCE_PER', np.float64)

    return source_list, pri_source, pri_source_count, sec_source, sec_source_count





# ('POPULATION_SCALE'.lower(), '|S1024'),
# ('THEM_ACC_SCORE'.lower(), np.float64),

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

def most_common(lst):
    return max(set(lst), key=lst.count), lst.count(max(set(lst), key=lst.count))

def minimun(lst):
    return min(lst), lst.count(min(lst))

def maximum(lst):
    return max(lst), lst.count(max(lst))

def nan_if(arr, value):
    return np.where(arr == value, np.nan, arr)

def thematic_accuracy(bbox, tree,column):
    scale_list = [represents_int(v) for v in intersect(bbox, tree, column)]
    count = len(scale_list)
    if count > 0:
        max_scale, max_scale_count = maximum(scale_list)
        min_scale, min_scale_count = minimun(scale_list)
        dom_scale, dom_count = most_common(scale_list)
        count_2500 = scale_list.count(2500)
        count_5000 = scale_list.count(5000)
        count_12500 = scale_list.count(12500)
        count_25000 = scale_list.count(25000)
        count_50000 = scale_list.count(50000)
        count_100000 = scale_list.count(100000)
        count_250000 = scale_list.count(250000)
        count_500000 = scale_list.count(500000)
        count_1000000 = scale_list.count(1000000)
        them_mean = np.nanmean(nan_if(scale_list, -999999.0))

    else:
        max_scale = -1
        max_scale_count = 0
        min_scale = -1
        min_scale_count = 0
        dom_scale = -1
        dom_count = 0
        count_2500 = 0
        count_5000 = 0
        count_12500 = 0
        count_25000 = 0
        count_50000 = 0
        count_100000 = 0
        count_250000 = 0
        count_500000 = 0
        count_1000000 = 0
        them_mean = -1

    msp = get_msp(dom_scale)

    return dom_scale, dom_count, min_scale, min_scale_count, max_scale, max_scale_count, count_2500, count_5000, \
        count_12500, count_25000, count_50000, count_100000, count_250000, count_500000, count_1000000, msp, them_mean



def currency_score(year):
    if year == "None":
        return 0
    else:
        year = parse(str(year))
        fifteen_y = datetime.datetime.now() - relativedelta(years=15)
        ten_y = datetime.datetime.now() - relativedelta(years=10)
        five_y = datetime.datetime.now() - relativedelta(years=5)
        two_y = datetime.datetime.now() - relativedelta(years=2)
        non_standard_year = datetime.datetime(1111, 1, 1)
        if year == non_standard_year:
            score = 6
        elif year >= two_y:
            score = 5
        elif two_y > year >= five_y:
            score = 4
        elif five_y > year >= ten_y:
            score = 3
        elif ten_y > year >= fifteen_y:
            score = 2
        else:
            score = 1
        return score


def get_pa_score(mean):
    value = 0
    if mean > 0:
        if 0 <= mean < 15:
            value = 5
        elif 15 <= mean <= 25:
            value = 4
        elif 25 < mean <= 50:
            value = 3
        elif 50 < mean <= 100:
            value = 2
        else:
            value = 1
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

def get_thematic_score(domScale, GRLS):
    if (domScale == 5000 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 5000 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 12500 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'G'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'R'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'L'):
        POPULATION_SCALE = 5
    elif (domScale == 25000 and GRLS == 'S/U'):
        POPULATION_SCALE = 5
    elif (domScale == 50000 and GRLS == 'G'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'R'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'L'):
        POPULATION_SCALE = 4
    elif (domScale == 50000 and GRLS == 'S/U'):
        POPULATION_SCALE = 2
    elif (domScale == 100000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale == 100000 and GRLS == 'R'):
        POPULATION_SCALE = 3
    elif (domScale == 100000 and GRLS == 'L'):
        POPULATION_SCALE = 2
    elif (domScale == 100000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    elif (domScale == 250000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale == 250000 and GRLS == 'R'):
        POPULATION_SCALE = 3
    elif (domScale == 250000 and GRLS == 'L'):
        POPULATION_SCALE = 2
    elif (domScale == 250000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    elif (domScale >= 500000 and GRLS == 'G'):
        POPULATION_SCALE = 3
    elif (domScale >= 500000 and GRLS == 'R'):
        POPULATION_SCALE = 2
    elif (domScale >= 500000 and GRLS == 'L'):
        POPULATION_SCALE = 1
    elif (domScale >= 500000 and GRLS == 'S/U'):
        POPULATION_SCALE = 1
    else:
        POPULATION_SCALE = 0
    return POPULATION_SCALE

def calculate_classification(classification, caveat, bbox, tree):
    class_values = [v for v in intersect(bbox, tree, classification)]
    caveat_values = [v for v in intersect(bbox, tree, caveat)]

    unique_class = list(set(class_values))
    unique_caveat = list(set(caveat_values))

    if len(unique_class) > 0:
        try:
            class_code = [class_codes['class_dict'][uc] for uc in unique_class]
            class_summary = list(class_codes['class_dict'].keys())[
                list(class_codes['class_dict'].values()).index(max(class_code))]
        except:
            class_summary = 'U'#'noInformation'
    else:
        class_summary = 'U'

    if len(unique_caveat) > 0:
        try:
            caveat_code = [class_codes['caveat_dict'][uc] for uc in unique_caveat]
            caveat_summary = list(class_codes['caveat_dict'].keys())[
                list(class_codes['caveat_dict'].values()).index(max(caveat_code))]
        except:
            caveat_summary = 'DS'#'noInformation'
    else:
        caveat_summary = 'DS'

    return class_summary, caveat_summary

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
    workspace = sys.argv[1]
    features = sys.argv[2]
    idx_location = sys.argv[3]
    out_location = sys.argv[4]
    try:
        update_flag = bool(sys.argv[5]) #True
        update_status = "Update"
    except:
        update_flag = False
        update_status = "Initial"

    log = f"proc_rtree_{features}.log"
    print(os.path.join(idx_location, log))
    print(f"start: {start}")
    logging.basicConfig(filename=os.path.join(idx_location, log), level=logging.INFO)
    logging.info(f"start: {start}")

    grid_fc = os.path.join(out_location, "sotd.sotd.pop_grid")

    c_features = os.path.join(out_location, "sotd.sotd.cd")

    index_file = os.path.join(idx_location, features)

    if arcpy.Exists(out_location):
        print(out_location +  " already exists.")
        logging.info(out_location +  " already exists.")
    else:
        print("Creating " + os.path.split(out_location)[0] + " and " + os.path.basename(out_location))
        logging.info("Creating " + os.path.split(out_location)[0] + " and " + os.path.basename(out_location))
        arcpy.CreateFileGDB_management(os.path.split(out_location)[0], os.path.basename(out_location))



    # Classification Aggregation
    classification_fc_name = features + '_' + 'Classification'
    classification_fc = os.path.join(out_location, classification_fc_name)

    #Source Lineage
    sl_fc_name = features + '_' + 'SourceLineage'
    sl_fc = os.path.join(out_location, sl_fc_name)

    #Positional Accuracy
    pa_fc_name = features + '_' + 'PositionalAccuracy'
    pa_fc = os.path.join(out_location, pa_fc_name)

    #Temporal Currency
    tc_fc_name = features + '_' + 'TemporalCurrency'
    tc_fc = os.path.join(out_location, tc_fc_name)

    #Temporal Accuracy
    temp_acc_fc_name = features + '_' + 'TemporalAccuracy'
    temp_acc_fc = os.path.join(out_location, temp_acc_fc_name)
    years = [1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015,
             2016, 2017, 2018]

    #Thematic Accuracy
    them_acc_fc_name = features + '_' + 'FeatureDensity'
    them_acc_fc = os.path.join(out_location, them_acc_fc_name)
    them_scale_fc = os.path.join(out_location, "thematic_scale") #This file gets deleted
    them_acc_fields = ['dom_scale', 'grls_score','them_acc_score']


    # Flags
    proc_tempcurr = True
    proc_poacc = True
    proc_slin = True
    proc_temp_acc = True
    proc_them_acc = True

    # tree name ? -> just use feature as name ?
    # workspace code is awful
    # provided a path to a gdb and the expectation that it is written as a dataset that
    # is within the gdb i.e. 1 level deep, the following will work.
    default_fields = ['globalid', 'f_code', 'OBJECTID']
    filename = 'ResourceSrf.p'
    resource_cache = os.path.join(idx_location, filename)
    if os.path.isfile(resource_cache):
        print("Reading ResourceSrf from Cache")
        logging.info("Reading ResourceSrf from Cache")
        df = pickle.load(open(resource_cache, "rb"))
    else:
        if update_flag:
            arcpy.env.workspace = out_location
            # arcpy.env.workspace = arcpy.ListDatasets('*TDS')[0]
            resource_arg = arcpy.ListFeatureClasses("*cells_to_update*")[0]
        else:
            arcpy.env.workspace = workspace
            arcpy.env.workspace = arcpy.ListDatasets('*TDS')[0]
            resource_arg = arcpy.ListFeatureClasses("*Resource*")[0]

        # match fields takes 2 args :list type expected and matches the elements of the list from the second
        # arg that best match the first arg
        fields = match_fields(resource_arg, default_fields)
        logging.info(f"fields used in the dataframe: {fields}")

        print("Reading ResourceSrf from DB")
        logging.info("Reading ResourceSrf from DB")
        df = ga.from_featureclass(resource_arg, fields=fields)
        print("Writing ResourceSrf to Cache")
        logging.info("Writing ResourceSrf to Cache")
        pickle.dump(df, open(resource_cache, "wb"))
    #df = ga.from_featureclass(resource_arg, fields=fields)

    # Rtree load, must exist on file if it doesnt it could make an empty file and fail later.
    # add configuration for sys.argv[2] for .bat to load various rtree indexs
    tree = Rtree(index_file, objects="raw")
    print('Rtree loaded.')
    logging.info('RTree loaded.')

    print("Generating Extent.")
    logging.info('Generating extent.')
    df['extent'] = df.apply(lambda x: x['SHAPE'].extent, axis=1)
    #df['extent'] = df.apply(lambda x: utilities.custom_buf(x['SHAPE']), axis=1)
    df['extent'].head()


    print("Assessing Classification.")
    logging.info("Assessing Classification.")
    df['classification'], df['caveat'] = zip(*df.apply(lambda x: calculate_classification('zsax_rs0','zsax_rx3', x['extent'], tree), axis=1))
    class_df = df.copy()
    class_df.head()
    class_df = class_df[class_df.columns.difference(['extent'])]

    class_df.spatial.to_featureclass(classification_fc, overwrite=True)
    finish = datetime.datetime.now()
    print(f"Finished aggregating classification at: {finish}")
    logging.info(f"Finished aggregating classification at: {finish}")

    print("Counting features per cell.")
    logging.info("Counting features per cell.")
    df['feature_cnt'] = df.apply(lambda x: tree.count(x['extent']), axis=1)
    finish = datetime.datetime.now()
    print(f"Finished counting features at: {finish}")
    logging.info(f"Finished counting features at: {finish}")

    #if pickle_fname:
    #   df.to_pickle(pickle_fname)

    if proc_poacc:
        print("Processing Positional Accuracy.")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', pa_fc_name, update_status, "Started")

        logging.info("Processing Positional Accuracy.")
        pa_df = df.copy()
        pa_df['pa_mean'], pa_df['pa_median'], pa_df['pa_mode'], pa_df['pa_min'], pa_df['pa_max'], pa_df['pa_null_cnt'] =\
            zip(*pa_df.apply(lambda x: stats(x['extent'], tree, 'aha'), axis=1))
        pa_df['pa_null_pct'] = ((pa_df['pa_null_cnt'] * 100 / pa_df['feature_cnt']).fillna(0)).round(2)
        pa_df['pa_score'] = pa_df.apply(lambda x: get_pa_score(x['pa_median']), axis=1)
        pa_df['pa_tier'] = pa_df.apply(lambda x: get_tier(x['pa_score']), axis=1)
        finish = datetime.datetime.now()
        print(f"Finished Positional Accuracy at: {finish}")
        logging.info(f"Finished Positional Accuracy at: {finish}")
        print('Writing Positional Accuracy to Feature Class')
        logging.info('Writing Positional Accuracy to Feature Class')
        pa_df = pa_df[pa_df.columns.difference(['extent'])]
        if update_flag:
            print("updating positional accuracy.")
            print(pa_df.head())
            logging.info("updating positional accuracy.")
            pa_fc = utilities.insert_new_features(pa_df, pa_fc, out_location, 'poac')
            print(pa_fc)
            logging.info(pa_fc)
        else:
            print("Overwriting positional accuracy.")
            logging.info("Overwriting positional accuracy.")
            pa_df.spatial.to_featureclass(pa_fc, overwrite=True)
            print(pa_fc)
            logging.info(pa_fc)
        finish = datetime.datetime.now()
        print(f"Finished Writing Positional Accuracy at: {finish}")
        logging.info(f"Finished Writing Positional Accuracy at: {finish}")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', pa_fc_name, update_status, "Completed")

    if proc_tempcurr:
        print("Processing Temporal Currency.")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', tc_fc_name, update_status, "Started")

        logging.info("Processing Temporal Currency.")
        tc_df = df.copy()
        tc_df['dom_date'], tc_df['dom_date_cnt'], tc_df['dom_year'], tc_df['dom_year_cnt'] = \
            zip(*tc_df.apply(lambda x: mode_dates(x['extent'], tree, 'zi001_sdv'), axis=1))

        finish = datetime.datetime.now()
        print(f"Finished mode_dates at: {finish}")
        logging.info(f"Finished mode_dates at: {finish}")

        tc_df['oldest_date'], tc_df['newest_date'] = \
            zip(*tc_df.apply(lambda x: bounding_dates(x['extent'], tree, 'zi001_sdv'), axis=1))

        finish = datetime.datetime.now()
        print(f"Finished bounding_dates at: {finish}")

        tc_df['pct_2_year'], tc_df['pct_5_year'], tc_df['pct_10_year'], tc_df['pct_15_year'], tc_df['pct_15_plus_year'], tc_df['no_date_cnt'] =\
            zip(*tc_df.apply(lambda x: percent_years(x['feature_cnt'], x['extent'], tree, 'zi001_sdv'), axis=1))

        finish = datetime.datetime.now()
        print(f"Finished percent_years at: {finish}")
        logging.info(f"Finished percent_years at: {finish}")

        tc_df['dom_year_int'] = tc_df['dom_year'].replace('None', -1).astype('int32')
        tc_df['pct_2_year'] = tc_df['pct_2_year'].round(2)
        tc_df['pct_5_year'] = tc_df['pct_5_year'].round(2)
        tc_df['pct_10_year'] = tc_df['pct_10_year'].round(2)
        tc_df['pct_15_year'] = tc_df['pct_15_year'].round(2)
        tc_df['pct_15_plus_year'] = tc_df['pct_15_plus_year'].round(2)
        tc_df['dom_date_per'] = (((tc_df["dom_date_cnt"] *100 / tc_df["feature_cnt"]).fillna(0)).round(2)).replace(np.inf, 0)
        tc_df['dom_year_per'] = (((tc_df["dom_year_cnt"] *100 / tc_df["feature_cnt"]).fillna(0)).round(2)).replace(np.inf, 0)
        tc_df['no_date_per'] = (((tc_df["no_date_cnt"] *100 / tc_df["feature_cnt"]).fillna(0)).round(2)).replace(np.inf, 0)
        tc_df['currency_score'] = tc_df.apply(lambda x: currency_score(x['dom_date']), axis=1)
        finish = datetime.datetime.now()
        print(f"Finished Temporal Currency at: {finish}")
        logging.info(f"Finished Temporal Currency at: {finish}")
        print('Writing Temporal Currency to Feature Class')
        logging.info('Writing Temporal Currency to Feature Class')
        tc_df = tc_df[tc_df.columns.difference(['extent'])]
        if update_flag:
            logging.info("Inserting new currency features.")
            print(tc_df.head())
            tc_fc = utilities.insert_new_features(tc_df, tc_fc, out_location, 'curr')
            print(tc_fc)
            logging.info(tc_fc)
        else:
            logging.info("Overwriting existing currency features.")
            tc_df.spatial.to_featureclass(tc_fc, overwrite=True)
            logging.info(tc_fc)
            print(tc_fc)
        finish = datetime.datetime.now()
        print(f"Finished Writing Temporal Currency at: {finish}")
        logging.info(f"Finished Writing Temporal Currency at: {finish}")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', tc_fc_name, update_status, "Completed")

    # scoring is configurable to its respective function
    if proc_slin:
        print("Running Source Lineage.")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', sl_fc_name, update_status, "Started")

        logging.info("Running Source Lineage.")
        sl_df = df.copy()
        sl_df['source_list'], sl_df['pri_source'], sl_df['pri_source_cnt'], sl_df['sec_source'], sl_df['sec_source_cnt'] =\
            zip(*sl_df.apply(lambda x: source_lineage('zi001_sdp', 'zi001_sps', x['extent'], tree), axis=1))
        sl_df['pri_source_per'] = ((sl_df["pri_source_cnt"] *100 / sl_df["feature_cnt"]).fillna(0)).round(2)
        sl_df['sec_source_per'] = ((sl_df["sec_source_cnt"] *100 / sl_df["feature_cnt"]).fillna(0)).round(2)
        finish = datetime.datetime.now()
        print(f"Finished Source Lineage at: {finish}")
        logging.info(f"Finished Source Lineage at: {finish}")
        print('Writing Source Lineage to Feature Class')
        logging.info('Writing Source Lineage to Feature Class')
        sl_df = sl_df[sl_df.columns.difference(['extent'])]
        if update_flag:
            logging.info("Inserting new Source Lineage features.")
            print(sl_df.head())
            sl_fc = utilities.insert_new_features(sl_df, sl_fc, out_location, 'srln')
            print(sl_fc)
            logging.info(sl_fc)
        else:
            logging.info("Overwriting existing Source Lineage features.")
            sl_df.spatial.to_featureclass(sl_fc, overwrite=True)
            print(sl_fc)
            logging.info(sl_fc)
        finish = datetime.datetime.now()
        print(f"Finished Source Lineage at: {finish}")
        logging.info(f"Finished Source Lineage at: {finish}")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', sl_fc_name, update_status, "Completed")

    if proc_temp_acc:
        print("Running temporal accuracy.")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', temp_acc_fc_name, update_status, "Started")

        logging.info("Running temporal accuracy.")
        #year_list = list(map(str, years))
        #print(year_list)
        temp_acc_calc = ta.TemporalAccuracy(c_features,
                                            tc_fc,
                                            temp_acc_fc,
                                            years)
        temp_acc_calc.create_temporal_accuracy()
        print(f"Finished temporal accuracy at: {finish}")
        logging.info(f"Finished temporal accuracy at: {finish}")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', temp_acc_fc_name, update_status, "Completed")

    if proc_them_acc:
        print("Processing Thematic Scale")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', them_acc_fc_name, update_status, "Started")

        logging.info("Processing Thematic Scale")
        them_acc_df = df.copy()
        them_acc_df['dom_scale'], them_acc_df['dom_count'], them_acc_df['min_scale'], them_acc_df['min_scale_count'],\
            them_acc_df['max_scale'], them_acc_df['max_scale_count'], them_acc_df['count_2500'], them_acc_df['count_5000'],\
            them_acc_df['count_12500'], them_acc_df['count_25000'], them_acc_df['count_50000'], them_acc_df['count_100000'], \
            them_acc_df['count_250000'], them_acc_df['count_500000'], them_acc_df['count_1000000'], them_acc_df['mission_planning'], \
            them_acc_df['them_mean'] = zip(*them_acc_df.apply(lambda x: thematic_accuracy(x['extent'], tree,'zi026_ctuu'), axis=1))

        finish = datetime.datetime.now()
        print(f"Finished Thematic Scale: {finish}")
        logging.info(f"Finished Thematic Scale: {finish}")
        print("Processing Thematic Statistics.")
        logging.info("Processing Thematic Statistics.")

        them_acc_df['dom_per'] = (((them_acc_df['dom_count'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(-np.inf, 0)
        them_acc_df['min_per'] = (((them_acc_df['min_scale_count'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['max_per'] = (((them_acc_df['max_scale_count'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_2500'] = (((them_acc_df['count_2500'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_5000'] = (((them_acc_df['count_5000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_12500'] = (((them_acc_df['count_12500'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_25000'] = (((them_acc_df['count_25000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_50000'] = (((them_acc_df['count_50000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_100000'] = (((them_acc_df['count_100000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf, 0).replace(
            -np.inf, 0)
        them_acc_df['per_250000'] = (((them_acc_df['count_250000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf,0).replace(-np.inf, 0)
        them_acc_df['per_500000'] = (((them_acc_df['count_500000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf,0).replace(-np.inf, 0)
        them_acc_df['per_1000000'] = (((them_acc_df['count_1000000'] * 100 / them_acc_df['feature_cnt']).fillna(0)).round(2)).replace(np.inf,0).replace(-np.inf, 0)
        print("Writing Thematic Scale Features")
        logging.info("Writing Thematic Scale Features")


        them_acc_df = them_acc_df[them_acc_df.columns.difference(['extent'])]
        if update_flag:
            logging.info("Inserting new thematic accuracy features.")
            print(them_acc_df.head())
            them_scale_fc = utilities.insert_new_features(them_acc_df, them_acc_fc, out_location, 'them')
            print(them_scale_fc)
            logging.info(them_scale_fc)
        else:
            logging.info("Overwriting new thematic accuracy features.")
            them_acc_df.spatial.to_featureclass(them_scale_fc, overwrite=True)

            print("Joining Scale and Population")
            logging.info("Joining Scale and Population")

            arcpy.analysis.SpatialJoin(them_scale_fc, grid_fc,
                                    them_acc_fc,
                                    "JOIN_ONE_TO_ONE", "KEEP_ALL",
                                    'Shape_Length "Shape_Length" false true true 8 Double 0 0,First,#,HydrographyCrvs_SotD,Shape_Length,-1,-1;Shape_Area "Shape_Area" false true true 8 Double 0 0,First,#,HydrographyCrvs_SotD,Shape_Area,-1,-1;F_CODE "F_CODE" true true false 255 Text 0 0,First,#,HydrographyCrvs_SotD,F_CODE,0,255;caveat "caveat" true true false 4 Text 0 0 ,First,#,HydrographyCrvs,caveat,-1,-1;classification "classification" true true false 2 Text 0 0 ,First,#,HydrographyCrvs,classification,-1,-1;count_100000 "count_100000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_100000,-1,-1;count_1000000 "count_1000000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_1000000,-1,-1;count_12500 "count_12500" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_12500,-1,-1;count_2500 "count_2500" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_2500,-1,-1;count_25000 "count_25000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_25000,-1,-1;count_250000 "count_250000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_250000,-1,-1;count_5000 "count_5000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_5000,-1,-1;count_50000 "count_50000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_50000,-1,-1;count_500000 "count_500000" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,count_500000,-1,-1;dom_count "dom_count" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,dom_count,-1,-1;dom_per "dom_per" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,dom_per,-1,-1;dom_scale "dom_scale" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,dom_scale,-1,-1;feature_cnt "feature_cnt" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,feature_cnt,-1,-1;max_per "max_per" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,max_per,-1,-1;max_scale "max_scale" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,max_scale,-1,-1;max_scale_count "max_scale_count" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,max_scale_count,-1,-1;min_per "min_per" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,min_per,-1,-1;min_scale "min_scale" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,min_scale,-1,-1;min_scale_count "min_scale_count" true true false 4 Long 0 0,First,#,HydrographyCrvs_SotD,min_scale_count,-1,-1;mission_planning "mission_planning" true true false 255 Text 0 0,First,#,HydrographyCrvs_SotD,mission_planning,0,255;per_100000 "per_100000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_100000,-1,-1;per_1000000 "per_1000000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_1000000,-1,-1;per_12500 "per_12500" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_12500,-1,-1;per_2500 "per_2500" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_2500,-1,-1;per_25000 "per_25000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_25000,-1,-1;per_250000 "per_250000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_250000,-1,-1;per_5000 "per_5000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_5000,-1,-1;per_50000 "per_50000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_50000,-1,-1;per_500000 "per_500000" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,per_500000,-1,-1;them_mean "them_mean" true true false 4 Float 0 0,First,#,HydrographyCrvs_SotD,them_mean,-1,-1;GRLS_SCORE "GRLS_SCORE" true true false 4 Text 0 0,First,#,grid,GRLS_SCORE,0,4',
                                    "HAVE_THEIR_CENTER_IN", None, '')

        #arcpy.Delete_management(them_scale_fc)
            arcpy.AddField_management(them_acc_fc, 'them_acc_score', "SHORT", "", "", "")

        #start edit session here:
        #edit = arcpy.da.Editor(out_location)
        #edit.startEditing(False, True)
        #edit.startOperation()

        with arcpy.da.UpdateCursor(them_acc_fc, them_acc_fields) as cursor:
            for row in cursor:
                row[2] = get_thematic_score(row[0], row[1])
                cursor.updateRow(row)

        #edit.stopOperation()
        #edit.stopEditing(True)

        finish = datetime.datetime.now()
        print(f"Finished Thematic Stats: {finish}")
        logging.info(f"Finished Thematic Stats: {finish}")

        utilities.log_update_to_table(out_location, 'SotD_ProcessingStatus', them_acc_fc_name, update_status, "Completed")

    logging.info(f"sample output {df.head(3)}")
    processing = datetime.datetime.now()
    logging.info(f"processor time: {processing - start}")
    print(f"process time for data: {processing - start}")
    logging.info("writing feature")
    print("writing feature")
    # should we put things where we found them ?

    finish = datetime.datetime.now()
    logging.info(f"finish: {finish}, runtime: {finish-start} ")
    print(f"finish: {finish}")
    print(f"runtime: {finish - start}")
    logging.shutdown()


if __name__ == '__main__':
    main()
