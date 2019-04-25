from sotd_indicators.field_schema import *

from arcgis.geoenrichment import enrich
from arcgis.raster import ImageryLayer
from arcgis.geometry import Geometry
from arcgis.gis.server import Server
from arcgis.features import SpatialDataFrame

from collections import Counter
import pandas as pd
import numpy as np
import datetime
import arcpy
from arcpy import da
import os
import zipfile
import sys

# Set Grid
# form_query_string
# get_datetime_string
# get_dates_in_ranges


def form_query_string(date_list):
    date_select_field = "MDE"
    if len(date_list)>1:
        dates_to_query = str(tuple(date_list))
    else:
        dates_to_query = str('('+ str(date_list[0]) + ')')
    query = date_select_field + ' IN ' + dates_to_query
    return query

def get_datetime_string_wo_zeros(s):
    dts = [dt.strftime('%#m/%#d/%Y') for dt in s]
    return dts

def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts


def get_dates_in_range(look_back_days):
    num_days = look_back_days
    today = datetime.datetime.today()
    date_list = [today - datetime.timedelta(days=x) for x in range(0, num_days)]
    dates = [d for d in get_datetime_string_wo_zeros(date_list)]
    return dates


def most_common(lst):
    return max(set(lst), key=lst.count)

# Completeness
# get_cp_score


def get_cp_score(ratio, baseVal, inputVal):
    if inputVal > 0:
        #ratio = baseVal/inputVal
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


# Positional Accuracy
# get_pa_score
# get_tier

def get_pa_score(mean):
    value = 0
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


# Logical Consistency
# get_field_alias
# get_fc_domains
# create_attr_dict
# get_answers
# most_common_lc_val
# get_lc_score


def get_field_alias(fc):
    fields = arcpy.ListFields(fc)

    field_dict = {}
    for field in fields:
        a1 = field.aliasName
        a2 = a1.replace('<', '(')
        field_dict[field.name] = a2.replace('>',')')

    return field_dict


def get_answers(oid, err, attr, feature_count):

    count = len(err)
    if count > 0:
        mean_err = round(np.mean(err),1)
        med_err = np.median(err)
        min_err = min(err)
        max_err = max(err)
        std_err = np.std(err)
        primary, primary_count, secondary, secondary_count = most_common_lc_val(err)
        lc_score = get_lc_score(primary)
        primary_percent = round(primary_count*100.0/count,1)
        secondary_percent = round(secondary_count*100.0/count,1)
        if mean_err >0:
            pri_attr, pri_attr_count, sec_attr, sec_attr_count = most_common_lc_val(attr)
            pri_attr_percent = round(pri_attr_count*100.0/feature_count,1) #count
            sec_attr_percent = round(sec_attr_count*100.0/feature_count,1) #count
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
        lc_score = 0
    #std_err,
    return (oid, mean_err, med_err, min_err,
            max_err, primary,
            secondary, primary_percent, secondary_percent,
            pri_attr, sec_attr, pri_attr_percent,
            sec_attr_percent, count, pri_attr_count,
            sec_attr_count, lc_score)


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
        sec_src = -1
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


# Temporal Currency
# get_datetime
# diff_date
# get_currency_score


def get_datetime(s, nsyr):
    try:
        if s:
            digits = s.split('-')
        else:
            digits=" "
        counter = 0
        if len(digits) == 3:
            if len(digits[0]) == 4:
                if digits[0]==nsyr:
                    return datetime.datetime(1902,1,1,0,0)
                else:
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
    except:
        if isinstance(s, (datetime.datetime, np.datetime64)) and not s is pd.NaT:
            #arcpy.AddMessage(s)
            date = s
        else:
            #arcpy.AddMessage("Bad year")
            date = datetime.datetime(1901,1,1,0,0)
        return date


def diff_date(date):
    """calculates the difference in days from today till the given date"""
    return float((datetime.datetime.now() - date).days)/365.25


def get_currency_score(year, nsy):

    current_year = datetime.datetime.now()

    if year == nsy:
        score = 6
    else:
        if year >= current_year.year - 2:
            score = 5
        elif year >= current_year.year - 4:
            score = 4
        elif year >= current_year.year - 9:
            score = 3
        elif year >= current_year.year - 14:
            score = 2
        else:
            score = 1

    return score


# Thematic Accuracy
# get_msp
# get_equal_breaks_score
# population_scale

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


def get_equal_breaks_score(mean):
    """"""
    ratio = mean
    if (ratio >= 0 and ratio <= 0.5):
        return "G"
    elif (ratio > 0.5 and ratio <= 1.0):
        return "R"
    elif (ratio > 1.0 and ratio <= 1.5):
        return "L"
    elif (ratio > 1.5 and ratio <= 2.0):
        return "S/U"
    else:
        return 0


def population_scale(domScale, GRLS):
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


def validate_field(field, field_list):

    """
    Attempt to match case before raising Exception. Perhaps KeyError would be better?
    :param field: String representing a Field expected in field_list.
    :param field_list: Column headers generated from Pandas Data Frame.
    :return: String representing proper case for input field.
    """

    if field not in field_list:
        field = field.lower()
        if field not in field_list:
            field = field.upper()
            if field not in field_list:
                raise Exception('The Field {} Was Not Found In Field List'.format(field))

    return field


def validate_geo_gis(geo_gis):

    try:
        enrich([Geometry({"x":-122.435,"y":37.785})], gis=geo_gis)
    except RuntimeError:
        raise Exception('GIS Does Not Support GeoEnrichment: {}'.format(geo_gis))


def validate_img_gis(geo_gis, img_url):

    try:
        img_lyr = ImageryLayer(img_url, gis=geo_gis)
        img_lyr.get_samples(Geometry({"x":-122.435,"y":37.785}), geometry_type='esriGeometryPoint')
        print('Thematic Service GIS: {}'.format(geo_gis))

    except RuntimeError:
        raise Exception('{} Does Not Support getSamples on Service: {}'.format(geo_gis, img_url))


def update_img_service(sdf, gis, svc, ras, url, fld):

    # Create Temporary Feature Class
    res = sdf.to_featureclass(
        os.path.dirname(ras),
        'TEMP_THEM',
        overwrite=True
    )
    print('Feature Class: {}'.format(res))

    # Get List of Server Services
    server = Server(url, gis=gis)
    print(server)
    server_manager = server.services
    services = server_manager.list(folder=fld)

    # Get Service Object
    target_service = None
    for service in services:
        if service.properties.serviceName == svc.split('/')[-2]:
            target_service = service
        else:
            print(service.url, svc)
    if not target_service:
        raise Exception('Target Service Not Found: {}'.format(svc))

    # Stop Target Service
    stop = target_service.stop()
    print('Target Service Stopped: {}'.format(stop))

    # Overwrite Existing Data
    arcpy.env.overwriteOutput = True
    arcpy.PolygonToRaster_conversion(
        in_features=res,
        value_field="POPULATION_SCALE",
        out_rasterdataset=ras,
        cell_assignment="CELL_CENTER",
        priority_field="NONE",
        cellsize="0.15"
    )

    # Remove Temporary File
    arcpy.Delete_management(res)

    # Start Target Service
    start = target_service.start()
    print('Target Service Started: {}'.format(start))


def zip_folder(folder_path, output_path):
    """Zip the contents of an entire folder (with that folder included
    in the archive). Empty subfolders will be included in the archive
    as well.
    """
    parent_folder = os.path.dirname(folder_path)
    # Retrieve the paths of the folder contents.
    contents = os.walk(folder_path)
    try:
        zip_file = zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED)
        for root, folders, files in contents:
            # Include all subfolders, including empty ones.
            for folder_name in folders:
                absolute_path = os.path.join(root, folder_name)
                relative_path = absolute_path.replace(parent_folder + '\\',
                                                      '')
                print("Adding '%s' to archive." % absolute_path)
                zip_file.write(absolute_path, relative_path)
            for file_name in files:
                absolute_path = os.path.join(root, file_name)
                relative_path = absolute_path.replace(parent_folder + '\\',
                                                      '')
                print("Adding '%s' to archive." % absolute_path)
                zip_file.write(absolute_path, relative_path)
        print("'%s' created successfully." % output_path)
    except IOError as message:
        print(message)
        sys.exit(1)
    except OSError as message:
        print(message)
        sys.exit(1)
    except zipfile.BadZipfile as message:
        print(message)
        sys.exit(1)
    finally:
        zip_file.close()

# authoritative_fc = r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\AppendInsertTest.gdb\Mongolia_Partial'
# results_fc = r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\AppendInsertTest.gdb\Mongolia'
# schema = "poac"
def update_insert_features(results_fc, authoritative_fc, schema):

    if arcpy.Exists(authoritative_fc):

        results_fl = arcpy.MakeFeatureLayer_management(results_fc, "res_fl")
        authoritative_fl = arcpy.MakeFeatureLayer_management(authoritative_fc, "auth_fl")
        overlapping_results_fl = arcpy.MakeFeatureLayer_management(results_fc, "ovrlp_res_fl")
        new_features_fl = arcpy.MakeFeatureLayer_management(results_fc, "new_res_fl")

        overlp_res_fl = arcpy.SelectLayerByLocation_management(overlapping_results_fl, "HAVE_THEIR_CENTER_IN",
                                                               authoritative_fl)
        selected_auth_fl = arcpy.SelectLayerByLocation_management(authoritative_fl,
                                                                  "HAVE_THEIR_CENTER_IN",
                                                                  results_fl)
        new_results_fl = arcpy.SelectLayerByLocation_management(new_features_fl,
                                                                "HAVE_THEIR_CENTER_IN",
                                                                authoritative_fl, "#", "#", 'INVERT')
        res_count = arcpy.GetCount_management(results_fl)[0]
        auth_count = arcpy.GetCount_management(selected_auth_fl)[0]
        new_res_count = arcpy.GetCount_management(new_results_fl)[0]

        print(res_count)
        print(auth_count)
        print(new_res_count)

        if int(auth_count) > 0:
            print('There are ' + str(auth_count) + ' Features that need inserting')
            insert_new_results(selected_auth_fl,overlp_res_fl, authoritative_fc, schema)

        if int(new_res_count) > 0:
            print('There are ' + str(new_res_count) + ' Features that need appending')
            append_new_results(new_results_fl, authoritative_fc)

    else:
        new_results_fl = arcpy.MakeFeatureLayer_management(results_fc, "new_res_fl")
        create_new_results(new_results_fl, authoritative_fc)

# Function inserts new results into the authoritative feature class if the
# feature geometries already exist
def insert_new_results(selected_auth_fl, selected_res_fl, authoritative_fc, schema):
    selected_auth_sdf = SpatialDataFrame.from_featureclass(selected_auth_fl)
    print(selected_auth_sdf)
    selected_res_sdf = SpatialDataFrame.from_featureclass(selected_res_fl)
    print(selected_res_sdf)

    fields = field_schema.get(schema)
    #for f in fields:
    #    print(f)

    # Write this function
    dtypes =  dts.get(schema)
    fields = field_schema.get(schema)

    for idx, sel_auth_row in enumerate(selected_auth_sdf.iterrows()):

        geom = sel_auth_row[1].SHAPE.buffer(-.01)
        #print(geom)
        #oid = sel_auth_row[1].OBJECTID

        # print(oid)

        ext = geom.extent

        sq = selected_res_sdf['SHAPE'].disjoint(geom) == False
        df_current = selected_res_sdf[sq].copy()
        df_current.reset_index(inplace=True)
        #print(df_current.head())

        if len(df_current) > 0:
            # print("Here")
            #['MEAN_CE90']
            for f in fields:
                try:
                    #print(f)
                    cur_val = df_current.loc[0].at[f]
                    #print(cur_val)
                    selected_auth_sdf.at[idx, f] = cur_val
                    #print(f)
                except:
                    # break
                    print("Field doesn't exist")
            #print(df_current)

    insert_df = selected_auth_sdf.drop(['SHAPE'], axis=1, inplace=False)

    print(insert_df)

    records = insert_df.to_records(index=False)

    print(records)

    rows = np.array(records, dtype=dtypes)


    array = rows  # np.array(rows, dtypes)
    #da.ExtendTable(authoritative_fc, "OID@", array, "_ID", False)
    try:
        print("trying _ID")
        da.ExtendTable(authoritative_fc, "OID@", array, "_ID", False)
    except:
        print('Trying OBJECTID')
        da.ExtendTable(authoritative_fc, "OID@", array, "OBJECTID", False)

    return authoritative_fc

# Function appends new results to an existing authoritative feature class
# if the feature geometries did not already exist
def append_new_results(new_results_fl, authoritative_fc):
    new_res_count = arcpy.GetCount_management(new_results_fl)[0]

    print("Appending " + str(new_res_count) + " New Features")
    arcpy.Append_management(new_results_fl, authoritative_fc, "NO_TEST")

    return authoritative_fc

# Function creates new features if the authoritative features don't already exist
def create_new_results(new_results_fl, authoritative_fc):

    gdb = create_new_gdb(os.path.dirname(authoritative_fc))
    new_fc = arcpy.CopyFeatures_management(new_results_fl, authoritative_fc)

    return new_fc

def create_new_gdb(gdb):

    if arcpy.Exists(gdb) == False:
        gdb_name = os.path.basename(gdb)
        gdb_path = os.path.dirname(gdb)
        arcpy.CreateFileGDB_management(gdb_path, gdb_name)

    return gdb

def create_temp_gdb(gdb_location, timestamp):

    gdb = os.path.join(gdb_location, 'daliy_'+timestamp+'.gdb')
    arcpy.CreateFileGDB_management(gdb_location, 'daliy_'+timestamp+'.gdb')

    return gdb

def copy_to_s3(gdb):

    zipped_gdb = gdb + ".zip"
    zip_folder(gdb, zipped_gdb)

    #Use boto3

    return zipped_gdb

