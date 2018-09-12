from sotd_indicators.utilities import *
from osm_runner import *
import arcgis

from arcgis.features import SpatialDataFrame, FeatureLayer
from arcgis.geometry import Geometry, Polyline, filters
from arcgis.geoenrichment import enrich
from arcgis.raster import ImageryLayer

import pandas as pd
import numpy as np
import json
import sys
import os

import temporal_accuracy as ta

def positional_accuracy(out_sdf, df_list, val_field):

    print('Running Positional Accuracy')

    val_field = validate_field(val_field, list(df_list[0]))

    for idx, row in enumerate(out_sdf.iterrows()):

        df_current = df_list[idx]

        PDVERSION = [int(v) for v in pd.__version__.split('.')]

        #Leave this line
        #sq = df_current['SHAPE'].disjoint(geom) == False
        #df_current = df_current[sq].copy()
        if len(df_current) > 0:
            df_notnull = df_current.loc[df_current[val_field].notnull() == True]
            if PDVERSION[1] <= 16:
                df_notnull = df_notnull.drop(val_field, axis=1).join(
                    df_notnull[val_field].astype(float, raise_on_error=False)).copy()
            elif PDVERSION[1] > 16:
                df_notnull = df_notnull.drop(val_field, axis=1).join(df_notnull[val_field].apply(pd.to_numeric,
                                                                                                     errors='coerce')).copy()  # CHANGES NON NUMERIC ROWS to NaN
            df_notnull = df_notnull.loc[df_notnull[val_field].notnull() == True].copy()  # Drops NaN values
            not_null_count = len(df_notnull)
            null_count = len(df_current) - not_null_count
            if PDVERSION[1] == 16:
                try:
                    s = df_notnull.loc[df_notnull[val_field] != 'No Information', val_field].copy().astype(
                        np.float64)
                except:
                    s = df_notnull.loc[
                        df_notnull[val_field].astype(str) != 'No Information', val_field].copy().astype(np.float64)
            elif PDVERSION[1] > 16:
                s = df_notnull.drop(val_field, axis=1).join(
                    df_notnull[val_field].apply(pd.to_numeric, errors='coerce'))[
                    val_field].copy()  # Drops Text Fields
            s = s[s.notnull() == True].copy()  # Drops NaN values
            mean = s.mean()
            median = s.median()
            mode = s.mode()
            if len(mode) > 0:
                mode = mode[0]
            else:
                mode = 0
            mmax = s.max()
            mmin = s.min()
            score = get_pa_score(mean)
            null_percent = float(null_count) * 100.0 / float(len(df_current))

            if not pd.isnull(mean):
                out_sdf.set_value(idx, field_schema.get('poac')[0],round(mean,1))
            else:
                out_sdf.set_value(idx, field_schema.get('poac')[0],-1)
            if not pd.isnull(median):
                out_sdf.set_value(idx, field_schema.get('poac')[1],median)
            else:
                out_sdf.set_value(idx, field_schema.get('poac')[1],-1)

            if not pd.isnull(mode):
                out_sdf.set_value(idx, field_schema.get('poac')[2],mode)
            else:
                out_sdf.set_value(idx, field_schema.get('poac')[2],-1)

            if not pd.isnull(mmin):
                out_sdf.set_value(idx, field_schema.get('poac')[3],mmin)
            else:
                out_sdf.set_value(idx, field_schema.get('poac')[3],-1)

            if not pd.isnull(mmax):
                out_sdf.set_value(idx, field_schema.get('poac')[4],mmax)
            else:
                out_sdf.set_value(idx, field_schema.get('poac')[4],-1)

            out_sdf.set_value(idx, field_schema.get('poac')[5],null_count)
            out_sdf.set_value(idx, field_schema.get('poac')[6],round(null_percent,1))
            out_sdf.set_value(idx, field_schema.get('poac')[7],len(df_current))#not_null_count
            out_sdf.set_value(idx, field_schema.get('poac')[8],score)
            out_sdf.set_value(idx, field_schema.get('poac')[9],get_tier(score))

            del df_notnull
            del mean
            del median
            del mode
            del mmax
            del mmin
            del score
            del null_percent

        else:
            out_sdf.set_value(idx, field_schema.get('poac')[0], -1)
            out_sdf.set_value(idx, field_schema.get('poac')[1], -1)
            out_sdf.set_value(idx, field_schema.get('poac')[2],-1)
            out_sdf.set_value(idx, field_schema.get('poac')[3],-1)
            out_sdf.set_value(idx, field_schema.get('poac')[4], -1)
            out_sdf.set_value(idx, field_schema.get('poac')[5], 0)
            out_sdf.set_value(idx, field_schema.get('poac')[6], 0)
            out_sdf.set_value(idx, field_schema.get('poac')[7], 0)
            out_sdf.set_value(idx, field_schema.get('poac')[8], 0)
            out_sdf.set_value(idx, field_schema.get('poac')[9], "No Ranking")

    return out_sdf


def completeness(out_sdf, df_list, osm_sdf):

    print('Running Completeness')

    for idx, row in enumerate(out_sdf.iterrows()):

        before_val = None
        geom = Geometry(row[1].SHAPE)

        # Unpack Geom Extent as OSM Expects
        bbox = (geom.extent[1], geom.extent[0], geom.extent[3], geom.extent[2])

        # Fetch OSM SpatialDataFrame
        osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')

        data_sdf = df_list[idx]
        if len(data_sdf) == 0:
            before_val = 0

        else:
            sq = data_sdf[data_sdf.geometry.notnull()].geometry.disjoint(geom) == False
            df_before = data_sdf[sq].copy()
            geoms_before = df_before.clip(geom.extent)
            geoms_before_sdf = SpatialDataFrame(geometry=geoms_before)

            q_before = geoms_before_sdf['SHAPE'] == {"paths": []}
            geoms_before_sdf = geoms_before_sdf[~q_before].copy()
            geoms_before_sdf.reset_index(inplace=True, drop=True)

        geometry_type = osm_sdf.geometry_type

        sq = osm_sdf[osm_sdf.geometry.notnull()].geometry.disjoint(geom) == False
        df_after = osm_sdf[sq].copy()

        geoms_after = df_after.clip(geom.extent)

        geoms_after_sdf = SpatialDataFrame(geometry=geoms_after)
        #geoms_after_sdf = SpatialDataFrame({'Pass': 'Pass'}, geometry=geoms_after, index=[0])

        q_after = geoms_after_sdf['SHAPE'] == {"paths": []}
        geoms_after_sdf = geoms_after_sdf[~q_after].copy()
        geoms_after_sdf.reset_index(inplace=True, drop=True)

        # This Need Work
        if geometry_type == "Polygon":
            if before_val == None:
                before_val = geoms_before_sdf.geometry.project_as(4326).get_area('GEODESIC','SQUAREKILOMETERS').sum()
            after_val = geoms_after_sdf.geometry.project_as(4326).get_area('GEODESIC','SQUAREKILOMETERS').sum()
            if after_val > 0:
                score = get_cp_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_cp_score(0, before_val, after_val)

            out_sdf.set_value(idx,field_schema.get('cmpl')[0],round(before_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[1],round(after_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[3],round(before_val - after_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[2],score)

        elif geometry_type == "Polyline":
            if before_val == None:

                geom = geoms_before_sdf.geometry
                geom_projected = geoms_before_sdf.geometry.project_as(3857)
                before_val = int(sum(geom_projected.length.tolist()))

            geom_projected = geoms_before_sdf.geometry.project_as(3857)
            after_val = int(sum(geom_projected.length.tolist()))

            if after_val > 0:
                score = get_cp_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_cp_score(0, before_val, after_val)

            out_sdf.set_value(idx,field_schema.get('cmpl')[0],round(before_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[1],round(after_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[3],round(before_val - after_val,1))
            out_sdf.set_value(idx,field_schema.get('cmpl')[2],score)

        else:
            if before_val == None:
                before_count = len(geoms_before_sdf)
            else:
                before_count = 0
            after_count = len(geoms_after_sdf)
            if after_count > 0:
                score = get_cp_score(ratio=before_count/after_count,
                        baseVal=before_count,
                        inputVal=after_count)
            else:
                score = get_cp_score(ratio=0,
                        baseVal=before_count,
                        inputVal=after_count)

            out_sdf.set_value(idx,field_schema.get('cmpl')[0], before_count)
            out_sdf.set_value(idx,field_schema.get('cmpl')[1], after_count)
            out_sdf.set_value(idx,field_schema.get('cmpl')[3], before_count - after_count)
            out_sdf.set_value(idx,field_schema.get('cmpl')[2], score)

        del sq
        del df_after
        del geom
        if before_val != None:
            print(before_val)
        #    del df_before

    return out_sdf


def logical_consistency(out_sdf, df_list, feat_lyr, err_cnt, err_def, attr_key, attr_file):

    print('Running Logical Consistency')

    TMP_FIELDS = (
        'DEFICIENCY',
        'FEATURE_CLASS',
        'SUBTYPE',
        'ORIG_OID',
        'DEFICIENCY_CNT',
        'SHAPE'
    )

    empty = (-999999, '', None, 'noInformation', 'None', 'Null', 'NULL', -999999.0)

    domain_dict = {}  # e.g. {'AP030': 'Road', 'AN010': 'Railway'}

    for t in feat_lyr.properties.types:
        if 'f_code' in t['domains'].keys():
            for cv in t['domains']['f_code']['codedValues']:
                domain_dict.update({cv['code']: cv['name']})

    attr_dict = json.load(open(attr_file))

    if attr_dict.get(attr_key, None):
        specificAttributeDict = {k: [v.lower() for v in v] for k, v in attr_dict.get(attr_key).items()}

    for idx, row in enumerate(out_sdf.iterrows()):

        data_sdf = df_list[idx]

        stList = set(data_sdf['f_code'].values)

        temp_result_df = pd.DataFrame(columns = TMP_FIELDS)#, dtypes=DTYPES)

        geoms=[]
        for idx_attr, row in data_sdf.iterrows():

            if row['f_code'] in stList:

                if row['f_code'] in specificAttributeDict:

                    vals = []
                    for i in specificAttributeDict[row['f_code']]:
                        vals.append(i)

                    line = row['SHAPE']
                    def_count = len(vals)
                    polyline = Polyline(line)
                    geoms.append(polyline)
                    if def_count > 0:
                        fs = ",".join(vals)
                        oid = row['objectid']

                        temp_result_df.set_value(idx_attr, TMP_FIELDS[0],fs)
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[1],feat_lyr)
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[2],(domain_dict[row['f_code']]))
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[3],round(oid))
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[4],len(vals))

                    else:
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[0],'N/A')
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[1],feat_lyr)
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[2],(domain_dict[row['f_code']]))
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[3],round(oid))
                        temp_result_df.set_value(idx_attr, TMP_FIELDS[4],len(vals))

        attr_sdf = temp_result_df#SpatialDataFrame(temp_result_df, geometry=geoms)

        df_current = attr_sdf
        fcount = len(df_current)

        error_field_count = err_cnt
        error_field_def = err_def

        errors = []
        attrs = []
        if fcount>0: #len(df_current) > 0:
            errors += df_current[error_field_count].tolist()
            def process(x):
                return [va for va in x.replace(' ', '').split('|')[-1].split(',') if len(va) > 1]
            for e in df_current[error_field_def].apply(process).tolist():
                attrs += e
                del e

        results = get_answers(0,errors,attrs, fcount)

        out_sdf.set_value(idx, field_schema.get('logc')[0], results[1])
        out_sdf.set_value(idx, field_schema.get('logc')[1], results[2])
        out_sdf.set_value(idx, field_schema.get('logc')[2], results[3])
        out_sdf.set_value(idx, field_schema.get('logc')[3], results[4])
        out_sdf.set_value(idx, field_schema.get('logc')[4], results[5])
        out_sdf.set_value(idx, field_schema.get('logc')[5], results[6])
        out_sdf.set_value(idx, field_schema.get('logc')[6], results[7])
        out_sdf.set_value(idx, field_schema.get('logc')[7], results[8])
        out_sdf.set_value(idx, field_schema.get('logc')[8], results[9])
        out_sdf.set_value(idx, field_schema.get('logc')[9], results[10])
        out_sdf.set_value(idx, field_schema.get('logc')[10], results[11])
        out_sdf.set_value(idx, field_schema.get('logc')[11], results[12])
        out_sdf.set_value(idx, field_schema.get('logc')[12], results[13])
        out_sdf.set_value(idx, field_schema.get('logc')[13], results[14])
        out_sdf.set_value(idx, field_schema.get('logc')[14], results[15])
        out_sdf.set_value(idx, field_schema.get('logc')[15], results[16])

    return out_sdf


def temporal_currency(out_sdf, df_list, f_currency, non_std_date):

    print('Running Temporal Currency')

    f_currency = validate_field(f_currency, list(df_list[0]))

    for idx, row in enumerate(out_sdf.iterrows()):

        df_current = df_list[idx]

        ##---cut stuff above-----
        #sq = df_current['SHAPE'].disjoint(geom) == False
        #df_current = df_current[sq].copy()
        if len(df_current) > 0:
            if df_current[f_currency].dtype == 'int64':
                df_current[f_currency] = pd.to_datetime(df_current[f_currency], unit='ms')

            dates = df_current[f_currency].tolist()
            count = len(dates)
            date_list_strings = [d for d in dates]
            date_list = [get_datetime(d, non_std_date) for d in dates]
            year_list = [int(x.year) for x in date_list]
            dom_year, dom_year_count = Counter(year_list).most_common()[0]
            dom_date, dom_date_count = Counter(get_datetime_string(date_list)).most_common()[0]
            count_picket_fences = sum(non_std == datetime.datetime(1902,1,1,0,0) for non_std in date_list)
            count_non_std_dates = sum(non_std == datetime.datetime(1901,1,1,0,0) for non_std in date_list) + count_picket_fences
            date_list_minus = [x for x in date_list if (x != datetime.datetime(1901,1,1,0,0) and x != datetime.datetime(1902,1,1,0,0))]
            if len(date_list_minus)>0:
                if dom_date == '1902-1-1' or dom_date == '1902-01-01':
                    dom_date = non_std_date
                    dom_year = int(non_std_date[0:4])
                    sccore = 6
                    oldest = min(get_datetime_string(date_list_minus))
                    newest = max(get_datetime_string(date_list_minus))
                    change_list = [diff_date(dd) for dd in date_list_minus]
                    count_2year = sum(x <= 2 for x in change_list)
                    count_5year = sum((x <= 5 and x > 2) for x in change_list)
                    count_10year = sum((x <= 10 and x > 5) for x in change_list)
                    count_15year = sum((x <= 15 and x > 10) for x in change_list)
                    count_15year_plus = sum(x >= 15 for x in change_list)
                elif dom_date == '1901-1-1' or dom_date == '1901-01-01':
                    dom_date = 'NoInformation'
                    dom_year = 0
                    score = 6
                    oldest = min(get_datetime_string(date_list_minus))
                    newest = max(get_datetime_string(date_list_minus))
                    change_list = [diff_date(dd) for dd in date_list_minus]
                    count_2year = sum(x <= 2 for x in change_list)
                    count_5year = sum((x <= 5 and x > 2) for x in change_list)
                    count_10year = sum((x <= 10 and x > 5) for x in change_list)
                    count_15year = sum((x <= 15 and x > 10) for x in change_list)
                    count_15year_plus = sum(x >= 15 for x in change_list)
                else:
                    dom_date = dom_date
                    dom_year = dom_year
                    oldest = min(get_datetime_string(date_list_minus))
                    newest = max(get_datetime_string(date_list_minus))
                    change_list = [diff_date(dd) for dd in date_list_minus]
                    count_2year = sum(x <= 2 for x in change_list)
                    count_5year = sum((x <= 5 and x > 2) for x in change_list)
                    count_10year = sum((x <= 10 and x > 5) for x in change_list)
                    count_15year = sum((x <= 15 and x > 10) for x in change_list)
                    count_15year_plus = sum(x >= 15 for x in change_list)
                    score = get_currency_score(dom_year, int(non_std_date[0:4]))
            else:
                if dom_date == '1902-01-01':
                    dom_date = non_std_date
                    dom_year = int(non_std_date[0:4])
                    oldest = non_std_date
                    newest = non_std_date
                    change_list = 0
                    count_2year = 0
                    count_5year = 0
                    count_10year = 0
                    count_15year = 0
                    count_15year_plus = 0
                    score = 6
                else:
                    dom_date = 'NoInformation'
                    dom_year = 0
                    oldest = 'NoInformation'
                    newest = 'NoInformation'
                    change_list = 0
                    count_2year = 0
                    count_5year = 0
                    count_10year = 0
                    count_15year = 0
                    count_15year_plus = 0
                    score = 6

            out_sdf.set_value(idx, field_schema.get('curr')[0],dom_date)
            out_sdf.set_value(idx, field_schema.get('curr')[1],dom_date_count)
            out_sdf.set_value(idx, field_schema.get('curr')[2],round(dom_date_count * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[3],dom_year)
            out_sdf.set_value(idx, field_schema.get('curr')[4],dom_year_count)
            out_sdf.set_value(idx, field_schema.get('curr')[5],round(dom_year_count * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[6],oldest)
            out_sdf.set_value(idx, field_schema.get('curr')[7],newest)
            out_sdf.set_value(idx, field_schema.get('curr')[8],count_non_std_dates)
            out_sdf.set_value(idx, field_schema.get('curr')[9],round(float(count_non_std_dates) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[10],round(float(count_2year) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[11],round(float(count_5year) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[12],round(float(count_10year) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[13],round(float(count_15year) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[14],round(float(count_15year_plus) * 100.0 / count,1))
            out_sdf.set_value(idx, field_schema.get('curr')[15],int(count))
            out_sdf.set_value(idx, field_schema.get('curr')[16],int(score))

        else:
            out_sdf.set_value(idx, field_schema.get('curr')[0],"None")
            out_sdf.set_value(idx, field_schema.get('curr')[1],0)
            out_sdf.set_value(idx, field_schema.get('curr')[2],0)
            out_sdf.set_value(idx, field_schema.get('curr')[3],0)
            out_sdf.set_value(idx, field_schema.get('curr')[4],0)
            out_sdf.set_value(idx, field_schema.get('curr')[5],0)
            out_sdf.set_value(idx, field_schema.get('curr')[6],"None")
            out_sdf.set_value(idx, field_schema.get('curr')[7],"None")
            out_sdf.set_value(idx, field_schema.get('curr')[8],0)
            out_sdf.set_value(idx, field_schema.get('curr')[9],0)
            out_sdf.set_value(idx, field_schema.get('curr')[10],0)
            out_sdf.set_value(idx, field_schema.get('curr')[11],0)
            out_sdf.set_value(idx, field_schema.get('curr')[12],0)
            out_sdf.set_value(idx, field_schema.get('curr')[13],0)
            out_sdf.set_value(idx, field_schema.get('curr')[14],0)
            out_sdf.set_value(idx, field_schema.get('curr')[15],0)
            out_sdf.set_value(idx, field_schema.get('curr')[16],0)

    return out_sdf


def thematic_accuracy(out_sdf, df_list, f_thm_acc, them_gis, them_url):

    print('Running Thematic Accuracy')

    f_thm_acc = validate_field(f_thm_acc, list(df_list[0]))

    # List Used for Logging Differences in Population Sources
    pop_diff = []

    for idx, row in enumerate(out_sdf.iterrows()):

        df_current = df_list[idx]


        ##-----------------------------------------------------------------------------
        ## Uses Geoenrichment - Not available outside of AGOL
        # Pull GeoEnrichment Figures
        # enriched = enrich([row[1]['SHAPE']], gis=geo_gis)
        # if 'TOTPOP' not in list(enriched):
        #     enriched_pop = -1
        # else:
        #     enriched_pop = enriched.TOTPOP[0]
        #
        # # Pull Samples From Configured Population Service
        # img_lyr = ImageryLayer(img_url, gis=geo_gis)
        # cells = img_lyr.properties.maxImageHeight * img_lyr.properties.maxImageWidth
        # samples = img_lyr.get_samples(
        #     row[1]['SHAPE'],
        #     geometry_type='esriGeometryPolygon',
        #     sample_count=cells
        # )
        # sample_total = sum([int(sample['value']) for sample in samples])
        #
        # # Push Significant Values Into List for Averaging
        # if enriched_pop or sample_total < 100:
        #     pass
        # else:
        #     diff = abs(enriched_pop - sample_total)
        #     if diff > 100:
        #         pop_diff.append(diff)
        #
        # tot_pop = enriched_pop if enriched_pop > 0 else sample_total
        # tot_pop = tot_pop if tot_pop > 0 else -1

        ##-----------------------------------------------------------------------------

        them_lyr = FeatureLayer(url=them_url, gis=them_gis)

        geom = Geometry(row[1].SHAPE)

        sp_filter = filters.intersects(geom, 4326)

        them_sdf = them_lyr.query(geometry_filter=sp_filter, return_all_records=True).df


        if len(df_current) > 0:
            count = len(df_current)
            max_val = df_current[f_thm_acc].max()
            max_scale = 100 * (len(df_current[df_current[f_thm_acc] == max_val])/count)
            min_val = df_current[f_thm_acc].min()
            min_scale = 100 * (len(df_current[df_current[f_thm_acc] == min_val])/count)
            vc = df_current[f_thm_acc].value_counts()
            common = df_current[f_thm_acc].mode() # Used in MSP
            if len(common) > 0:
                common = common[0]
                common_count = vc[common]
                common_per = (vc[common]/count) * 100
            else:
                common = min_val
                common_count = 1
                common_per = 100
            count_2500 = 0
            count_5000 = 0
            count_12500 = 0
            count_25000 = 0
            count_50000 = 0
            count_100000 = 0
            count_250000 = 0
            count_500000 = 0
            count_1000000 = 0
            if 2500 in vc:
                count_2500 = vc[2500]
            if 5000 in vc:
                count_5000 = vc[5000]
            if 12500 in vc:
                count_12500 = vc[12500]
            if 25000 in vc:
                count_25000 = vc[25000]
            if 50000 in vc:
                count_50000 = vc[50000]
            if 100000 in vc:
                count_100000 = vc[100000]
            if 250000 in vc:
                count_250000 = vc[250000]
            if 500000 in vc:
                count_500000 = vc[500000]
            if 1000000 in vc:
                count_1000000 = vc[1000000]

            MSP = get_msp(scale=common) # SHOULD UPDATE MISSION_PLANNING FIELD

            if not out_sdf['MEAN'][0]:
                m = 0
            else:
                m = out_sdf['MEAN'][0]

            SCORE_VALUE = them_sdf['grls_score'].loc[0]#get_equal_breaks_score(m)# get_equal_breaks_score(output_features, ['MEAN','EQUAL']) # PUT SCORE IN EQUAL

            #GRLS = SCORE_VALUE
            #domScale = common
            # FIELD 1 is the source, Field 2 is the field to be updated
            #df_current['EQUAL'] = SCORE_VALUE # ASSIGNS EQUAL TO LANSCAN_SCALE
            #29 field

            out_sdf.set_value(idx, field_schema.get('them')[0],common)# median
            out_sdf.set_value(idx, field_schema.get('them')[1],common_count) # % common
            out_sdf.set_value(idx, field_schema.get('them')[2],round(common_per,1))
            out_sdf.set_value(idx, field_schema.get('them')[3],min_val)
            out_sdf.set_value(idx, field_schema.get('them')[4],round(min_scale,1))
            out_sdf.set_value(idx, field_schema.get('them')[5],max_val)
            out_sdf.set_value(idx, field_schema.get('them')[6],round(max_scale,1))
            out_sdf.set_value(idx, field_schema.get('them')[7],count_2500)
            out_sdf.set_value(idx, field_schema.get('them')[8],count_5000)
            out_sdf.set_value(idx, field_schema.get('them')[9],count_12500)
            out_sdf.set_value(idx, field_schema.get('them')[10],count_25000)
            out_sdf.set_value(idx, field_schema.get('them')[11],count_50000)
            out_sdf.set_value(idx, field_schema.get('them')[12],count_100000)
            out_sdf.set_value(idx, field_schema.get('them')[13],count_250000)
            out_sdf.set_value(idx, field_schema.get('them')[14],count_500000)
            out_sdf.set_value(idx, field_schema.get('them')[15],count_1000000)
            out_sdf.set_value(idx, field_schema.get('them')[16],round(count_2500*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[17],round(count_5000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[18],round(count_12500*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[19],round(count_25000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[20],round(count_50000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[21],round(count_100000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[22],round(count_250000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[23],round(count_500000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[24],round(count_1000000*100/count,1))
            out_sdf.set_value(idx, field_schema.get('them')[25],count)
            out_sdf.set_value(idx, field_schema.get('them')[26],str(MSP)) #MISSION_PLANNING FIELD
            out_sdf.set_value(idx, field_schema.get('them')[27],SCORE_VALUE)#), # THEMATIC SCALE VALUE
            #out_sdf.set_value(idx, field_schema.get('them')[27], tot_pop)  # ), # THEMATIC SCALE VALUE
            out_sdf.set_value(idx, field_schema.get('them')[28],population_scale(common, SCORE_VALUE)) # POPULATION_SCALE
            #to 28

        else:
            for i in range(0,25):
                out_sdf.set_value(idx, field_schema.get('them')[i],-1)

            out_sdf.set_value(idx, field_schema.get('them')[25],0)
            out_sdf.set_value(idx, field_schema.get('them')[26],'N/A')
            out_sdf.set_value(idx, field_schema.get('them')[27],'N/A')
            out_sdf.set_value(idx, field_schema.get('them')[28],0)

        del df_current

    print('Average Difference of Population Estimates: {}'.format(np.average(pop_diff)))

    return out_sdf


def source_lineage(out_sdf, df_list, f_value, f_search, search_val):

    print('Running Source Lineage')

    f_value = validate_field(f_value, list(df_list[0]))

    if f_search:
        f_search = validate_field(f_search, list(df_list[0]))

    for idx, row in enumerate(out_sdf.iterrows()):

        df_sub = df_list[idx]

        if df_sub.empty:

            out_sdf.set_value(idx, field_schema.get('srln')[0], 'None')
            out_sdf.set_value(idx, field_schema.get('srln')[1], 'None')
            out_sdf.set_value(idx, field_schema.get('srln')[2], 0)
            out_sdf.set_value(idx, field_schema.get('srln')[3], float(0))
            out_sdf.set_value(idx, field_schema.get('srln')[4], 'None')
            out_sdf.set_value(idx, field_schema.get('srln')[5], 0)
            out_sdf.set_value(idx, field_schema.get('srln')[6], float(0))

        else:
            #df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()

            if f_search:
                df_sub = df_sub.loc[df_sub[f_search] == search_val].copy()

            df_sub = df_sub.replace({np.nan: "NULL"})

            grp = df_sub.groupby(by=f_value).size() # Get the counts.
            # sort the values to get the biggest on the top
            #pandas 0.18
            try:
                grp.sort_values(axis=0, ascending=False,
                            inplace=True, kind='quicksort',
                            na_position='last')
            #pandas 0.16
            except:
                grp.sort(axis=0, ascending=False,
                            inplace=True, kind='quicksort',
                            na_position='last')

            if len(grp) > 1:
                grp = grp.head(2)
                out_sdf.set_value(idx, field_schema.get('srln')[0],",".join(df_sub[f_value].unique().tolist()))
                out_sdf.set_value(idx, field_schema.get('srln')[1],grp.index[0])
                out_sdf.set_value(idx, field_schema.get('srln')[2],int(grp[0]))
                out_sdf.set_value(idx, field_schema.get('srln')[3],float(grp[0]) * 100.0 / float(len(df_sub)))
                out_sdf.set_value(idx, field_schema.get('srln')[4],grp.index[1])
                out_sdf.set_value(idx, field_schema.get('srln')[5],int(grp[1]))
                out_sdf.set_value(idx, field_schema.get('srln')[6],float(grp[1]) * 100.0 / float(len(df_sub)))

            elif len(grp) == 0:
                out_sdf.set_value(idx, field_schema.get('srln')[0],'None')
                out_sdf.set_value(idx, field_schema.get('srln')[1],'None')
                out_sdf.set_value(idx, field_schema.get('srln')[2],0)
                out_sdf.set_value(idx, field_schema.get('srln')[3],float(0))
                out_sdf.set_value(idx, field_schema.get('srln')[4],'None')
                out_sdf.set_value(idx, field_schema.get('srln')[5],0)
                out_sdf.set_value(idx, field_schema.get('srln')[6],float(0))

            elif len(grp) == 1:
                out_sdf.set_value(idx, field_schema.get('srln')[0],",".join(df_sub[f_value].unique().tolist()))
                out_sdf.set_value(idx, field_schema.get('srln')[1],grp.index[0])
                out_sdf.set_value(idx, field_schema.get('srln')[2],int(grp[0]))
                out_sdf.set_value(idx, field_schema.get('srln')[3],float(grp[0]) * 100.0 / float(len(df_sub)))
                out_sdf.set_value(idx, field_schema.get('srln')[4],'None')
                out_sdf.set_value(idx, field_schema.get('srln')[5],0)
                out_sdf.set_value(idx, field_schema.get('srln')[6],float(0))
                #print(out_sdf)

    return out_sdf




def temporal_accuracy(c_features, curr_url, output_workspace, output_features, years, curr_gis):

    import zipfile
    import arcpy

    fl = FeatureLayer(url=curr_url, gis=curr_gis)

    item = curr_gis.content.get(fl.properties.serviceItemId)

    export_item = item.export(export_format='File Geodatabase', title='CURRENCY')

    result = export_item.download(save_path=output_workspace)

    folder = os.path.dirname(result)

    with zipfile.ZipFile(result, "r") as zip_ref:
        zip_ref.extractall(folder)

    gdbs = []
    for file in zip_ref.namelist():
        gdbs.append(os.path.split(file)[0])

    gdb = os.path.join(folder, most_common(gdbs))

    arcpy.env.workspace = gdb
    fc = arcpy.ListFeatureClasses()

    feature_class = os.path.join(gdb, fc[0])

    temp_acc_calc = ta.TemporalAccuracy(c_features, feature_class, output_features, years)

    temp_acc_calc.create_temporal_accuracy()