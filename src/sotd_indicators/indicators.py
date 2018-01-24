from src.sotd_indicators.utilities import *

from arcgis.features import SpatialDataFrame, FeatureLayer
from arcgis.geometry import Geometry, Polyline, filters

import pandas as pd
import numpy as np


def positional_accuracy(
        out_sdf,
        df_list,
        val_field
):

    print('Running Positional Accuracy')

    FIELDS = ('MEAN',
        'MEDIAN',
        'MODE',
        'MIN_',
        'MAX_',
        'NO_DATE_CNT',
        'NO_DATE_PCT',
        'FEATURE_CNT',
        'PA_SCORE',
        "TIER"
    )

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
                out_sdf.set_value(idx, FIELDS[0],round(mean,1))
            else:
                out_sdf.set_value(idx, FIELDS[0],-1)
            if not pd.isnull(median):
                out_sdf.set_value(idx, FIELDS[1],median)
            else:
                out_sdf.set_value(idx, FIELDS[1],-1)

            if not pd.isnull(mode):
                out_sdf.set_value(idx, FIELDS[2],mode)
            else:
                out_sdf.set_value(idx, FIELDS[2],-1)

            if not pd.isnull(mmin):
                out_sdf.set_value(idx, FIELDS[3],mmin)
            else:
                out_sdf.set_value(idx, FIELDS[3],-1)

            if not pd.isnull(mmax):
                out_sdf.set_value(idx, FIELDS[4],mmax)
            else:
                out_sdf.set_value(idx, FIELDS[4],-1)

            out_sdf.set_value(idx, FIELDS[5],null_count)
            out_sdf.set_value(idx, FIELDS[6],round(null_percent,1))
            out_sdf.set_value(idx, FIELDS[7],len(df_current))#not_null_count
            out_sdf.set_value(idx, FIELDS[8],score)
            out_sdf.set_value(idx, FIELDS[9],get_tier(score))

            del df_notnull
            del mean
            del median
            del mode
            del mmax
            del mmin
            del score
            del null_percent

        else:
            out_sdf.set_value(idx, FIELDS[0], -1)
            out_sdf.set_value(idx, FIELDS[1], -1)
            out_sdf.set_value(idx, FIELDS[2],-1)
            out_sdf.set_value(idx, FIELDS[3],-1)
            out_sdf.set_value(idx, FIELDS[4], -1)
            out_sdf.set_value(idx, FIELDS[5], 0)
            out_sdf.set_value(idx, FIELDS[6], 0)
            out_sdf.set_value(idx, FIELDS[7], 0)
            out_sdf.set_value(idx, FIELDS[8], 0)
            out_sdf.set_value(idx, FIELDS[9], "No Ranking")

    return out_sdf


def completeness(
        out_sdf,
        df_list,
        osm_sdf
):

    print('Running Completeness')

    FIELDS = [
        'TDS_DENSITY',
        'COMP_DENSITY',
        'COMPLETENESS_VALUE',
        'DIFFERENCE'
    ]

    for idx, row in enumerate(out_sdf.iterrows()):

        geom = Geometry(row[1].SHAPE)
        #buff = geom.buffer(1)

        data_sdf = df_list[idx]

        geometry_type = osm_sdf.geometry_type

        sq = data_sdf[data_sdf.geometry.notnull()].geometry.disjoint(geom) == False
        df_before = data_sdf[sq].copy()

        sq = osm_sdf[osm_sdf.geometry.notnull()].geometry.disjoint(geom) == False
        df_after = osm_sdf[sq].copy()

        geoms_after = df_after.clip(geom.extent)
        geoms_before = df_before.clip(geom.extent)

        #after == comparison data
        #before == your data
        geoms_before_sdf = SpatialDataFrame(geometry=geoms_before)
        geoms_after_sdf = SpatialDataFrame(geometry=geoms_after)

        q_after = geoms_after_sdf['SHAPE'] == {"paths": []}
        geoms_after_sdf = geoms_after_sdf[~q_after].copy()
        geoms_after_sdf.reset_index(inplace=True, drop=True)

        q_before = geoms_before_sdf['SHAPE'] == {"paths": []}
        geoms_before_sdf = geoms_before_sdf[~q_before].copy()
        geoms_before_sdf.reset_index(inplace=True, drop=True)

        # This Need Work
        if geometry_type == "Polygon":
            before_val = geoms_before_sdf.geometry.project_as(4326).get_area('GEODESIC','SQUAREKILOMETERS').sum()
            after_val = geoms_after_sdf.geometry.project_as(4326).get_area('GEODESIC','SQUAREKILOMETERS').sum()
            if after_val > 0:
                score = get_cp_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_cp_score(0, before_val, after_val)

            out_sdf.set_value(idx,FIELDS[0],round(before_val,1))
            out_sdf.set_value(idx,FIELDS[1],round(after_val,1))
            out_sdf.set_value(idx,FIELDS[3],round(before_val - after_val,1))
            out_sdf.set_value(idx,FIELDS[2],score)

        elif geometry_type == "Polyline":
            before_val = geoms_before_sdf.geometry.project_as(4326).get_length('GEODESIC','KILOMETERS').sum()
            after_val = geoms_after_sdf.geometry.project_as(4326).get_length('GEODESIC','KILOMETERS').sum()

            if after_val > 0:
                score = get_cp_score(ratio=before_val/after_val,
                        baseVal=before_val,
                        inputVal=after_val)
            else:
                score = get_cp_score(0, before_val, after_val)

            out_sdf.set_value(idx,FIELDS[0],round(before_val,1))
            out_sdf.set_value(idx,FIELDS[1],round(after_val,1))
            out_sdf.set_value(idx,FIELDS[3],round(before_val - after_val,1))
            out_sdf.set_value(idx,FIELDS[2],score)

        else:
            before_count = len(geoms_before_sdf)
            after_count = len(geoms_after_sdf)
            if after_count > 0:
                score = get_cp_score(ratio=before_count/after_count,
                        baseVal=before_count,
                        inputVal=after_count)
            else:
                score = get_cp_score(ratio=0,
                        baseVal=before_count,
                        inputVal=after_count)

            out_sdf.set_value(idx,FIELDS[0], before_count)
            out_sdf.set_value(idx,FIELDS[1], after_count)
            out_sdf.set_value(idx,FIELDS[3], before_count - after_count)
            out_sdf.set_value(idx,FIELDS[2], score)

        del sq
        del df_after
        del df_before
        del geom

    return out_sdf


def logical_consistency(
        out_sdf,
        df_list,
        feat_url,
        f_att_err_cnt,
        f_att_err_def,
        template_fc,
        template_gdb,
        attr_check_tab,
        attr_check_file
):

    print('Running Logical Consistency')

    SUM_FIELDS = [
        'MEAN_DEF_CNT',
        'MEDIAN_DEF_CNT',
        'MIN_DEF_CNT',
        'MAX_DEF_CNT',
        'PRI_NUM_DEF',
        'SEC_NUM_DEF',
        'PER_PRI',
        'PER_SEC',
        'PRI_ATTR_DEF',
        'SEC_ATTR_DEF',
        'PRI_ATTR_DEF_PER',
        'SEC_ATTR_DEF_PER',
        'FEATURE_CNT',
        'PRI_ATTR_DEF_CNT',
        'SEC_ATTR_DEF_CNT',
        'LC_SCORE'
    ]

    FIELDS = [
        'DEFICIENCY',
        'FEATURE_CLASS',
        'SUBTYPE',
        'ORIG_OID',
        'DEFICIENCY_CNT',
        'SHAPE'
    ]

    empty = (-999999, '', None, 'noInformation', 'None', 'Null', 'NULL', -999999.0)

    fc = feat_url

    #alias_table = get_field_alias(template_fc)
    fc_domain_dict = get_fc_domains(template_gdb)

    specificAttributeDict, attrCheck = create_attr_dict(attr_check_file, attr_check_tab)

    for idx, row in enumerate(out_sdf.iterrows()):

        data_sdf = df_list[idx]

        stList = set(data_sdf['F_CODE'].values)

        temp_result_df = pd.DataFrame(columns = FIELDS)#, dtypes=DTYPES)

        geoms=[]
        for idx_attr, row in data_sdf.iterrows():
            if row['F_CODE'] in stList:
                if row['F_CODE'] in specificAttributeDict:
                    vals = [
                        i for i in specificAttributeDict[row['F_CODE']]
                        if row[i] in empty
                    ]
                    #vals = [
                    #    alias_table[i] for i in specificAttributeDict[row['F_CODE']]
                    #    if row[i] in empty
                    #]

                    line = row['SHAPE']
                    def_count = len(vals)
                    polyline = Polyline(line)
                    geoms.append(polyline)
                    if def_count > 0:
                        fs = ",".join(vals)
                        oid = row['OBJECTID']

                        temp_result_df.set_value(idx_attr, FIELDS[0],fs)
                        temp_result_df.set_value(idx_attr, FIELDS[1],fc)
                        temp_result_df.set_value(idx_attr, FIELDS[2],(fc_domain_dict[row['F_CODE']]))
                        temp_result_df.set_value(idx_attr, FIELDS[3],round(oid))
                        temp_result_df.set_value(idx_attr, FIELDS[4],len(vals))


                    else:
                        temp_result_df.set_value(idx_attr, FIELDS[0],'N/A')
                        temp_result_df.set_value(idx_attr, FIELDS[1],fc)
                        temp_result_df.set_value(idx_attr, FIELDS[2],(fc_domain_dict[row['F_CODE']]))
                        temp_result_df.set_value(idx_attr, FIELDS[3],round(oid))
                        temp_result_df.set_value(idx_attr, FIELDS[4],len(vals))

        attr_sdf = temp_result_df#SpatialDataFrame(temp_result_df, geometry=geoms)

        df_current = attr_sdf
        fcount = len(df_current)

        error_field_count = f_att_err_cnt
        error_field_def = f_att_err_def

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

        out_sdf.set_value(idx, SUM_FIELDS[0], results[1])
        out_sdf.set_value(idx, SUM_FIELDS[1], results[2])
        out_sdf.set_value(idx, SUM_FIELDS[2], results[3])
        out_sdf.set_value(idx, SUM_FIELDS[3], results[4])
        out_sdf.set_value(idx, SUM_FIELDS[4], results[5])
        out_sdf.set_value(idx, SUM_FIELDS[5], results[6])
        out_sdf.set_value(idx, SUM_FIELDS[6], results[7])
        out_sdf.set_value(idx, SUM_FIELDS[7], results[8])
        out_sdf.set_value(idx, SUM_FIELDS[8], results[9])
        out_sdf.set_value(idx, SUM_FIELDS[9], results[10])
        out_sdf.set_value(idx, SUM_FIELDS[10], results[11])
        out_sdf.set_value(idx, SUM_FIELDS[11], results[12])
        out_sdf.set_value(idx, SUM_FIELDS[12], results[13])
        out_sdf.set_value(idx, SUM_FIELDS[13], results[14])
        out_sdf.set_value(idx, SUM_FIELDS[14], results[15])
        out_sdf.set_value(idx, SUM_FIELDS[15], results[16])

    return out_sdf


def temporal_currency(
        out_sdf,
        df_list,
        f_currency,
        non_std_date
):

    print('Running Temporal Currency')

    FIELDS = ['DOM_DATE',
              'DOM_DATE_CNT',
              'DOM_DATE_PER',
              'DOM_YEAR',
              'DOM_YEAR_CNT',
              'DOM_YEAR_PER',
              'OLDEST_DATE',
              'NEWEST_DATE',
              'NO_DATE_CNT',
              'NO_DATE_PER',
              'PCT_2_YEAR',
              'PCT_5_YEAR',
              'PCT_10_YEAR',
              'PCT_15_YEAR',
              'PCT_15_PLUS_YEAR',
              'FEATURE_CNT',
              'CURRENCY_SCORE']

    for idx, row in enumerate(out_sdf.iterrows()):

        df_current = df_list[idx]

        ##---cut stuff above-----
        #sq = df_current['SHAPE'].disjoint(geom) == False
        #df_current = df_current[sq].copy()
        if len(df_current) > 0:
            dates = df_current[f_currency].tolist()
            count = len(dates)
            print(count)
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

            out_sdf.set_value(idx, FIELDS[0],dom_date)
            out_sdf.set_value(idx, FIELDS[1],dom_date_count)
            out_sdf.set_value(idx, FIELDS[2],round(dom_date_count * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[3],dom_year)
            out_sdf.set_value(idx, FIELDS[4],dom_year_count)
            out_sdf.set_value(idx, FIELDS[5],round(dom_year_count * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[6],oldest)
            out_sdf.set_value(idx, FIELDS[7],newest)
            out_sdf.set_value(idx, FIELDS[8],count_non_std_dates)
            out_sdf.set_value(idx, FIELDS[9],round(float(count_non_std_dates) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[10],round(float(count_2year) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[11],round(float(count_5year) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[12],round(float(count_10year) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[13],round(float(count_15year) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[14],round(float(count_15year_plus) * 100.0 / count,1))
            out_sdf.set_value(idx, FIELDS[15],int(count))
            out_sdf.set_value(idx, FIELDS[16],int(score))

        else:
            out_sdf.set_value(idx, FIELDS[0],"None")
            out_sdf.set_value(idx, FIELDS[1],0)
            out_sdf.set_value(idx, FIELDS[2],0)
            out_sdf.set_value(idx, FIELDS[3],0)
            out_sdf.set_value(idx, FIELDS[4],0)
            out_sdf.set_value(idx, FIELDS[5],0)
            out_sdf.set_value(idx, FIELDS[6],"None")
            out_sdf.set_value(idx, FIELDS[7],"None")
            out_sdf.set_value(idx, FIELDS[8],0)
            out_sdf.set_value(idx, FIELDS[9],0)
            out_sdf.set_value(idx, FIELDS[10],0)
            out_sdf.set_value(idx, FIELDS[11],0)
            out_sdf.set_value(idx, FIELDS[12],0)
            out_sdf.set_value(idx, FIELDS[13],0)
            out_sdf.set_value(idx, FIELDS[14],0)
            out_sdf.set_value(idx, FIELDS[15],0)
            out_sdf.set_value(idx, FIELDS[16],0)

    return out_sdf


def thematic_accuracy(
        out_sdf,
        df_list,
        f_thm_acc
):

    print('Running Thematic Accuracy')

    FIELDS = ['DOM_SCALE',
              'DOM_COUNT',
              'DOM_PER',
              'MIN_SCALE',
              'MIN_PER',
              'MAX_SCALE',
              'MAX_PER',
              'CNT_2500',
              'CNT_5000',
              'CNT_12500',
              'CNT_25000',
              'CNT_50000',
              'CNT_100000',
              'CNT_250000',
              'CNT_500000',
              'CNT_1000000',
              'PER_2500',
              'PER_5000',
              'PER_12500',
              'PER_25000',
              'PER_50000',
              'PER_100000',
              'PER_250000',
              'PER_500000',
              'PER_1000000',
              'COUNT_',  # Add Underscore if AGOL
              'MISSION_PLANNING',
              'POPULATION_SCALE',
              'THEM_ACC_SCORE']

    # Moved From Function Signature
    # output_features = thematic_url
    # value_field = thematic_acc_field

    for idx, row in enumerate(out_sdf.iterrows()):

        df_current = df_list[idx]

        #sq = df_current['SHAPE'].disjoint(geom) == False
        #df_current = df_current[sq].copy()

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

            SCORE_VALUE = get_equal_breaks_score(mean=out_sdf['MEAN'][0])# get_equal_breaks_score(output_features, ['MEAN','EQUAL']) # PUT SCORE IN EQUAL
            #GRLS = SCORE_VALUE
            #domScale = common
            # FIELD 1 is the source, Field 2 is the field to be updated
            #df_current['EQUAL'] = SCORE_VALUE # ASSIGNS EQUAL TO LANSCAN_SCALE
            #29 field

            out_sdf.set_value(idx, FIELDS[0],common)# median
            out_sdf.set_value(idx, FIELDS[1],common_count) # % common
            out_sdf.set_value(idx, FIELDS[2],round(common_per,1))
            out_sdf.set_value(idx, FIELDS[3],min_val)
            out_sdf.set_value(idx, FIELDS[4],round(min_scale,1))
            out_sdf.set_value(idx, FIELDS[5],max_val)
            out_sdf.set_value(idx, FIELDS[6],round(max_scale,1))
            out_sdf.set_value(idx, FIELDS[7],count_2500)
            out_sdf.set_value(idx, FIELDS[8],count_5000)
            out_sdf.set_value(idx, FIELDS[9],count_12500)
            out_sdf.set_value(idx, FIELDS[10],count_25000)
            out_sdf.set_value(idx, FIELDS[11],count_50000)
            out_sdf.set_value(idx, FIELDS[12],count_100000)
            out_sdf.set_value(idx, FIELDS[13],count_250000)
            out_sdf.set_value(idx, FIELDS[14],count_500000)
            out_sdf.set_value(idx, FIELDS[15],count_1000000)
            out_sdf.set_value(idx, FIELDS[16],round(count_2500*100/count,1))
            out_sdf.set_value(idx, FIELDS[17],round(count_5000*100/count,1))
            out_sdf.set_value(idx, FIELDS[18],round(count_12500*100/count,1))
            out_sdf.set_value(idx, FIELDS[19],round(count_25000*100/count,1))
            out_sdf.set_value(idx, FIELDS[20],round(count_50000*100/count,1))
            out_sdf.set_value(idx, FIELDS[21],round(count_100000*100/count,1))
            out_sdf.set_value(idx, FIELDS[22],round(count_250000*100/count,1))
            out_sdf.set_value(idx, FIELDS[23],round(count_500000*100/count,1))
            out_sdf.set_value(idx, FIELDS[24],round(count_1000000*100/count,1))
            out_sdf.set_value(idx, FIELDS[25],count)
            out_sdf.set_value(idx, FIELDS[26],str(MSP)) #MISSION_PLANNING FIELD
            out_sdf.set_value(idx, FIELDS[27],SCORE_VALUE)#), # THEMATIC SCALE VALUE
            out_sdf.set_value(idx, FIELDS[28],population_scale(common, SCORE_VALUE)) # POPULATION_SCALE
            #to 28

        else:
            for i in range(0,25):
                out_sdf.set_value(idx, FIELDS[i],-1)
            out_sdf.set_value(idx, FIELDS[25],0)
            out_sdf.set_value(idx, FIELDS[26],'N/A')
            out_sdf.set_value(idx, FIELDS[27],'N/A')
            out_sdf.set_value(idx, FIELDS[28],0)

        #del geom
        #del sq
        del df_current

    return out_sdf


def source_lineage(
        out_sdf,
        df_list,
        f_value,
        f_search,
        search_val
):

    print('Running Source Lineage')

    FIELDS = ('SOURCE_LIST',
              'PRI_SOURCE',
              'PRI_SOURCE_CNT',
              'PRI_SOURCE_PER',
              'SEC_SOURCE',
              'SEC_SOURCE_CNT',
              'SEC_SOURCE_PER')

    for idx, row in enumerate(out_sdf.iterrows()):

        df_sub = df_list[idx]

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
            out_sdf.set_value(idx, FIELDS[0],",".join(df_sub[f_value].unique().tolist()))
            out_sdf.set_value(idx, FIELDS[1],grp.index[0])
            out_sdf.set_value(idx, FIELDS[2],int(grp[0]))
            out_sdf.set_value(idx, FIELDS[3],float(grp[0]) * 100.0 / float(len(df_sub)))
            out_sdf.set_value(idx, FIELDS[4],grp.index[1])
            out_sdf.set_value(idx, FIELDS[5],int(grp[1]))
            out_sdf.set_value(idx, FIELDS[6],float(grp[1]) * 100.0 / float(len(df_sub)))

        elif len(grp) == 0:
            out_sdf.set_value(idx, FIELDS[0],'None')
            out_sdf.set_value(idx, FIELDS[0],'None')
            out_sdf.set_value(idx, FIELDS[0],0)
            out_sdf.set_value(idx, FIELDS[0],float(0))
            out_sdf.set_value(idx, FIELDS[0],'None')
            out_sdf.set_value(idx, FIELDS[0],0)
            out_sdf.set_value(idx, FIELDS[0],float(0))

        elif len(grp) == 1:
            out_sdf.set_value(idx, FIELDS[0],",".join(df_sub[f_value].unique().tolist()))
            out_sdf.set_value(idx, FIELDS[0],grp.index[0])
            out_sdf.set_value(idx, FIELDS[0],int(grp[0]))
            out_sdf.set_value(idx, FIELDS[0],float(grp[0]) * 100.0 / float(len(df_sub)))
            out_sdf.set_value(idx, FIELDS[0],'None')
            out_sdf.set_value(idx, FIELDS[0],0)
            out_sdf.set_value(idx, FIELDS[0],float(0))

    return out_sdf
