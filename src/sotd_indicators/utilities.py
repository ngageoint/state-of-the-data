from collections import Counter
import pandas as pd
import numpy as np
import datetime
import arcpy
import xlrd
import ast


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


def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts


def get_dates_in_range(look_back_days):
    num_days = look_back_days
    today = datetime.datetime.today()
    date_list = [today - datetime.timedelta(days=x) for x in range(0, num_days)]
    dates = [d for d in get_datetime_string(date_list)]
    return dates


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
        field_dict[field.name] = field.aliasName

    return field_dict


def get_fc_domains(gdb):
    domains = arcpy.da.ListDomains(gdb)
    domain_dict = {}
    for domain in domains:
        if 'FCODE' in domain.name:
            domain_dict.update(domain.codedValues)

    return domain_dict


def create_attr_dict(filename, check):
    """Creates and attribute dictionary"""
    xl_workbook = xlrd.open_workbook(filename)
    specificAttributeString = '{'
    specificAttributeDict = {}
    xl_sheet = xl_workbook.sheet_by_name(check)
    for row in range(xl_sheet.nrows):
        if row>0:
            cell = xl_sheet.cell(row,8)
            specificAttributeString += cell.value
    specificAttributeDict = ast.literal_eval(specificAttributeString[:-1] + '}')
    return specificAttributeDict, check


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
