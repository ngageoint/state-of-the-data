"""-----------------------------------------------------------------------------
Name: create_selection_layers.py
Purpose:
Description:
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: March, 2017
Upated: April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

import os
import datetime

def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts

def get_dates_in_range(look_back_days):
    num_days = look_back_days #7 by default
    today = datetime.datetime.today()
    date_list = [today - datetime.timedelta(days=x) for x in range(0, num_days)]
    dates = [d for d in get_datetime_string(date_list)]
    return dates

def form_query_string(date_list):
    date_select_field = "MDE"
    if len(date_list)>1:
        dates_to_query = str(tuple(date_list))
    else:
        dates_to_query = str('('+ str(date_list[0]) + ')')
    query = date_select_field + ' IN ' + dates_to_query
    return query
