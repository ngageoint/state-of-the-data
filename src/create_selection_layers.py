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
import arcpy
from geodataset import SpatialDataFrame
import datetime

def get_datetime_string(s):
    dts = [dt.strftime('%Y-%m-%d') for dt in s]
    return dts

def get_dates_in_range(look_back_days):
    num_days = look_back_days #7 by default
    today = datetime.datetime.today()
    date_list = [today - datetime.timedelta(days=x) for x in range(0, num_days)]
    dates = [d for d in get_datetime_string(date_list)]
    arcpy.AddMessage(date_list)
    arcpy.AddMessage(dates)
    return dates

def form_query_string(date_list):
    date_select_field = "MDE"
    if len(date_list)>1:
        dates_to_query = str(tuple(date_list))
    else:
        dates_to_query = str('('+ str(date_list[0]) + ')')
    query = date_select_field + ' IN ' + dates_to_query
    print(query)
    return query


master_times = datetime.datetime.now()

input_features = arcpy.GetParameterAsText(0)
grid_features = arcpy.GetParameterAsText(1)
output_feature_lyr =  arcpy.GetParameterAsText(3)#"feature_lyr"
output_selected_grid = arcpy.GetParameterAsText(4)#"grid_lyr"
look_back_days = int(arcpy.GetParameterAsText(2))
#last_processed_crvs = arcpy.GetParameterAsText(4)
#last_processed_grids = arcpy.GetParameterAsText(5)

dates = get_dates_in_range(look_back_days)
where_clause = form_query_string(dates)
arcpy.MakeFeatureLayer_management(grid_features,
                                    output_selected_grid)

arcpy.SelectLayerByAttribute_management(output_selected_grid,
                                        "NEW_SELECTION",
                                        where_clause)

arcpy.MakeFeatureLayer_management(input_features, output_feature_lyr)
print(arcpy.GetCount_management(output_selected_grid)[0])
arcpy.SelectLayerByLocation_management(in_layer=output_feature_lyr,
                                        overlap_type="INTERSECT",
                                        select_features=output_selected_grid)

#lines_sdf = SpatialDataFrame.from_featureclass(filename=output_feature_lyr)
#lines_sdf.to_featureclass(os.path.dirname(last_processed_crvs),
#                    os.path.basename(last_processed_crvs))

#grid_sdf = SpatialDataFrame.from_featureclass(filename=output_selected_grid)
#grid_sdf.to_featureclass(os.path.dirname(last_processed_grids),
#                    os.path.basename(last_processed_grids))

print("Total Time %s" % (datetime.datetime.now() - master_times))
print('Done.')