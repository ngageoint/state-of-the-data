import datetime
import arcpy
import os
import sys

arcpy.env.overwriteOutput = True


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
    dates = [d for d in get_datetime_string(date_list)]
    return dates


def main():

    workspace =         sys.argv[1]
    out_location =      sys.argv[2]
    look_back_days =    int(sys.argv[3]) #90
    output_fcname =     sys.argv[4] #'cells_to_update'

    arcpy.env.workspace = workspace
    arcpy.env.workspace = arcpy.ListDatasets('*TDS')[0]
    feature_arg = arcpy.ListFeatureClasses("*Resource*")[0]
    print(feature_arg)

    if look_back_days > 0:
        print("Selecting features.")
        selecting_feature_class = os.path.join(workspace, feature_arg)

        date_list = get_dates_in_range(look_back_days)
        where_clause = form_query_string(date_list)
        print(where_clause)

        selected_resource_srfs = arcpy.MakeFeatureLayer_management(selecting_feature_class, 'sel_fl',
                                                                   where_clause=where_clause)
        print("Selecting Feature Count: ")
        print(str(arcpy.GetCount_management(selected_resource_srfs)[0]))

        arcpy.CopyFeatures_management(selected_resource_srfs, os.path.join(out_location, output_fcname))


if __name__ == '__main__':
    main()