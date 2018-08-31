import arcpy
import numpy as np
import os

arcpy.env.overwriteOutput = True

# Determines the population thresholds based on population breaks
# here: https://www.arcgis.com/home/item.html?id=0f83177f15d640ed911bdcf6614810a5
def get_thresholds(fc, field):

    means = []
    with arcpy.da.SearchCursor(fc, field) as cursor:
        for row in cursor:
            if row[0]:
                means.append(row[0])

    g_thresh = np.percentile(np.array(means), 3.3)
    r_thresh = np.percentile(np.array(means), 3.3 + 13.3)
    l_thresh = np.percentile(np.array(means), 3.3 + 13.3 + 59.4)

    print(g_thresh)
    print(r_thresh)
    print(l_thresh)

    return g_thresh, r_thresh, l_thresh

# Determines grls score from mean value and thresholds
def get_pop_score(mean, g_thresh, r_thresh, l_thresh):
    """"""
    ratio = mean
    #print("Mean: " + str(ratio))
    if ratio == None:
        return "N/A"
    elif (ratio == 0 or ratio == 0.0):
        return "G"
    elif (ratio > 0 and ratio <= g_thresh):
        return "G"
    elif (ratio > g_thresh and ratio <= r_thresh):
        return "R"
    elif (ratio > r_thresh and ratio <= l_thresh):
        return "L"
    elif (ratio > l_thresh and ratio <= 1000000):
        return "S/U"
    else:
        return 0

# Adds the score to the dataset
def add_score(fc, val_field, score_field, g_thresh, r_thresh, l_thresh):
    with arcpy.da.UpdateCursor(fc, [val_field, score_field]) as cursor:
        for row in cursor:
            #if row[0]:
            row[1] = get_pop_score(row[0], g_thresh, r_thresh, l_thresh)

            cursor.updateRow(row)

    return fc

# Creates the population scored dataset
def create_ancillary_features(grid, pop, output_features):

    gdb = os.path.dirname(output_features)
    print(gdb)

    if arcpy.Exists(gdb):
        print("Geodatabase already exists")
    else:
        print("Creating: " + str(gdb))
        arcpy.CreateFileGDB_management(os.path.dirname(gdb), os.path.basename(gdb))

    zonal_table = os.path.join(gdb, 'zonalstats')  # in_memory\\table"

    output_features = arcpy.CopyFeatures_management(grid, output_features)[0]
    print("Done Copy Features")

    arcpy.gp.ZonalStatisticsAsTable_sa(output_features, "OBJECTID", pop, zonal_table, "DATA", "ALL")
    print("Done Zonal Stats")
    # zonal_oid = arcpy.Describe(zonal_table).OIDFieldName
    arcpy.JoinField_management(output_features, "OBJECTID",
                               zonal_table, "OBJECTID_1",
                               "Count;Area;Min;Max;Range;Variety;Majority;Minority;Median;Mean;Std;Sum")

    score_field = "GRLS_SCORE"
    arcpy.AddField_management(output_features, score_field, "TEXT", field_length=4)

    print("Getting Value Thresholds")
    g_thresh, r_thresh, l_thresh = get_thresholds(output_features, 'MEAN')

    print("Adding Score to Raster")
    add_score(output_features, "MEAN", score_field, g_thresh, r_thresh, l_thresh)

    print("Done.")

    return output_features

# Publish to feature service: Not implemented yet
#def publish_thematic_data():
#
#    return

# Runs prepare_thematic_data tools to create a scored poplation feature class
if __name__ == '__main__':
    grid =
    pop =
    output_features =

    create_ancillary_features(grid, pop, output_features)