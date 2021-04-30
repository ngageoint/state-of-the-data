import os
import datetime
import numpy as np
import arcpy
from arcpy import da
#from arcgis.features import SpatialDataFrame
from arcgis.features import GeoAccessor as Ga
from arcgis.geometry import Geometry
from arcgis.geometry import Polygon
import field_schema


def buf(geom):
    geom = Geometry(geom).buffer(-.01)
    #print(geom)
    return Polygon(geom)

def custom_buf(geom):
    geom = Geometry(geom).buffer(-.000001)
    #print(geom)
    return Polygon(geom)

def log_update_to_table(gdb, table_name, feature_class, new_or_update, status):
    gdb_and_table = os.path.join(gdb, table_name)
    if arcpy.Exists(gdb_and_table):
        print("Table already exists.")
    else:
        print('Creating table and adding fields')
        arcpy.CreateTable_management(gdb, table_name)
        arcpy.AddField_management(gdb_and_table, "Feature_Class", "TEXT", field_length=48)
        arcpy.AddField_management(gdb_and_table, "New_or_Update", "TEXT", field_length=12) #Status: "New" or "Update"
        arcpy.AddField_management(gdb_and_table, "Status", "TEXT", field_length=12) #"Started" or "Completed"
        arcpy.AddField_management(gdb_and_table, "Last_Update_Time", "DATE")


    print('Counting table entries.')
    # The following inputs are layers or table views: "StatusTable"
    #where_clause = "Feature_Class = 'TransportationGroundCrvs'"
    where_clause = "Feature_Class = " + "'" + feature_class + "'"
    rows = arcpy.SelectLayerByAttribute_management(gdb_and_table, selection_type="NEW_SELECTION",
                                            where_clause=where_clause)
    feature_count = int(arcpy.GetCount_management(rows)[0])
    print('Table entries: ' + str(feature_count))
    if feature_count == 0:

        print('Inserting new entry.')
        # Open an InsertCursor
        cursor = arcpy.da.InsertCursor(gdb_and_table,['Feature_Class', 'New_or_Update', 'Status', 'Last_Update_Time'])

        #row_values = ('TransportationGroundCrvs', 'New', 'Started', datetime.datetime.now())
        row_values = (feature_class, new_or_update, status, datetime.datetime.now())
        cursor.insertRow(row_values)

        del cursor

    elif feature_count == 1:
        print('Updating existing entry.')
        with arcpy.da.UpdateCursor(rows, ['New_or_Update', 'Status', 'Last_Update_Time']) as cursor:
            for row in cursor:
                row[0] = new_or_update
                row[1] = status
                row[2] = datetime.datetime.now()
                cursor.updateRow(row)

    else:
        print("ERROR. Too many rows.")

    return feature_class

# Function inserts new results into the authoritative feature class if the
# feature geometries already exist
def insert_new_features(new_data_df, data_fc, wkspace, schema):

    dtypes = field_schema.dts.get(schema)
    #print(dtypes)

    gadf = Ga.from_featureclass(data_fc)
    #print(gadf.head())

    new_data_df.spatial.sr = gadf.spatial.sr
    gadf.drop(gadf.columns.difference(['SHAPE', 'objectid']), 1, inplace=True)
    new_data_df.drop(['objectid'], 1, inplace=True)


    new_data_df['SHAPE'] = new_data_df.apply(lambda x: buf(x['SHAPE']), axis=1)

    print('Joining results to features.')
    res = Ga(new_data_df).join(gadf)


    if schema != 'them':
        #print('removing: ' + str(['SHAPE','globalid','f_code', 'index_right','st_area(shape)','st_perimeter(shape)']))
        new_insert_df = res.drop(['SHAPE','globalid','f_code', 'index_right'], axis=1,
                                 inplace=False)
        #,'st_area(shape)','st_perimeter(shape)'
    else:
        try:
            #print('removing: ' + str(['SHAPE', 'join_count', 'target_fid', 'f_code',
            #                          'grls_score', 'them_acc_score', 'index_right']))
            new_insert_df = res.drop(['SHAPE', 'join_count', 'target_fid', 'f_code',
                                      'grls_score', 'them_acc_score', 'index_right'], axis=1,
                                     inplace=False)
        except:
            #print("trying this combination")
            new_insert_df = res.drop(['SHAPE', 'globalid', 'f_code', 'index_right'], axis=1,
                                     inplace=False)

    id_col = new_insert_df.pop('objectid')
    new_insert_df.insert(0, 'objectid', id_col)

    records = new_insert_df.to_records(index=False)

    rows = np.array(records, dtype=dtypes)


    try:
        print("trying _ID")
        da.ExtendTable(data_fc, "OID@", rows, "_ID", False)
    except:
        print('Trying objectid')
        da.ExtendTable(data_fc, "OID@", rows, "objectid", False)

    print("FEATURES UPDATED.")


    return data_fc
