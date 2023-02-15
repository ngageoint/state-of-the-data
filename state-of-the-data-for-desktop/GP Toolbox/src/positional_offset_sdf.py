"""-----------------------------------------------------------------------------
Name: positional_offset_sdf.py
Purpose: Determines the positional offset of a line feature class to another
        line feature class. This tool also summarizes those results at a grid
        cell level. Uses uses the spatial dataframe to perform all processing.
Description: This python script tool will calculate the positional offset
        between two sets of curve features (for example, your road features
        and Open Street Map roads) and then summarize the scores on a given
        polygon grid. This tool uses a spatial dataframe to perform all
        calculations.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: April, 2017
Modified:
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

import os
import sys

import arcpy
from arcpy import env
from arcpy import da

import numpy as np
import pandas as pd
from geodataset import SpatialDataFrame

import datetime

from __future__ import division

#--------------------------------------------------------------------------
class FunctionError(Exception):
    """ raised when a function fails to run """
    pass
#--------------------------------------------------------------------------
def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, __file__, synerror

#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        tds_features = argv[0]
        in_fields = str(argv[1]).upper()#'zi001_vsn'.upper()
        comparative_features = argv[2]
        polygon_grid = argv[3]
        output_gdb = argv[4]
        distanceTolerance = float(argv[5])#25 #assumes meters
        sample_size = int(argv[6]) #2 #if 100 or less, is a percent. if 101 or greater, is a number of features
        #  Local Variables
        #
        #output_features = output_grid + "_lines"
        sr = arcpy.SpatialReference(3857)
        is_percentage = False
        results = []
        #  Logic
        #
        import datetime
        start = datetime.datetime.now()
        master_times = datetime.datetime.now()
        arcpy.AddMessage("Copying Features.")
        tds_features = arcpy.CopyFeatures_management(tds_features,
                                                     os.path.join(output_gdb,
                                                                  "PositionalOffset_Roads")
                                                     )[0]
        polygon_grid = arcpy.CopyFeatures_management(polygon_grid,
                                                    os.path.join(output_gdb,
                                                        "PositionalOffset_Grid")
                                                    )[0]
        arcpy.AddMessage("Total Time to Copy %s" % (datetime.datetime.now() - master_times))
        desc = arcpy.Describe(tds_features)
        efields = ['SHAPE', 'OID', 'FID', 'OBJECTID', 'SHAPE_LENGTH']

        efields = efields + in_fields.upper().split(';')
        if hasattr(desc, 'OIDFieldName'):
            efields.append(desc.OIDFieldName)
        if hasattr(desc, 'lengthFieldName'):
            efields.append(desc.lengthFieldName)
        if hasattr(desc, 'areaFieldName'):
            efields.append(desc.areaFieldName)
        for f in [field.name for field in arcpy.ListFields(tds_features)]:
            if f.upper() not in efields:
                try:
                    arcpy.DeleteField_management(tds_features, f)
                    arcpy.AddMessage(" Deleted " + f)
                except:
                    arcpy.AddMessage("Failed to Delete " + f)
            del f
        del desc, efields
        if sample_size <= 100:
            sample_size = sample_size/100
            is_percentage = True
        tds_sdf = SpatialDataFrame.from_featureclass(tds_features, sr=sr)
        if is_percentage:
            sample_size = int(len(tds_sdf) * sample_size)
        sampled_sdf = tds_sdf.sample(n=sample_size).copy()
        cols = { c : c.upper() for c in sampled_sdf.columns}
        sampled_sdf.rename(columns=cols, inplace=True)
        # Reproject to SR specified above.
        #
        sampled_sdf.geometry = sampled_sdf.geometry.projectAs(sr)
        comparison_sdf = SpatialDataFrame.from_featureclass(comparative_features, sr=sr)
        cols = { c : c.upper() for c in comparison_sdf.columns}
        comparison_sdf.rename(columns=cols, inplace=True)
        #comparison_sdf.geometry = comparison_sdf.geometry.projectAs(sr)
        grid_sdf = SpatialDataFrame.from_featureclass(polygon_grid, sr=sr)
        cols = { c : c.upper() for c in grid_sdf.columns}
        grid_sdf.rename(columns=cols, inplace=True)
        #grid_sdf.geometry = grid_sdf.geometry.projectAs(sr)
        grid_sdf['MEAN_CE90'] = 0.0
        grid_sdf['OFFSET_METERS'] = 0.0
        grid_sdf['OSM_MATCH'] = ""
        compare_sindex = comparison_sdf.sindex
        sample_sindex = sampled_sdf.sindex
        counter = 0
        for idx, row in grid_sdf.iterrows():
            print('Processing Grid Number ' + str(counter))
            arcpy.AddMessage('Processing Grid Number ' + str(counter))
            counter=counter+1
            g = row['SHAPE']
            oid = row['OBJECTID']
            ext = [g.extent.XMin, g.extent.YMin,
                   g.extent.XMax, g.extent.YMax]
            # extract select_compare_df
            select_compare_df = comparison_sdf.loc[compare_sindex.intersect(ext)] # OSM Source
            q = select_compare_df.geometry.disjoint(g) == False
            select_compare_df = select_compare_df[q].copy()
            # extract sample df rows
            select_sample_df = sampled_sdf.loc[sample_sindex.intersect(ext)] # source NGA
            q = select_sample_df.geometry.disjoint(g) == False
            select_sample_df = select_sample_df[q].copy()
            # Find distances to each other.
            for idx_s, row_s in select_sample_df.iterrows():
                geom = row_s['SHAPE']
                sample_oid = row_s['OBJECTID']
                print('Processing Feature ' + str(sample_oid))
                arcpy.AddMessage('Processing Feature ' + str(sample_oid))
                #geom = geom.as_arcpy
                first_point = arcpy.PointGeometry(row_s['SHAPE'].firstPoint, sr)
                last_point = arcpy.PointGeometry(row_s['SHAPE'].lastPoint, sr)
                source_angle = (first_point.angleAndDistanceTo(last_point)[0] + 360) % 360
                buffer_shape = geom.buffer(distanceTolerance)
                q = select_compare_df.geometry.disjoint(buffer_shape) == False
                for idx_c, row_c in select_compare_df[q].iterrows():
                    geom_clipped = row_c['SHAPE'].intersect(geom.buffer(distanceTolerance), 2)
                    if geom_clipped and geom_clipped.length > 0:
                        first_point = arcpy.PointGeometry(geom_clipped.firstPoint, sr)
                        last_point = arcpy.PointGeometry(geom_clipped.lastPoint, sr)
                        comparative_angle = (first_point.angleAndDistanceTo(last_point)[0] + 360) % 360

                        if (
                            (comparative_angle <= source_angle + 7.5 and comparative_angle >= source_angle - 7.5) or
                            (comparative_angle <= source_angle + 180 + 7.5 and comparative_angle >= source_angle + 180 - 7.5) or
                            (comparative_angle <= source_angle - 180 + 7.5 and comparative_angle >= source_angle - 180 - 7.5)):
                            near_distance = geom.distanceTo(geom_clipped)
                            #if offset_meters < 0 or near_distance < offset_meters:
                                #offset_meters = near_distance
                            results.append((sample_oid, row_c['OBJECTID'], oid, "YES", near_distance)) # OID of Source (NGA), OID of Grid, Yes, distance to compare feature
                    del idx_c, row_c
                del idx_s
                del row_s
                del geom
                del q
                del buffer_shape

            del ext
            del idx, row
        print (datetime.datetime.now() - start)
        dtypes = np.dtype([('SAMPLE_OID', np.int), # SAMPLE OID
                           ('COMPARE_OID', np.int), # COMPARE OID
                           ('GRID_OID', np.int), # GRID OID
                           ('NEAROSM', '|S255'), #
                           ('FEATURE_DISTANCE', np.float64)])
        array = np.array(results, dtype=dtypes)
        df = pd.DataFrame(data=array, columns=['SAMPLE_OID', 'COMPARE_OID', 'GRID_OID',
                                               'NEAROSM', 'FEATURE_DISTANCE'])
        del df['NEAROSM']
        print ("join average grid distance to grid")
        sample_mean_distance = df.groupby(by='GRID_OID', as_index=False)['FEATURE_DISTANCE'].mean().copy()
        sample_mean_distance['MEAN_CE90'] = sample_mean_distance['FEATURE_DISTANCE']
        sample_mean_distance = sample_mean_distance.to_records(index=False)
        dt1 = np.dtype([('GRID_OID', '<i4'), ('MEAN_CE90', '<f8')])
        join_sample = np.array(sample_mean_distance, dtype=dt1)
        oidName = arcpy.Describe(polygon_grid).oidFieldName
        da.ExtendTable(in_table=polygon_grid,
                       table_match_field=oidName,
                       in_array=join_sample,
                       array_match_field="GRID_OID",
                       append_only=False)
        del sample_mean_distance
        del oidName
        del join_sample
        del dt1
        print ("join closest distance to tds_features")
        dt2 = np.dtype([('SAMPLE_OID', '<i4'), ('FEATURE_DISTANCE', '<f8')])
        tds_join_data = df.groupby(by='SAMPLE_OID', as_index=False)['FEATURE_DISTANCE'].min().copy()
        tds_join_data = tds_join_data.to_records(index=False)
        join_sample = np.array(tds_join_data, dtype=dt2)
        oidName = arcpy.Describe(tds_features).oidFieldName
        da.ExtendTable(in_table=tds_features,
                           table_match_field=oidName,
                           in_array=join_sample,
                           array_match_field="SAMPLE_OID",
                           append_only=False)
        del dt2
        del tds_join_data
        del join_sample
        del oidName
        print ('return results')
        arcpy.SetParameterAsText(7, polygon_grid)
        arcpy.SetParameterAsText(8, tds_features)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        arcpy.AddError("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except FunctionError as f_e:
        messages = f_e.args[0]
        arcpy.AddError("error in function: %s" % messages["function"])
        arcpy.AddError("error on line: %s" % messages["line"])
        arcpy.AddError("error in file name: %s" % messages["filename"])
        arcpy.AddError("with error message: %s" % messages["synerror"])
        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
#--------------------------------------------------------------------------
if __name__ == "__main__":
    env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)