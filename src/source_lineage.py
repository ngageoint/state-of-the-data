"""-----------------------------------------------------------------------------
Name: source_lineage.py
Purpose: Summarizes the source information of a given feature class.
Description: Creates a statistical summary of the source field. The source
        field my contain the name of the editor, the program the data was
        collected under, or the organization that collected the data and must
        contain a string.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: October, 2015
Modified: July, 2016 | April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

from __future__ import division
from __future__ import print_function
import os
import sys
import numpy as np
import datetime

#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import sotd_config as config

###########################################################################
module = __file__
###########################################################################

FIELDS = ('SOURCE_LIST',
          'PRI_SOURCE',
          'PRI_SOURCE_CNT',
          'PRI_SOURCE_PER',
          'SEC_SOURCE',
          'SEC_SOURCE_CNT',
          'SEC_SOURCE_PER')

##        dtypes = np.dtype(
##            [
##                ('_ID', np.int),
##                ('SOURCE_LIST', '|S1024'),
##                ('PRI_SOURCE', '|S256'),
##                ('PRI_SOURCE_CNT', np.int32),
##                ('PRI_SOURCE_PER', np.float64),
##                ('SEC_SOURCE', '|S256'),
##                ('SEC_SOURCE_CNT', np.int32),
##                ('SEC_SOURCE_PER', np.float64)
##            ]

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

def source_lineage_by_grids(gis, input_features, output_features, search_field, value_field, search_val=1001):
    try:

        out_fl = FeatureLayer(gis=gis,url=output_features)
        out_sdf = out_fl.query(return_geometry=True, return_all_records=True).df

        print(out_sdf)

        sr = {'wkid':4326}
        sp_rel = "esriSpatialRelIntersects"

        for idx, row in enumerate(out_sdf.iterrows()):
            print(idx)
            geom = row[1].SHAPE

            sp_filter = filters._filter(geom, sr, sp_rel)

            data_fl = FeatureLayer(url=input_features)
            #out_fields=in_fields,
            df_sub = data_fl.query(geometry_filter=sp_filter,
                return_geometry=True,
                return_all_records=False,
                out_fields = ",".join([search_field, value_field])).df

            if len(df_sub)>0:

                #print(df_sub.head())

                #df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()
                #df_sub = df = df_sub.loc[df_sub[search_field] == search_val].copy()

                df_sub = df_sub.replace({np.nan: "NULL"})

                grp = df_sub.groupby(by=value_field).size() # Get the counts.
                #print(grp)

                #print(df_sub.head())

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

                #test = df_sub[value_field].unique().tolist()
                #print(",".join(test))

                if len(grp) > 1:
                    grp = grp.head(2)
                    out_sdf.set_value(idx,FIELDS[0],",".join(filter(None, df_sub[value_field].unique().tolist())))
                    out_sdf.set_value(idx,FIELDS[1],grp.index[0])
                    out_sdf.set_value(idx,FIELDS[2],int(grp[0]))
                    out_sdf.set_value(idx,FIELDS[3],float(grp[0]) * 100.0 / float(len(df_sub)))
                    out_sdf.set_value(idx,FIELDS[4],grp.index[1])
                    out_sdf.set_value(idx,FIELDS[5],int(grp[1]))
                    out_sdf.set_value(idx,FIELDS[6],float(grp[1]) * 100.0 / float(len(df_sub)))

                elif len(grp) == 0:
                    out_sdf.set_value(idx,FIELDS[0],'None')
                    out_sdf.set_value(idx,FIELDS[1],'None')
                    out_sdf.set_value(idx,FIELDS[2],0)
                    out_sdf.set_value(idx,FIELDS[3],float(0))
                    out_sdf.set_value(idx,FIELDS[4],'None')
                    out_sdf.set_value(idx,FIELDS[5],0)
                    out_sdf.set_value(idx,FIELDS[6],float(0))

                elif len(grp) == 1:
                    out_sdf.set_value(idx,FIELDS[0],",".join(filter(None, df_sub[value_field].unique().tolist())))
                    out_sdf.set_value(idx,FIELDS[1],grp.index[0])
                    out_sdf.set_value(idx,FIELDS[2],int(grp[0]))
                    out_sdf.set_value(idx,FIELDS[3],float(grp[0]) * 100.0 / float(len(df_sub)))
                    out_sdf.set_value(idx,FIELDS[4],'None')
                    out_sdf.set_value(idx,FIELDS[5],0)
                    out_sdf.set_value(idx,FIELDS[6],float(0))
            else:
                print("No Data")

        return out_sdf, out_fl

    except FunctionError as f_e:
        messages = f_e.args[0]
        print('EXCEPTION HIT')

def source_lineage(gis, df_current, output_features, grid_filter, geom, search_field, value_field, search_val=1001):
    """ main driver of program """
    try:

        out_fl = FeatureLayer(gis=gis,url=output_features)
        out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
            return_all_records=True).df

        df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()

        if search_field:
            df_sub = df_sub.loc[df_sub[search_field] == search_val].copy()

        df_sub = df_sub.replace({np.nan: "NULL"})

        grp = df_sub.groupby(by=value_field).size() # Get the counts.
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
            out_sdf[FIELDS[0]][0]=",".join(df_sub[value_field].unique().tolist())
            out_sdf[FIELDS[1]][0]=grp.index[0]
            out_sdf[FIELDS[2]][0]=int(grp[0])
            out_sdf[FIELDS[3]][0]=float(grp[0]) * 100.0 / float(len(df_sub))
            out_sdf[FIELDS[4]][0]=grp.index[1]
            out_sdf[FIELDS[5]][0]=int(grp[1])
            out_sdf[FIELDS[6]][0]=float(grp[1]) * 100.0 / float(len(df_sub))

        elif len(grp) == 0:
            out_sdf[FIELDS[0]][0]='None'
            out_sdf[FIELDS[1]][0]='None'
            out_sdf[FIELDS[2]][0]=0
            out_sdf[FIELDS[3]][0]=float(0)
            out_sdf[FIELDS[4]][0]='None'
            out_sdf[FIELDS[5]][0]=0
            out_sdf[FIELDS[6]][0]=float(0)

        elif len(grp) == 1:
            out_sdf[FIELDS[0]][0]=",".join(df_sub[value_field].unique().tolist())
            out_sdf[FIELDS[1]][0]=grp.index[0]
            out_sdf[FIELDS[2]][0]=int(grp[0])
            out_sdf[FIELDS[3]][0]=float(grp[0]) * 100.0 / float(len(df_sub))
            out_sdf[FIELDS[4]][0]='None'
            out_sdf[FIELDS[5]][0]=0
            out_sdf[FIELDS[6]][0]=float(0)

##        out_sdf_as_featureset = out_sdf.to_featureset()
##        print(out_sdf_as_featureset)
##        out_fl.edit_features(updates=out_sdf_as_featureset)

        return out_sdf, out_fl

    except FunctionError as f_e:
        messages = f_e.args[0]
        #log.error("error in function: %s" % messages["function"])
        #log.error("error on line: %s" % messages["line"])
        #log.error("error in file name: %s" % messages["filename"])
        #log.error("with error message: %s" % messages["synerror"])
        #log.error("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        #log.error("error on line: %s" % line)
        #log.error("error in file name: %s" % filename)
        #log.error("with error message: %s" % synerror)
    ##finally:
        ##logging.shutdown()
