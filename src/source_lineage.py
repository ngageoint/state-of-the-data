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
#import logging
import datetime

#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import create_selection_layers as csl
import sotd_config as config

###########################################################################
module = __file__
#logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
#                    format='%(name)s (%(levelname)s): %(message)s')
#log = logging.getLogger(module)
#log.diabled = True
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

def main():
    """ main driver of program """
    try:
        master_times = datetime.datetime.now()

        gis = GIS(config.portal, config.un, config.pw)

        fc = config.features_url
        polygon_grid = config.grid_url
        output_features = config.source_lineage_url

        search_field = 'zi001_sdp'.upper() #str(argv[1]).upper()
        value_field  = 'zi001_sps'.upper() #str(argv[2]).upper()

        return_all_records = False
        look_back_days = config.look_back_days

        dates = csl.get_dates_in_range(look_back_days)
        where_clause = csl.form_query_string(dates)

        grid_fl = FeatureLayer(url=polygon_grid)
        grid_sdf = grid_fl.query(return_all_records=False, where=where_clause).df

        geometry = grid_sdf.geometry
        sr = {'wkid':4326}
        sp_rel = "esriSpatialRelIntersects"

        results = []
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            ext = [geom.extent.lowerLeft.X+.1, geom.extent.lowerLeft.Y+.1,
                   geom.extent.upperRight.X-.1, geom.extent.upperRight.Y-.1]

            new_geom = Geometry({
                "rings" : [[[geom.extent.upperRight.X-.1, geom.extent.lowerLeft.Y+.1], [geom.extent.lowerLeft.X+.1, geom.extent.lowerLeft.Y+.1], [geom.extent.lowerLeft.X+.1, geom.extent.upperRight.Y-.1], [geom.extent.upperRight.X-.1, geom.extent.upperRight.Y-.1], [geom.extent.upperRight.X-.1, geom.extent.lowerLeft.Y+.1]]],
                "spatialReference" : {"wkid" : 4326}
            })

            print(new_geom.extent)
            grid_filter = filters._filter(new_geom, sr, sp_rel)
            sp_filter = filters._filter(geom, sr, sp_rel)

            out_fl = FeatureLayer(url=output_features)
            out_sdf = out_fl.query(geometry_filter=grid_filter,return_geometry=True,
                return_all_records=True).df

            print(out_sdf)

            data_fl = FeatureLayer(url=fc)
            data_sdf = data_fl.query(out_fields=",".join([search_field, value_field]),
                geometry_filter=sp_filter,return_geometry=True,
                return_all_records=return_all_records).df

            print("copying data_sdf to current_df")
            df_current = data_sdf

            df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()
            df_sub = df_sub.loc[df_sub[search_field] == 1001].copy()

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

            out_sdf_as_featureset = out_sdf.to_featureset()
            print(out_sdf_as_featureset)
            out_fl.edit_features(updates=out_sdf_as_featureset)

            del grp
            del df_sub
            del df_current

        print("Total Time %s" % (datetime.datetime.now() - master_times))
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
#--------------------------------------------------------------------------
if __name__ == "__main__":
    main()