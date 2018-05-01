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
import logging
import datetime

import geodataset as geomotion
import arcpy
from arcpy import env
from arcpy import da

if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping
    reload(sys)
    sys.setdefaultencoding('utf8')
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

FIELDS = ('SOURCE_LIST',
          'PRI_SOURCE',
          'PRI_SOURCE_CNT',
          'PRI_SOURCE_PER',
          'SEC_SOURCE',
          'SEC_SOURCE_CNT',
          'SEC_SOURCE_PER')
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
def validate_workspace(wrksp):
    """
    Validates and ensures output workspace exists
    """
    try:
        if wrksp.lower().endswith('.gdb') and \
           os.path.isdir(wrksp) == False:
                return arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(wrksp),
                                                     out_name=os.path.basename(wrksp))[0]
        elif wrksp.lower().endswith('.sde') and \
             os.path.isfile(wrksp) == False:
            raise ValueError("SDE workspace must exist before using it.")
        elif os.path.isdir(wrksp) == False:
            os.makedirs(wrksp)
            return wrksp
        else:
            return wrksp
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "validate_workspace",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )


#--------------------------------------------------------------------------
def extend_table(fc, array):
    """
    extends to append field and data to an existing table
    """
    try:
        da.ExtendTable(fc, "OID@", array, "_ID", False)
        return fc
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                    "function": "extend_table",
                    "line": line,
                    "filename": filename,
                    "synerror": synerror,
                    "arc" : str(arcpy.GetMessages(2))
                    }
            )
#--------------------------------------------------------------------------
def process_source_lineage(grid, data, search_field=None, value_field=None, where_clause=None):
    """
    performs the operation to generate the
    """
    try:
        poly_desc = arcpy.Describe(grid)
        fc_desc = arcpy.Describe(data)
        if poly_desc.extent.within(fc_desc.extent):

            temp_fc = 'in_memory/clip'
            arcpy.AddMessage('Clipping features to polygon')
            arcpy.Clip_analysis(data, grid, temp_fc)
            arcpy.AddMessage('Created in_memory fc')
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(temp_fc,
                    fields=[search_field, value_field],
                    encoding='utf-8')
            arcpy.AddMessage('features read into spatial dataframe after clipping')
        else:
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(data_sdf,
                    fields=[search_field, value_field],
                    encoding='utf-8')
            arcpy.AddMessage('features read into spatial dataframe without clipping')


        grid_sdf = geomotion.SpatialDataFrame.from_featureclass(grid,
                    where_clause=where_clause)

        index = data_sdf.sindex
        results = []
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids = list(index.intersect(ext))
            df_current = data_sdf.loc[data_sdf.index.isin(row_oids)]
            # disjoint == False means intersection with Grid polygon
            # query out the data we need. in this case zi001_sps == 1001
            df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()
            df_sub = df = df_sub.loc[df_sub[search_field] == 1001].copy()

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
                results.append(
                    (
                        int(row[1].OBJECTID),
                        ",".join(df_sub[value_field].unique().tolist()),
                        grp.index[0],
                        int(grp[0]),
                        round(float(grp[0]) * 100.0 / float(len(df_sub)),1),
                        grp.index[1],
                        int(grp[1]),
                        round(float(grp[1]) * 100.0 / float(len(df_sub)),1),
                    )
                )
            elif len(grp) == 0:
                results.append(
                    (int(row[1].OBJECTID),
                     'None',
                     'None',
                     0,
                     float(0),
                     'None',
                     0,
                     float(0))
                )
            elif len(grp) == 1:
                results.append(
                    (
                        int(row[1].OBJECTID),
                        ",".join(df_sub[value_field].unique().tolist()),
                        grp.index[0],
                        int(grp[0]),
                        round(float(grp[0]) * 100.0 / float(len(df_sub)),1),
                        'None',
                        0,
                        float(0)
                    )
                )
            del grp
            del df_sub
            del row_oids
            del df_current
        del grid_sdf
        del data_sdf
        dtypes = np.dtype(
            [
                ('_ID', np.int),
                ('SOURCE_LIST', '|S1024'),
                ('PRI_SOURCE', '|S256'),
                ('PRI_SOURCE_CNT', np.int32),
                ('PRI_SOURCE_PER', np.float64),
                ('SEC_SOURCE', '|S256'),
                ('SEC_SOURCE_CNT', np.int32),
                ('SEC_SOURCE_PER', np.float64)
            ]
        )
        array = np.array(results, dtypes)
        del results
        return array
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                        "function": "process_source_lineage",
                        "line": line,
                        "filename": filename,
                        "synerror": synerror,
                        "arc" : str(arcpy.GetMessages(2))
                    }
            )
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        fcs = argv[0]
        source_field = str(argv[1]).upper()
        value_field = str(argv[2]).upper()
        polygon_grid = argv[3]
        output_fc = argv[4]
        out_fc_exists = arcpy.Exists(output_fc)

        output_gdb, out_name = os.path.split(output_fc)
        #   Local Variables
        #
        scratchGDB = env.scratchGDB
        scratchFolder = env.scratchFolder
        results = []
        source_fields = ['zi001_sdp'.upper(),
                         'zi001_sps'.upper()]
        #  Logic
        #
        if not source_field:
            source_field = source_fields[1]
        if not value_field:
            value_field = source_fields[0]
        #if not output_gdb:
        #    output_gdb = env.scratchGDB
        master_times = datetime.datetime.now()
        fc = fcs
        if not out_fc_exists:
            output_gdb = validate_workspace(wrksp=output_gdb)

            #out_name = "srcLin_%s_%s" % (os.path.basename(fc[:-3]), fc[-3:])
            out_grid = os.path.join(output_gdb, out_name)
            out_grid = arcpy.CopyFeatures_management(polygon_grid, out_grid)[0]

            arcpy.AddMessage("Working on feature class: %s" % os.path.basename(fc))
            array = process_source_lineage(grid=out_grid,
                                           data=fc,
                                           search_field=source_field,
                                           value_field=value_field)
            extend_table(out_grid, array)
            results.append(out_grid)
        else:
            arcpy.MakeFeatureLayer_management(output_fc, "lyr")
            arcpy.SelectLayerByLocation_management("lyr", "HAVE_THEIR_CENTER_IN", polygon_grid)
            oids = [row[0] for row in arcpy.da.SearchCursor("lyr", "OID@")]
            if len(oids) >1:
                oids_string = str(tuple(oids))
            else:
                oids_string = str('('+ str(oids[0]) + ')')

            arcpy.AddMessage("Analyzing " + oids_string)
            arcpy.AddMessage("Working on feature class: %s" % os.path.basename(fc))
            array = process_source_lineage(grid=output_fc,
                                           data=fc,
                                           search_field=source_field,
                                           value_field=value_field,
                                           where_clause='OBJECTID IN ' + oids_string)
            extend_table(output_fc, array)
            results.append(output_fc)

        arcpy.AddMessage("Analysis on feature class: %s finished." % os.path.basename(fc))
        arcpy.AddMessage("Total Time %s" % (datetime.datetime.now() - master_times))
        #arcpy.SetParameterAsText(5, results)
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        arcpy.AddError("ArcPy Error Message: %s" % arcpy.GetMessages(2))
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
        log.error("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except FunctionError as f_e:
        messages = f_e.args[0]
        arcpy.AddError("error in function: %s" % messages["function"])
        arcpy.AddError("error on line: %s" % messages["line"])
        arcpy.AddError("error in file name: %s" % messages["filename"])
        arcpy.AddError("with error message: %s" % messages["synerror"])
        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
        log.error("error in function: %s" % messages["function"])
        log.error("error on line: %s" % messages["line"])
        log.error("error in file name: %s" % messages["filename"])
        log.error("with error message: %s" % messages["synerror"])
        log.error("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        log.error("error on line: %s" % line)
        log.error("error in file name: %s" % filename)
        log.error("with error message: %s" % synerror)
    finally:
        logging.shutdown()
#--------------------------------------------------------------------------
if __name__ == "__main__":
    #env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)