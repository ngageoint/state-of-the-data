"""-----------------------------------------------------------------------------
Name: thematic_summary.py
Purpose: Statistically sumarizes the selcted field in a feature class.
Description: Performs a statistical summary of a specific thematic field in a
        table. For example, the field could indicate the type of road
        (highway, motorway, footway, etc.). This field must be a string.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Andrew Chapkowski, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August, 2015
Modified: August 12, 2016 | April, 2017
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
###########################################################################
module = __file__
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG,
                    format='%(name)s (%(levelname)s): %(message)s')
log = logging.getLogger(module)
###########################################################################

FIELDS = ('THEME_LIST',
          'PRI_THEME',
          'PRI_THEME_CNT',
          'PRI_THEME_PER',
          'SEC_THEME',
          'SEC_THEME_CNT',
          'SEC_THEME_PER')
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
def process_source_lineage(grid_sdf, data_sdf, value_field=None):
    """
    performs the operation to generate the
    """
    try:
        subtypes  = arcpy.da.ListSubtypes(data_sdf)
        st_dict = {}
        for stcode, stdict in list(subtypes.items()):
            st_dict[stcode] = subtypes[stcode]['Name']

        fields = arcpy.ListFields(data_sdf)
        use_subtypes = False
        for field in fields:
            if field.name == value_field and field.type == 'Integer':
                arcpy.AddMessage("Field has subtypes")
                use_subtypes = True

        poly_desc = arcpy.Describe(grid_sdf)
        fc_desc = arcpy.Describe(data_sdf)
        if poly_desc.extent.within(fc_desc.extent):

            temp_fc = 'in_memory/clip'
            arcpy.AddMessage('Clipping features to polygon')
            arcpy.Clip_analysis(data_sdf, grid_sdf, temp_fc)
            arcpy.AddMessage('Created in_memory fc')
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(temp_fc,
                                                            fields=[value_field])
            arcpy.AddMessage('features read into spatial dataframe after clipping')
        else:
            data_sdf = geomotion.SpatialDataFrame.from_featureclass(data_sdf, fields=[value_field])
            arcpy.AddMessage('features read into spatial dataframe without clipping')

        grid_sdf = geomotion.SpatialDataFrame.from_featureclass(grid_sdf)
        #data_sdf = geomotion.SpatialDataFrame.from_featureclass(data_sdf, fields=[value_field])
        index = data_sdf.sindex
        results = []
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = row[1].SHAPE
            ext = [geom.extent.lowerLeft.X, geom.extent.lowerLeft.Y,
                   geom.extent.upperRight.X, geom.extent.upperRight.Y]
            row_oids = list(index.intersect(ext))
            df_current = data_sdf.loc[data_sdf.index.isin(row_oids)]
            # disjoint == False means intersection with Grid polygon
            df_sub = df_current.loc[df_current.disjoint(geom) == False].copy()

            df_sub = df_sub.replace({np.nan: "NULL"})

            grp = df_sub.groupby(by=value_field).size() # Get the counts.
            # sort the values to get the biggest on the top
            grp.sort_values(axis=0, ascending=False,
                            inplace=True, kind='quicksort',
                            na_position='last')
            if use_subtypes:
                if len(grp) > 1:
                    grp = grp.head(2)
                    results.append(
                        (
                            int(row[1].OBJECTID),
                            ",".join([st_dict[i] for i in df_sub[value_field].unique().tolist()]),
                            st_dict[grp.index[0]],
                            int(grp[grp.index[0]]),
                            round(float(grp[grp.index[0]]) * 100.0 / float(len(df_sub)),1),
                            st_dict[grp.index[1]],
                            int(grp[grp.index[1]]),
                            round(float(grp[grp.index[1]]) * 100.0 / float(len(df_sub)),1),
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
                            ",".join([st_dict[i] for i in df_sub[value_field].unique().tolist()]),
                            st_dict[grp.index[0]],
                            int(grp[grp.index[0]]),
                            round(float(grp[grp.index[0]]) * 100.0 / float(len(df_sub)),1),
                            'None',
                            0,
                            float(0)
                        )
                    )
            else:
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
                ('THEME_LIST', '|S1024'),
                ('PRI_THEME', '|S256'),
                ('PRI_THEME_CNT', np.int32),
                ('PRI_THEME_PER', np.float64),
                ('SEC_THEME', '|S256'),
                ('SEC_THEME_CNT', np.int32),
                ('SEC_THEME_PER', np.float64)
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
        #fcs = argv[0].split(';')
        #polygon_grid = argv[1]
        #output_gdb = argv[2]
        #value_field = str(argv[3])
        fcs =  str(argv[0]).split(';')
        value_field = str(argv[1])
        polygon_grid = argv[2]
        output_fc = argv[3]

        output_gdb, out_name = os.path.split(output_fc)

        #   Local Variables
        #
        scratchGDB = env.scratchGDB
        scratchFolder = env.scratchFolder
        results = []
        #  Logic
        #

        if not output_gdb:
            output_gdb = env.scratchGDB
        master_times = datetime.datetime.now()
        output_gdb = validate_workspace(wrksp=output_gdb)
        for fc in fcs:
            #out_name = "srcLin_%s_%s" % (os.path.basename(fc[:-3]), fc[-3:])
            out_grid = os.path.join(output_gdb, out_name)
            out_grid = arcpy.CopyFeatures_management(polygon_grid, out_grid)[0]
            arcpy.AddMessage("Working on feature class: %s" % os.path.basename(fc))
            array = process_source_lineage(grid_sdf=out_grid,
                                           data_sdf=fc,
                                           value_field=value_field)
            extend_table(out_grid, array)
            results.append(out_grid)
            arcpy.AddMessage("Analysis on feature class: %s finished." % os.path.basename(fc))
        arcpy.AddMessage("Total Time %s" % (datetime.datetime.now() - master_times))
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
    env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)