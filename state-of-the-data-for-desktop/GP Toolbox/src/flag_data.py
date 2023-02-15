"""-----------------------------------------------------------------------------
Name: flag_data.py
Purpose: Flag selected attribute that are NULL.
Description: This tool searches for and flags input features that have NULL
        values in its attribute table. The NULL values can be user defined
        (for example, -999999, "No Information", etc.).
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: March, 2017
Upated: April, 2017
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""

import os
import sys
import platform
import itertools
import numpy as np
import pandas as pd
import arcpy
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping

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
def grouper_it(n, iterable):
    """
    creates chunks of cursor row objects to make the memory
    footprint more manageable
    """
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
#--------------------------------------------------------------------------
def calc_chunk_size():
    """determines the chunk size based on 32 vs 64-bit python"""
    try:
        if platform.architecture()[0].lower() == "32bit":
            return 500000
        else:
            return 5000000
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                    "function": "calc_chunk_size",
                    "line": line,
                    "filename": __file__,
                    "synerror": synerror,
                    "arc" : str(arcpy.GetMessages(2))
                }
        )
def copy_rows(source, destination):
    """
    copies a dataset from source to a destination
    """
    try:
        if arcpy.Exists(destination):
            arcpy.Delete_management(destination)
        desc = arcpy.Describe(source)
        fields = [field.name for field in arcpy.ListFields(source) if field.type not in ['Geometry', 'OID@']]
        if desc.datasetType.lower() == 'table':
            out_table = arcpy.CreateTable_management(out_path=os.path.dirname(destination),
                                                     out_name=os.path.basename(destination),
                                                    template=source)[0]
        elif desc.datasetType.lower() == 'featureclass':
            out_table = arcpy.CreateFeatureclass_management(out_path=os.path.dirname(destination),
                                                            out_name=os.path.basename(destination),
                                                            geometry_type=desc.shapeType.upper(),
                                                            template=source,
                                                            spatial_reference=desc.spatialReference)[0]
            fields.append("SHAPE@")
        else:
            raise Exception("Invalid datatype")
        with arcpy.da.SearchCursor(source, fields) as rows:
            with da.InsertCursor(out_table, fields) as irows:
                for row in rows:
                    irows.insertRow(row)
                    del row
                del irows
            del rows
        return out_table
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                    "function": "copy_rows",
                    "line": line,
                    "filename": __file__,
                    "synerror": synerror,
                    "arc" : str(arcpy.GetMessages(2))
                }
        )

#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        fc = argv[0]
        find_value = argv[1] #None #
        fields =str(argv[2]).split(';') #str("name;name_en").split(";") #
        copy_fc = argv[3]

        feat_count = arcpy.GetCount_management(fc)
        index = int(feat_count.getOutput(0))-1

        text_fields = ",".join(fields)
        flag_field = "FLAGGED_DATA"
        oid_field = None
        scratchGDB = env.scratchGDB
        #copy_fc = os.path.join(scratchGDB, os.path.basename(fc))
        isnumber = False
        #  Logic
        #
        if find_value is None or \
           find_value == "" or \
           str(find_value).lower() == "none":
            find_value = None
        elif find_value.startswith("-") and \
           find_value.find(".") < -1 and \
           find_value[1:].isdigit():
            find_value = int(find_value)
            isnumber = True
        elif find_value.find(".") < -1 and \
             find_value.isdigit():
            find_value = int(find_value)
            isnumber = True
        if isnumber ==  False:
            try:
                find_value = float(find_value)
                isnumber = True
            except:
                isnumber = False
        if arcpy.Exists(copy_fc):
            arcpy.Delete_management(copy_fc)
        copy_fc = copy_rows(source=fc, destination=copy_fc)# Instead of arcpy.CopyFeatures()
        #arcpy.CopyFeatures_management(fc, copy_fc)
        #if check_shape:
        #    fields.append("SHAPE@")
        if oid_field is None:
            oid_field = arcpy.Describe(fc).OIDFieldName
            if oid_field not in fields:
                fields.append(oid_field)
        chunk_size = calc_chunk_size()
        count = 0
        dtypes = np.dtype([('_ID', np.int),
                           ('NULL_COLUMNS', '|S255'),
                           ('FLAG_FIELDS', '|S255'),
                           ('NULL_COUNT', np.int32),
                           ('FLAGGED_DATA', np.int32)])
                           #('SELECTED_FIELDS', '|S1024'),
                           #('FLAGGED_FIELDS', '|S1024'),
        with da.SearchCursor(copy_fc, fields,
                             where_clause=None) as cursor:
            search_fields = [field for field in cursor.fields if field != oid_field]
            for group in grouper_it(chunk_size, cursor):
                flagged_fields = []
                count += 1
                df = pd.DataFrame.from_records(group, columns=cursor.fields)
                df['NULL_COLUMNS'] = df.apply(lambda x: ','.join(x[x.isin([find_value])].index),axis=1)
                #df.apply(lambda x: ','.join(x[x.isnull()].index),axis=1)#
                df['FLAG_FIELDS'] = text_fields
                #df['SELECTED_FIELDS'] = text_fields
                #df['FLAGGED_FIELDS'] = text_fields
                if find_value is None:
                    df['NULL_COUNT'] = len(df.columns) - df.count(axis=1)
                elif find_value and isnumber:
                    df['NULL_COUNT'] = (df[fields] == find_value).sum(axis=1)
                elif find_value and isnumber == False:
                    df['NULL_COUNT'] = (df[fields] == find_value).sum(axis=1)
                    df['NULL_COUNT'] = df[fields].isin([find_value]).sum(axis=1)
                df[flag_field] = 0
                df.loc[df['NULL_COUNT'] > 0, flag_field] = 1
                for fld in search_fields:
                    del df[fld]

                array = df.to_records(index=False).tolist()
                array = np.array(array, dtypes)
                da.ExtendTable(copy_fc,oid_field,
                               array, '_ID',
                               False)
                del group
                del df
                del array
        #arcpy.SetParameterAsText(3, copy_fc)
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