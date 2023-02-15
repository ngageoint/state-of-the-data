"""-----------------------------------------------------------------------------
Name: attribution_checker_enterprise.py
Purpose: This tool checks a list of attributes to determine whether they are
        null.
Description: This tool runs the attribution check for a given mission or
        product type (i.e., HADR, Routing, etc.). This tool acts in a similar
        way to GAIT in that it seeks to identify features whose attribute
        values are either: -999999, '', None, 'noInformation', 'None', 'Null',
        'NULL', -999999.0. For a given product type or mission type, the
        script reads from a Microsoft Excel file that lists the necessary
        attribution and for each feature counts the number of attribution
        deficiencies and lists out the attributes that are deficient.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Aaron Frye, National Geospatial-Intelligence Agency (NGA) |
        Gregory Brunner, Contractor for NGA |
        Andrew Chapkowski, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: October 5, 2015
Modified By: Tim Hord, NGA |
        Gregory Brunner, Contractor for NGA |
        Andrew Chapkowski, Contractor for NGA
Copyright: Esri
License: TBD
-----------------------------------------------------------------------------"""
import os
import ast
import sys
import time
import xlrd
import numpy as np
try:
    import pandas as pd
    has_pandas = True
except ImportError:
    has_pandas = False
import datetime
import platform
import itertools
import traceback
from xlrd.sheet import ctype_text

import arcpy
from arcpy import env
from arcpy import da


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
if __file__ is None:
    __file__ =  os.path.basename(sys.argv[0])
#--------------------------------------------------------------------------
def assemble_sql(field_name, field_type=None, not_in=False):
    """
    Creates a sql statement to find empty values
    """
    try:
        if not_in:
            sql = "{field_name} not in ({values})"
        else:
            sql = "{field_name} in ({values})"

        if field_type.lower() in ('short', 'integer', 'long',
                                  'double', 'float'):
            return sql.format(field_name=field_name,
                              values="NULL, -999999, -999999.0")
        else:
            return sql.format(field_name=field_name,
                              values="NULL, 'noInformation', 'Null', 'NULL', 'None', ''")

    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "assemble_sql",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
#-------------------------------------------------------------------------------
def get_field_alias(fc):
    fields = arcpy.ListFields(fc)

    field_dict = {}
    for field in fields:
        field_dict[field.name] = field.aliasName

    return field_dict

#-------------------------------------------------------------------------------
def get_fc_domains(gdb):
    domains = da.ListDomains(gdb)
    domain_dict = {}
    for domain in domains:
        if 'FCODE' in domain.name:
            domain_dict.update(domain.codedValues)

    return domain_dict

#-------------------------------------------------------------------------------
def create_error_fc(outFC, geometryType, sr=None):
    """
    Creates an error feature class for a given geometry type and
    spatial reference.

    Output:
       path to a feature class
    """
    try:
        if arcpy.Exists(outFC):
            arcpy.AddMessage("Error Feature Class Already Exists, Recreating...")
            arcpy.Delete_management(outFC)
        arcpy.CreateFeatureclass_management(out_path=os.path.dirname(outFC),
                                            out_name=os.path.basename(outFC),
                                           geometry_type=geometryType.upper(),
                                           spatial_reference=sr)
        narray = np.array([],
                             np.dtype([('_ID', np.int),# _ID will not be included in the extend
                                       ('DEFICIENCY', '|S1024'),
                                       ('FEATURE_CLASS', '|S254'),
                                       ('SUBTYPE', '|S254'),
                                       ('ORIG_OID', np.int),
                                       ('DEFICIENCY_CNT', np.int)
                                       ])
                             )
        da.ExtendTable(outFC,
                       "OID@",
                       narray,
                       "_ID")
        return outFC
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
#--------------------------------------------------------------------------
def create_attr_dict(filename, check):
    """Creates and attribute dictionary"""
    try:
        xl_workbook = xlrd.open_workbook(filename)
        specificAttributeString = '{'
        specificAttributeDict = {}
        xl_sheet = xl_workbook.sheet_by_name(check)
        for row in range(xl_sheet.nrows):
            if row>0:
                cell = xl_sheet.cell(row,8)
                specificAttributeString += cell.value
        specificAttributeDict = ast.literal_eval(specificAttributeString[:-1] + '}')
        return specificAttributeDict, check
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "create_attr_dict",
                "line": line,
                "filename": __file__,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
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
#--------------------------------------------------------------------------
def unique_values(table, field):
    """gets a list of unique values from a table's column
    table: path to a table
    field: string of a field name

    output:
       list
    """
    try:
        if has_pandas:
            uvalues = None
            chunk_size = calc_chunk_size()
            with da.SearchCursor(table, [field]) as cursor:
                for group in grouper_it(chunk_size, cursor):
                    df = pd.DataFrame.from_records(group, columns=cursor.fields)
                    column = df[field].unique()
                    if uvalues is None:
                        uvalues = column
                    else:
                        uvalues = np.concatenate([column, uvalues])
                    del group
                    del df
                    del column
                del cursor
            if uvalues is None:
                return []
            return list(set(uvalues.tolist()))
        else:
            desc = arcpy.Describe(table)
            if desc.hasOID:
                oidFieldname = desc.OIDFieldName
            else:
                raise Exception("Table must have an object id table")
            template = da.FeatureClassToNumPyArray(table, [field],
                                                   where_clause="{ofield} < 1".format(ofield=oidFieldname))
            uvalues = None
            chunk_size = calc_chunk_size()
            with da.SearchCursor(table, [field]) as cursor:
                for group in grouper_it(chunk_size, cursor):
                    df = np.fromiter(group,template.dtype,-1)
                    column = np.unique(df[field])
                    if uvalues is None:
                        uvalues = column
                    else:
                        uvalues = np.unique(np.concatenate([column, uvalues]))
            if uvalues is None:
                return []
            return list(set(uvalues.tolist()))
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                    "function": "unique_values",
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
        #   User Inputs
        #
        inputFC = argv[0]
        #inputFD = os.path.split(inputFC)[0]
        desc = arcpy.Describe(inputFC)
        sr = desc.spatialReference
        try:
            inputFD = desc.featureClass.path #argv[1]#
            fc = decs.featureClass.name

        except:
            inputFD = desc.path #argv[1]#
            fc = desc.name

        #Spreedsheet
        filename = argv[1]#
        #Check Type
        tabname = argv[2]#
        #Output GDB
        output_fcs = argv[3]


        alias_table = get_field_alias(inputFC)
        arcpy.AddMessage(alias_table)
        arcpy.AddMessage(os.path.dirname(inputFD))
        try:
            fc_domain_dict = get_fc_domains(os.path.dirname(inputFD))
        except:
            #fc_domain_dict = get_fc_domains(r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\TDS\TDS_6_1_MNG_FGCM_sub1.gdb')
            arcpy.AddMessage("Attribution Assessment not configured to pull "
                + "domain dictionary from service. Please add a path to a TDS"
                + " feature class here.")
            exit(0)
        arcpy.AddMessage(os.path.dirname(inputFD))
        arcpy.AddMessage(fc_domain_dict)

        outputGDB = os.path.dirname(output_fcs) #argv[3]#
        #  Local Variables
        #
        error_fcs = {}
        empty = (-999999, '', None, 'noInformation',
                 'None', 'Null', 'NULL', -999999.0)
        #  Logic
        #
        now = datetime.datetime.now()
        if outputGDB is None or \
           outputGDB == "" or \
           outputGDB == "#":
            outputGDB = env.scratchGDB
        if arcpy.Exists(outputGDB) == False:
            arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(outputGDB),
                                          out_name=os.path.basename(outputGDB))
        arcpy.AddMessage("Beginning null attribute check.")
        env.workspace = inputFD
        specificAttributeDict, attrCheck = create_attr_dict(filename, tabname)
        #desc = arcpy.Describe(inputFD)
##        if desc.dataType.lower() == "FeatureDataset".lower():
##            sr = arcpy.Describe(inputFD).spatialReference
##        else:
##            sr = None
        error_fc = output_fcs
        error_fc = create_error_fc(output_fcs,'POLYLINE',sr=sr)
        del sr
        edit = da.Editor(outputGDB)
        edit.startEditing(False, True)
        edit.startOperation()
        crvInsert = da.InsertCursor(error_fc,
                                    ["SHAPE@", "DEFICIENCY", "FEATURE_CLASS",
                                     "SUBTYPE", "ORIG_OID", "DEFICIENCY_CNT"])

##-----------------
        arcpy.AddMessage("Looking at: %s" % output_fcs)
        stList = unique_values(inputFC,"F_CODE")
        errorCount = 0
        if len(stList) > 0 :
            field_names_lookup = {field.name : field.type \
                                  for field in arcpy.ListFields(inputFC) \
                                  if field.type not in ['Blob', 'Geometry', 'OID', 'Raster']}
            field_names_lookup['SHAPE@'] = 'Geometry'
            field_names_lookup['OID@'] = 'OID'
            for s in stList:
                if s in specificAttributeDict:
                    sub_sql = " or ".join([assemble_sql(field_name=f,
                                            field_type=field_names_lookup[f]) \
                                           for f in specificAttributeDict[s] ])
                    sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=sub_sql)
                    with da.SearchCursor(inputFC,
                                         field_names_lookup.keys(),
                                         where_clause=sql) as rows:
                        index_lookup = None
                        for row in rows:
                            if index_lookup is None:
                                index_lookup = {key:rows.fields.index(key) \
                                                for key in rows.fields}
                            vals = [alias_table[i] for i in specificAttributeDict[s] \
                                    if row[index_lookup[i]] in empty]
                            if len(vals) > 0:
                                fs = ",".join(vals)
                                oid = row[index_lookup["OID@"]]
                                #arcpy.AddMessage(fc_domain_dict[s])
                                ERROR = str(fc) + r" | " + str(fc_domain_dict[s]) + r" | OID: " + str(oid) + r" | " + fs
                                irow = [row[index_lookup['SHAPE@']],
                                        ERROR,
                                        fc,
                                        fc_domain_dict[s],
                                        oid,
                                        len(vals)
                                        ]

                                crvInsert.insertRow(irow)

                                errorCount += 1
                                del irow
                                del oid
                                del ERROR
                                del fs
                            del vals
                            del row

                    not_sub_sql = " and ".join([assemble_sql(field_name=f,
                                            field_type=field_names_lookup[f],
                                            not_in=True) \
                                           for f in specificAttributeDict[s] ])

                    not_sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=not_sub_sql)
                    with da.SearchCursor(inputFC,
                                         field_names_lookup.keys(),
                                         where_clause=not_sql) as rows:
                        index_lookup = None
                        for row in rows:
                            if index_lookup is None:
                                index_lookup = {key:rows.fields.index(key) \
                                                for key in rows.fields}
                            vals = [i for i in specificAttributeDict[s] \
                                    if row[index_lookup[i]] in empty]
                            fs = "N/A"
                            oid = row[index_lookup["OID@"]]
                            ERROR = str(fc) + r" | " + str(fc_domain_dict[s]) + r" | OID: " + str(oid) + r" | " + fs
                            irow = [row[index_lookup['SHAPE@']],
                                    ERROR,
                                    fc,
                                    fc_domain_dict[s],
                                    oid,
                                    0
                                    ]

                            crvInsert.insertRow(irow)

                            errorCount += 1
                            del irow
                            del oid
                            del ERROR
                            del fs
                            del vals
                            del row
                        del index_lookup
                del s
            del field_names_lookup
        if errorCount > 0:
            arcpy.AddMessage("       Errors in " + fc + ": " + str(errorCount))
        del stList

##------------------------------------------------------------------------------
        edit.stopOperation()
        edit.stopEditing(True)
        del crvInsert
        del edit
        arcpy.AddMessage("Total Processing time: %s" % str(datetime.datetime.now() - now))
        #arcpy.SetParameterAsText(4, ";".join(error_fcs.values()))
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
    now= datetime.datetime.now()
    main(*argv)
    print datetime.datetime.now() - now
