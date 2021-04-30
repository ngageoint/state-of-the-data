import os
import ast
import sys
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
from rtree import Rtree
from itertools import repeat
import logging

import arcpy
from arcpy import env
from arcpy import da

from common import FunctionError, trace
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
#--------------------------------------------------------------------------
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
                                       ('DEFICIENCY', '|S254'),
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
            return 50000
        else:
            return 500000
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

def force_lower(attr_dict):
    lower_dict = {}
    for k, v in attr_dict.items():
        val_list = []
        for val in v:
            val_list.append(val.lower())
        lower_dict[k] = val_list
    return lower_dict

#def main(*argv):
def main():
    """ main driver of program """
    try:
        now = datetime.datetime.now()
        print(now)

        # Common Inputs
        workspace = sys.argv[1]#r"sde_connection_files\viewer.sde"#
        fc =  sys.argv[2] #r'TransportationGroundCrv'
        idx_location = sys.argv[3] #r'\RESULTS\RTREE'
        try:
            resource_srf = sys.argv[4] #r'cells_to_update'
        except:
            resource_srf = None

        filename = r'..\data\SotD_Attribute_Check_20200227.xlsx'

        if not os.path.exists(idx_location):
            os.makedirs(idx_location)

        log = f"gen_lc_rtree_{fc}.log"
        print(log)
        print(f"start: {now}")
        logging.basicConfig(filename=os.path.join(idx_location, log), level=logging.INFO)
        logging.info(f"start: {now}")


        # Hard Coded Inputs
        lc_fields = ['field_list', 'deficiency_cnt']
        tabs = ['PSG', 'HADR']#, 'MTM', 'Routing', 'LOC']
        logging.info(f"tabs used in the analysis: {str(tabs)}")

        error_fcs = {}
        empty = (-999999, '', None, 'noInformation','None', 'Null', 'NULL', -999999.0)

        env.workspace = workspace
        arcpy.AddMessage('GeoDatabase: ' + env.workspace)
        inputFD = arcpy.ListDatasets('*TDS')[0]
        env.workspace = inputFD
        arcpy.AddMessage('Feature Dataset: ' + env.workspace)
        logging.info('Feature Dataset: ' + env.workspace)

        arcpy.AddMessage("Beginning null attribute check, per appendix 4 of the FGCM SOW.")
        logging.info("Beginning null attribute check, per appendix 4 of the FGCM SOW.")

        fcs = arcpy.ListFeatureClasses()
        fc_fullname = [f for f in fcs if fc in f][0]

        arcpy.AddMessage("Looking at: %s" % fc)
        logging.info("Looking at: %s" % fc)
        stList = unique_values(os.path.join(env.workspace,fc_fullname),"F_CODE")

        if resource_srf == None:
            for tabname in tabs:
                specificAttributeDict, attrCheck = create_attr_dict(filename, tabname)
                #print(specificAttributeDict)
                if 'sde' in env.workspace:
                    specificAttributeDict = force_lower(specificAttributeDict)
                #print(specificAttributeDict)

                idx_name = fc + "_" + tabname.lower() + "_Index"
                idx_filename = os.path.join(idx_location, idx_name)
                idx = Rtree(idx_filename, interleaved=False)
                errorCount = 0
                if len(stList) > 0 :
                    field_names_lookup = {field.name : field.type \
                                          for field in arcpy.ListFields(os.path.join(env.workspace, fc_fullname)) \
                                          if field.type not in ['Blob', 'Geometry', 'OID', 'Raster']}
                    field_names_lookup['SHAPE@'] = 'Geometry'
                    field_names_lookup['OID@'] = 'OID'
                    for s in stList:
                        #print(s)
                        if s in specificAttributeDict:
                            sub_sql = " or ".join([assemble_sql(field_name=f,
                                                    field_type=field_names_lookup[f]) \
                                                   for f in specificAttributeDict[s] ])
                            arcpy.AddMessage(sub_sql)
                            logging.info(f"Query: {str(sub_sql)}")
                            try:
                                sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=sub_sql)

                                fn = list(field_names_lookup.keys())

                                feature_class =  os.path.join(env.workspace, fc_fullname)
                                with da.SearchCursor(feature_class,
                                                     field_names = fn,
                                                     where_clause=sql) as rows:
                                    index_lookup = None
                                    for row in rows:
                                        if index_lookup is None:
                                            index_lookup = {key:rows.fields.index(key) \
                                                            for key in rows.fields}
                                        vals = [i for i in specificAttributeDict[s] \
                                                if row[index_lookup[i]] in empty]
                                        if len(vals) > 0:
                                            fs = ",".join(vals)
                                            oid = row[index_lookup["OID@"]]
                                            extent = row[index_lookup["SHAPE@"]].extent
                                            geoms = (extent.XMin, extent.XMax, extent.YMin, extent.YMax)

                                            fields = list(map(str.lower,lc_fields))
                                            lc_results = [fs, len(vals)]
                                            item = dict(zip(fields, lc_results))
                                            idx.insert(oid, tuple(map(round, geoms, repeat(12))), item)

                                            errorCount += 1
                                            del oid
                                            del fs
                                        del vals
                                        del row

                                not_sub_sql = " and ".join([assemble_sql(field_name=f,
                                                        field_type=field_names_lookup[f],
                                                        not_in=True) \
                                                       for f in specificAttributeDict[s] ])

                                not_sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=not_sub_sql)
                                with da.SearchCursor(feature_class,
                                                     list(field_names_lookup.keys()),
                                                     where_clause=not_sql) as rows:
                                    index_lookup = None
                                    for row in rows:
                                        if index_lookup is None:
                                            index_lookup = {key:rows.fields.index(key) \
                                                            for key in rows.fields}

                                        oid = row[index_lookup["OID@"]]
                                        extent = row[index_lookup["SHAPE@"]].extent
                                        geoms = (extent.XMin, extent.XMax, extent.YMin, extent.YMax)

                                        fields = list(map(str.lower, lc_fields))
                                        lc_results = ["N/A", 0]
                                        item = dict(zip(fields, lc_results))
                                        idx.insert(oid, tuple(map(round, geoms, repeat(12))), item)
                                        del oid
                                        del row
                                    del index_lookup
                            except:
                                logging.info(f"Failed")
                        del s
                    del field_names_lookup
                if errorCount > 0:
                    arcpy.AddMessage("       Errors in " + fc + ": " + str(errorCount))
                    logging.info("       Errors in " + fc + ": " + str(errorCount))
                arcpy.AddMessage("Total Processing time: %s" % str(datetime.datetime.now() - now))
                logging.info("Total Processing time: %s" % str(datetime.datetime.now() - now))
            del stList
        else:
            for tabname in tabs:
                specificAttributeDict, attrCheck = create_attr_dict(filename, tabname)
                # print(specificAttributeDict)
                if 'sde' in env.workspace:
                    specificAttributeDict = force_lower(specificAttributeDict)
                # print(specificAttributeDict)

                idx_name = fc + "_" + tabname.lower() + "_Index"
                idx_filename = os.path.join(idx_location, idx_name)
                idx = Rtree(idx_filename, interleaved=False)

                print("Selecting features.")
                logging.info("Selecting Features.")
                selected_resource_srfs = arcpy.MakeFeatureLayer_management(resource_srf, 'selecting_fl')
                analysis_feature_layer = arcpy.MakeFeatureLayer_management(fc_fullname, 'analysis_fl')

                selected_features = arcpy.SelectLayerByLocation_management(analysis_feature_layer, 'INTERSECT',
                                                                           selected_resource_srfs)

                errorCount = 0
                if len(stList) > 0:
                    field_names_lookup = {field.name: field.type \
                                          for field in arcpy.ListFields(os.path.join(env.workspace, fc_fullname)) \
                                          if field.type not in ['Blob', 'Geometry', 'OID', 'Raster']}
                    field_names_lookup['SHAPE@'] = 'Geometry'
                    field_names_lookup['OID@'] = 'OID'
                    for s in stList:
                        # print(s)
                        if s in specificAttributeDict:
                            sub_sql = " or ".join([assemble_sql(field_name=f,
                                                                field_type=field_names_lookup[f]) \
                                                   for f in specificAttributeDict[s]])
                            arcpy.AddMessage(sub_sql)
                            logging.info(f"Query: {str(sub_sql)}")
                            try:
                                sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=sub_sql)

                                fn = list(field_names_lookup.keys())

                                feature_class = os.path.join(env.workspace, fc_fullname)
                                with da.SearchCursor(selected_features,
                                                     field_names=fn,
                                                     where_clause=sql) as rows:
                                    index_lookup = None
                                    for row in rows:
                                        if index_lookup is None:
                                            index_lookup = {key: rows.fields.index(key) \
                                                            for key in rows.fields}
                                        vals = [i for i in specificAttributeDict[s] \
                                                if row[index_lookup[i]] in empty]
                                        if len(vals) > 0:
                                            fs = ",".join(vals)
                                            oid = row[index_lookup["OID@"]]
                                            extent = row[index_lookup["SHAPE@"]].extent
                                            geoms = (extent.XMin, extent.XMax, extent.YMin, extent.YMax)

                                            fields = list(map(str.lower, lc_fields))
                                            lc_results = [fs, len(vals)]
                                            item = dict(zip(fields, lc_results))
                                            idx.insert(oid, tuple(map(round, geoms, repeat(12))), item)

                                            errorCount += 1
                                            del oid
                                            del fs
                                        del vals
                                        del row

                                not_sub_sql = " and ".join([assemble_sql(field_name=f,
                                                                         field_type=field_names_lookup[f],
                                                                         not_in=True) \
                                                            for f in specificAttributeDict[s]])

                                not_sql = "F_CODE = '{fcode}' and ({subsql})".format(fcode=s, subsql=not_sub_sql)
                                with da.SearchCursor(selected_features,
                                                     list(field_names_lookup.keys()),
                                                     where_clause=not_sql) as rows:
                                    index_lookup = None
                                    for row in rows:
                                        if index_lookup is None:
                                            index_lookup = {key: rows.fields.index(key) \
                                                            for key in rows.fields}

                                        oid = row[index_lookup["OID@"]]
                                        extent = row[index_lookup["SHAPE@"]].extent
                                        geoms = (extent.XMin, extent.XMax, extent.YMin, extent.YMax)

                                        fields = list(map(str.lower, lc_fields))
                                        lc_results = ["N/A", 0]
                                        item = dict(zip(fields, lc_results))
                                        idx.insert(oid, tuple(map(round, geoms, repeat(12))), item)
                                        del oid
                                        del row
                                    del index_lookup
                            except:
                                logging.info(f"Failed")
                        del s
                    del field_names_lookup
                if errorCount > 0:
                    arcpy.AddMessage("Errors in " + fc + ": " + str(errorCount))
                    logging.info("Errors in " + fc + ": " + str(errorCount))
                arcpy.AddMessage("Total Processing time: %s" % str(datetime.datetime.now() - now))
                logging.info("Total Processing time: %s" % str(datetime.datetime.now() - now))
            del stList
        arcpy.AddMessage("Done. Total Processing Time: %s" % str(datetime.datetime.now() - now))
        logging.info("Done. Total Processing Time: %s" % str(datetime.datetime.now() - now))
        logging.shutdown()
        #arcpy.SetParameterAsText(4, ";".join(error_fcs.values()))
    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        logging.info("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        logging.info("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        logging.info("with error message: %s" % synerror)
        arcpy.AddError("ArcPy Error Message: %s" % arcpy.GetMessages(2))
        logging.info("ArcPy Error Message: %s" % arcpy.GetMessages(2))
        logging.shutdown()
    except FunctionError as f_e:
        messages = f_e.args[0]
        arcpy.AddError("error in function: %s" % messages["function"])
        logging.info("error in function: %s" % messages["function"])
        arcpy.AddError("error on line: %s" % messages["line"])
        logging.info("error on line: %s" % messages["line"])
        arcpy.AddError("error in file name: %s" % messages["filename"])
        logging.info("error in file name: %s" % messages["filename"])
        arcpy.AddError("with error message: %s" % messages["synerror"])
        logging.info("with error message: %s" % messages["synerror"])
        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
        logging.info("ArcPy Error Message: %s" % messages["arc"])
        logging.shutdown()
    except:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        logging.info("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        logging.info("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        logging.info("with error message: %s" % synerror)
        logging.shutdown()
#--------------------------------------------------------------------------
if __name__ == "__main__":
    env.overwriteOutput = True
    #argv = tuple(arcpy.GetParameterAsText(i)
    #for i in range(arcpy.GetArgumentCount()))
    now= datetime.datetime.now()
    #main(*argv)
    main()
    print(datetime.datetime.now() - now)
