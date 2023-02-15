"""
@author: A. Chapkowski
@contact: achapkowski@esri.com
@company: Esri
@version: 1.0.0
@description: Contains a collection of common classes and functions
 aimed to reduce code redundancy and provice a common set of error
 handling functions.
@requirements: Python 2.7.x, ArcGIS 10.3.1+
"""
import os
import sys
import arcpy
import numpy as np
##import pandas as pd
import platform
import itertools
import traceback
from arcpy import da

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
    import platform

    if platform.architecture()[0].lower() == "32bit":
        return 50000
    else:
        return 500000
#--------------------------------------------------------------------------
def unique_values(table, field="OID@"):
    try:
        uvalues = None
        chunk_size = calc_chunk_size()
        arcpy.AddMessage(["Start Unique Values - 1", table, field, arcpy.Exists(table)])
        with da.SearchCursor(table, [field]) as cursor:
            arcpy.AddMessage("Start Unique Values - 2")
            for group in grouper_it(chunk_size, cursor):
                df = pd.DataFrame.from_records(group, columns=cursor.fields)
                column = df[field].unique()#.tolist()
                if uvalues is None:
                    uvalues = column
                else:
                    uvalues = np.concatenate([column, uvalues])
                del group
                del df
                del column
            del cursor
        return list(set(uvalues.tolist()))
    except:
        arcpy.AddError([trace(), arcpy.GetMessages(2)])
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
def maximum(lst):
    """
    """
    try:
        #if isinstance(lst, list):
        #    m = max(lst)
        #    return max(m), lst.count(max(lst))
        if isinstance(lst, np.ndarray):
            lst.max()
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "common.maximum",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
        )
#--------------------------------------------------------------------------
def minimum(lst):
    """
    """
    try:
        #if isinstance(lst, list):
        #    m = max(lst)
        #    return max(m), lst.count(max(lst))
        if isinstance(lst, np.ndarray):
            lst.max()
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "common.minimum",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
        )
#--------------------------------------------------------------------------
def most_common(lst):
    """
    """
    try:
        #if isinstance(lst, list):
        #    m = max(lst)
        #    return max(m), lst.count(max(lst))
        if isinstance(lst, np.ndarray):
            lst.max()
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "common.most_common",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )