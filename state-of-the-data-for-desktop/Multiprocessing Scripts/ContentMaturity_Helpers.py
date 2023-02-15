"""-----------------------------------------------------------------------------
Name: ContentMaturity_Helpers.py
Purpose: Some functions that might be called by the content maturity tools
        (suitability indicators)
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.3+/Pro 1.2+
Author(s): Gregory Brunner, Contractor for National Geospatial-Intelligence
        Agency (NGA)
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: August 24, 2015
Modified: August, 2016
Copyright: National Geospatial-Intelligence Agency
Domestic Restrictions:
Foreign Restrictions:
License: MIT License
-----------------------------------------------------------------------------"""
from __future__ import print_function
from __future__ import division
import math
import sys, os

def get_num_batches(core_count, cell_count):
    """Return the ceiling of cell_count/core_count as a float, the smallest
    integer value greater than or equal to x."""
    return math.ceil(cell_count/core_count)

def get_install_path():
    ''' Return 64bit python install path from registry (if installed and registered),
        otherwise fall back to current 32bit process install path.
    '''
    if sys.maxsize > 2**32: return sys.exec_prefix #We're running in a 64bit process

    #We're 32 bit so see if there's a 64bit install
    path = r'SOFTWARE\Python\PythonCore\2.7'

    from _winreg import OpenKey, QueryValue
    from _winreg import HKEY_LOCAL_MACHINE, KEY_READ, KEY_WOW64_64KEY

    try:
        with OpenKey(HKEY_LOCAL_MACHINE, path, 0, KEY_READ | KEY_WOW64_64KEY) as key:
            return QueryValue(key, "InstallPath").strip(os.sep) #We have a 64bit install, so return that.
    except: return sys.exec_prefix #No 64bit, so return 32bit path

def represents_int(s):
    """determines if value is an int"""
    try:
        int(s)
        return True
    except ValueError:
        return False

def represents_float(s):
    try:
        float(s)
        return float(s)
    except ValueError:
        return -1

def make_delete_lines_list(all_fields, keep_fields):

    always_keep = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Leng', 'SHAPE', 'SHAPE_Length', 'SHAPE_Leng']

    for element in always_keep:
        keep_fields.append(element)

    drop_fields = [x.name for x in all_fields if x.name not in keep_fields]

    return drop_fields

def make_delete_polygons_list(all_fields, keep_fields):

    always_keep = ['OBJECTID', 'SHAPE', 'SHAPE_Length', 'SHAPE_Area', 'Shape_Area', 'Shape_Length', 'Shape']

    for element in always_keep:
        keep_fields.append(element)

    drop_fields = [x.name for x in all_fields if x.name not in keep_fields]

    return drop_fields
