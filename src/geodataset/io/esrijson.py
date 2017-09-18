"""
Converts Esri JSON to and from a Spatial DataFrame.
"""
from __future__ import print_function

#----------------------------------------------------------------------
def to_json(df):
    """converts a dataframe to GeoJSON"""
    return df.__geo_interface__()

