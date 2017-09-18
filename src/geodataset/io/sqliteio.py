"""
Converts a Spatial DataFrame to a SQLite DB
"""
from __future__ import print_function
from __future__ import division
import os
import json
import warnings
from .fileops import to_featureclass
SUPPORTED_FORMATS = []
try:
    import arcpy
    HASARCPY = True
except ImportError:
    HASARCPY = False
    warnings.warn("ArcPy Missing, cannot perform sqlite operations")

#----------------------------------------------------------------------
def to_sqlite(df,
              out_folder,
              db_name,
              table_name,
              spatial_type="ST_GEOMETRY",
              overwrite=True):
    """
    Creates a new sqlite database and imports the dataframe data to that
    database.

    Inputs:

    df: spatial dataframe to export
    out_folder: Location of the SQLite database to be created
    db_name: Name of the SQLite database file. An extension of .sqlite will
     be automatically assigned if the spatial_type is ST_GEOMETRY or
     SPATIALITE. If the spatial_type is GEOPACKAGE, a .gpkg extension is
     automatically assigned.
    table_name: name of the exported spatial dataframe
    spatial_type: The spatial type to install with the new SQLite database.
     ST_GEOMETRY-Esri's spatial storage type. This is the default.
     SPATIALITE-SpatiaLite spatial storage type.
     GEOPACKAGE-OGC GeoPackage dataset.
    overwrite: True will erase data if it exists, false will throw error if
     the sqlite database exists.
    Output:
     sqlite Database path location plus the feature class.
    """
    if HASARCPY:
        sqldb = os.path.join(out_folder, db_name)
        if os.path.isdir(out_folder) == False:
            raise ValueError("Save folder does not exist")
        if os.path.isfile(sqldb) and overwrite:
            os.remove(sqldb)
        elif os.path.isfile(sqldb) and overwrite:
            raise ValueError("SQLite database exists, please pick a different name")
        db = arcpy.CreateSQLiteDatabase_management(out_database_name=sqldb,
                                                   spatial_type=spatial_type)[0]
        return to_featureclass(df=df, out_location=db, out_name=table_name)
    else:
        raise ImportError("arcpy is required to perform this operation")
    return
