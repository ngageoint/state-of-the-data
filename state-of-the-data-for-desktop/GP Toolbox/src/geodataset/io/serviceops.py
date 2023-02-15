"""
   Converts a Layer to a Spatial DataFrame
"""
from __future__ import print_function
from __future__ import division
from .. import SpatialDataFrame
try:
    import arcgis
    from arcgis.features.layer import FeatureLayer, Table
    from arcgis.data.geodataset.utils import chunks
    HASARCGIS = True
except:
    HASARCGIS = False
import pandas as pd
import json
import warnings
#--------------------------------------------------------------------------
if HASARCGIS:
    def from_layer(layer):
        """
        Converts a Feature Service Layer to a Pandas' DataFrame

        Parameters:
         :layer: FeatureLayer or Table object.  If the object is a FeatureLayer
          the function will return a Spatial DataFrame, if the object is of
          type Table, the function will return a Pandas' DataFrame

        Usage:
        >>> from arcgis.arcgisserver import Layer
        >>> from arcgis import from_layer
        >>> mylayer = Layer("https://sampleserver6.arcgisonline.com/arcgis/rest" +\
                            "/services/CommercialDamageAssessment/FeatureServer/0")
        >>> sdf = from_layer(mylayer)
        >>> print(sdf)
        """
        if isinstance(layer, (Table, FeatureLayer)) == False:
            raise ValueError("Invalid inputs: must be FeatureLayer or Table")
        max_records = layer.properties['maxRecordCount']
        service_count = layer.query(return_count_only=True)
        if service_count > max_records:
            frames = []
            oid_info = layer.query(return_ids_only=True)
            for ids in chunks(oid_info['objectIds'], max_records):
                ids = [str(i) for i in ids]
                sql = "%s in (%s)" % (oid_info['objectIdFieldName'],
                                      ",".join(ids))
                frames.append(layer.query(where=sql).df)
            res = pd.concat(frames, ignore_index=True)
        else:
            res = layer.query().df
        return res
