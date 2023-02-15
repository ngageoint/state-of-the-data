"""
Contains common spatial operations that use
arcpy.
"""
import arcpy
from arcpy import env
from arcpy import da
import numpy as np
#--------------------------------------------------------------------------
def get_oids(features, return_type="list"):
    """returns a list of OBJECTIDs for either a list or numpy array"""
    array = da.FeatureClassToNumPyArray(features, ('OID@'))
    if return_type == "list":
        return array['OID@'].tolist()
    return array['OID@']
#--------------------------------------------------------------------------
def build_oid_chunks(oids, num_chunks):
    """
    Takes a list of np.ndarray and converts it into smaller
    arrays of equal lenths.

    oids: list or numpy.ndarray
    num_chunks: positive integer, must be >= 1
    """
    if num_chunks < 1:
        raise ValueError("num_chunks must be >= 1")
    if isinstance(oids, (list, np.ndarray)) == False:
        raise ValueError("oids must be a list of numpy.ndarray type object")
    if isinstance(oids, np.ndarray):
        return np.array_split(oids, num_chunks)
    chunk_size = (len(oids) + num_chunks - 1) / num_chunks
    chunked_list = [oids[i:i + chunk_size] for i in xrange(0, len(oids), chunk_size)]
    return chunked_list