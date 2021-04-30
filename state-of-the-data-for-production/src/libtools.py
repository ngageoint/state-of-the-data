import configparser
import numpy as np
from rtree import index, Rtree
import arcpy
from arcgis.features import SpatialDataFrame
from ast import literal_eval
import datetime


def tree_generator(records):
    """stream records into tree for insertion beware interleaved assumed to be false"""
    for record in records:
        # verify format xmin,xmax,ymin,ymax for indexing and for intersecting. check extent.XMin, Xmax, Ymin, Ymax
        # for inserting order is xmin, xmax, ymin, ymax if not interleaved.
        xmin, ymin, xmax, ymax = record['SHAPE'].extent
        yield record.objectid, (xmin, xmax, ymin, ymax), record


def load_objects(tree, grid):
    """ for intersection order is xmin,ymin"""
    xmin, ymin, xmax, ymax = grid.extent
    return np.array([n for n in tree.intersection((xmin, xmax, ymin, ymax), objects="raw")]).view(np.recarray)


def match(itera, iterb, key=lambda x: x):
    # @jfs - https://stackoverflow.com/questions/1479979/case-insensitive-comparison-of-sets-in-python
    def unify(iterable):
        d = {}
        for item in iterable:
            d.setdefault(key(item), []).append(item)
        return d
    A, B = unify(itera), unify(iterb)
    values = [(A[k], B[k]) for k in A if k in B]
    return [a for a, b in values for a in a]


def match_fields(feature, cmp_fields):
    """
    :usage we have a feature with n fields of unknown casing and we have a config with a list of fields
    we want the config to be indifferent to the casing and so we need to map the fields appropriately
    so we create a dictionary where key is the field (lambda) i.e. str.lower and the value is the field as it was passed
    this method is a wrapper for match that creates the fields list from a feature, and matches it to the
    passed cmp_fields list
    """
    desc = arcpy.da.Describe(feature)
    fields = [f.name for f in desc.get('fields')]
    return match(itera=fields, iterb=cmp_fields, key=str.lower)


def write_frame(df, out_location, out_name):
    SpatialDataFrame(df).to_featureclass(out_location=out_location, out_name=out_name, overwrite=True)


def AggregateVectors(resource, feature):
    """
        Expected input :resource and :feature
        where resource is the feature class being binned into and
        the feature class is being aggregated
        :usage: df = AggregateVectors(resource data frame , feature data frame).df
    """
    # todo enhance loading of tree to parse poyline sub-lines to minimize bounding box edge case
    print(f"starting aggregation")
    start = datetime.datetime.now()
    feature_records = feature.to_records()
    stree = index.Index(tree_generator(feature_records), interleaved=False)
    resource['objects'] = resource.apply(lambda x: load_objects(stree, x.SHAPE), axis=1)
    finish = datetime.datetime.now()
    runtime = finish-start
    print(f"finished at: {finish} runtime: {runtime}")


class LoadConfig(object):
    """
    given a config file read and assign arbitrary number of attributes with values for use however we might want.
    probably not a pythonic best practice since the class instantiation is not explicit
    """
    def __init__(self, config_file):
        self.config_file = config_file
        self.load_config()

    def load_config(self):
        config = configparser.ConfigParser()
        config.read_file(open(self.config_file))
        for section in config.sections():
            for k, v in dict(config.items(section)).items():
                self.__setattr__(k, literal_eval(v))

