"""
Similarity Measurements.

"""
from __future__ import division
from __future__ import absolute_import
import math
from decimal import Decimal
__all__ = ['euclidean_distance',
           'manhattan_distance',
           'minkowski_distance',
           'cosine_similarity',
           'jaccard_similarity']
#--------------------------------------------------------------------------
def euclidean_distance(pts):
    """
    straightline distance between two points.

    return euclidean distance between two lists """
    if len(pts) <= 1:
        return 0
    return math.sqrt(sum(pow(a-b,2) for a, b in pts))
#--------------------------------------------------------------------------
def manhattan_distance(pts):
    """
    distance between two points measured along axes at right angles
    return manhattan distance between two lists
    """
    if len(pts) <= 1:
        return 0
    return sum(abs(a-b) for a,b in pts)
#--------------------------------------------------------------------------
def square_rooted(x):
    """ return 3 rounded square rooted value """
    return round(math.sqrt(sum([a*a for a in x])),3)
#--------------------------------------------------------------------------
def minkowski_distance(pts,p_value):
    """
    The Minkowski distance is a generalized metric form of Euclidean
    distance and Manhattan distance.

    return minkowski distance between two lists """
    if len(pts) <= 1:
        return 0
    return nth_root(sum(math.pow(abs(a-b),p_value) \
                        for a,b in pts), p_value)
#--------------------------------------------------------------------------
def nth_root(value, n_root):
    """ returns the n_root of an value """
    root_value = 1/float(n_root)
    return round (Decimal(value) ** Decimal(root_value),3)
#--------------------------------------------------------------------------
def cosine_similarity(pts):
    """
    Cosine similarity metric finds the normalized dot product of the two
    attributes. By determining the cosine similarity, we would effectively
    try to find the cosine of the angle between the two objects. The
    cosine of 0 is 1, and it is less than 1 for any other angle. It is thus
    a judgement of orientation and not magnitude: two vectors with the same
    orientation have a cosine similarity of 1, two vectors at 90 have a
    similarity of 0, and two vectors diametrically opposed have a
    similarity of -1, independent of their magnitude. Cosine similarity is
    particularly used in positive space, where the outcome is neatly
    bounded in [0,1]. One of the reasons for the popularity of cosine
    similarity is that it is very efficient to evaluate

    return cosine similarity between two lists
    """
    if len(pts) <= 1:
        return 0
    numerator = sum(a*b for a,b in pts)
    denominator = square_rooted(x)*square_rooted(y)
    return round(numerator/float(denominator),3)
#--------------------------------------------------------------------------
def jaccard_similarity(x, y):
    """
    The Jaccard similarity measures the similarity between finite sample
    sets and is defined as the cardinality of the intersection of sets
    divided by the cardinality of the union of the sample sets.
    returns the jaccard similarity between two lists
    """
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    return intersection_cardinality/float(union_cardinality)