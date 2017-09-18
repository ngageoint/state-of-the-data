
from rtree import RTreeError
from rtree import RTree
class SpatialIndex(RTree):
    """
    A simple wrapper around rtree's RTree Index
    """

    def __init__(self, *args):
        RTree.__init__(self, *args)

    @property
    def size(self):
        return len(self.count)

    @property
    def is_empty(self):
        if len(self.count) > 1:
            return False
        return self.size < 1
