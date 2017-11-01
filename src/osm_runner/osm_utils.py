# Format: http://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide
# Section 13 / 13.1
Format = '[out:json]'

# Period: http://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL
# Section 5 / 5.1
Period = '(._;>;);out body;'

# OSM Features: http://wiki.openstreetmap.org/wiki/Map_Features
Filters = {
    "aerialway": [],
    "aeroway": [],
    "amenity": [],
    "barrier": [],
    "boundary": [],
    "building": [],
    "craft": [],
    "emergency": [],
    "geological": [],
    "highway": ["primary", "residential"],
    "historic": [],
    "landuse": [],
    "leisure": [],
    "man_made": [],
    "military": [],
    "natural": [],
    "office": [],
    "place": [],
    "power": [],
    "public transport": [],
    "railway": [],
    "route": [],
    "shop": [],
    "sport": [],
    "tourism": [],
    "waterway": [],
}

# OSM Feature Types
OSM_Features = {
    "point":   "node",
    "line":    "way",
    "polygon": "way"
}
