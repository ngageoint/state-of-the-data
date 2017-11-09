# Format: http://wiki.openstreetmap.org/wiki/Overpass_API/Language_Guide
# Section 13 / 13.1
Format = '[out:json]'

# Period: http://wiki.openstreetmap.org/wiki/Overpass_API/Overpass_QL
# Section 5 / 5.1
Output = '(._;>;);out geom qt;'

# OSM Features: http://wiki.openstreetmap.org/wiki/Map_Features
# E.G. "highway" = ["primary", "residential"]
# E.G. [out:json];way["highway"~"primary|residential"](bounding_box);(._;>;);out geom qt;
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
    "highway": [],
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

# OSM Element Types
Elements = {"point": "node", "line": "way", "polygon": "way"}
