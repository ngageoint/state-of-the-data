from src.sotd_indicators import indicators
from src.osm_runner import *
from src.sotd_indicators.config import *
from arcgis.gis import *

portal_gis = GIS(portal, un, pw)

# Completeness - In Progress
# Test Parameters For OSM SDF
bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')
print(osm_sdf.head())
cp_sdf = indicators.completeness(osm_sdf, portal_gis)
print(cp_sdf)

# Logical Consistency - In Progress
# Configured Inputs Need To Be Addressed
# 1 - AliasTable - Template Feature Class
# 2 - FC Domain Dict - Template FGDB
lc_sdf = indicators.logical_consistency(portal_gis)
print(lc_sdf)

# Positional Accuracy - Functional
pa_sdf = indicators.positional_accuracy(portal_gis)
print(pa_sdf)

# Temporal Currency - Functional
tc_sdf = indicators.temporal_currency(portal_gis)
print(tc_sdf)

# Thematic Accuracy - Functional
ta_sdf = indicators.thematic_accuracy(portal_gis)

# Source Lineage - Functional
sl_sdf = indicators.source_lineage(portal_gis)
print(sl_sdf)
