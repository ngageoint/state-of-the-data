from src.sotd_indicators import indicators
from src.osm_runner import *

# Completeness - In Progress
# Test Parameters For OSM SDF
bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')
cp_sdf = indicators.completeness(osm_sdf)

# Logical Consistency - In Progress
# Configured Inputs Need To Be Addressed
# 1 - AliasTable - Template Feature Class
# 2 - FC Domain Dict - Template FGDB
lc_sdf = indicators.logical_consistency()

# Positional Accuracy - Functional
pa_sdf = indicators.positional_accuracy()

# Temporal Currency - Functional
tc_sdf = indicators.temporal_currency()

# Thematic Accuracy - Functional
ta_sdf = indicators.thematic_accuracy()

# Source Lineage - Functional
sl_sdf = indicators.source_lineage()
