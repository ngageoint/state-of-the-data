from sotd_indicators.Indicator import Indicator
from osm_runner import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas

# Get OSM SDF
bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')
print('OSM SDF: {}'.format(len(osm_sdf)))

# Indicator Instance From Configuration File
config_file = r'C:\Users\jeff8977\Desktop\SOTD\devconfig_topo.ini'

# OSM SDF As Only Input Not Derived From Configuration File
print("Running Indicators")
import time
start = time.time()
indicator = Indicator()
indicator.load_config(config_file)
indicator.set_gis()
indicator.set_grid_sdf()
indicator.set_features()

# GIS Object for Output
from arcgis.gis import GIS
the_gis = GIS()

pa_sdf = indicator.run_poac('ZI001_SDP', apply_edits=False)
print('POAC: {}'.format(type(pa_sdf)))
pa_sdf.to_featurelayer('POAC', gis=the_gis)

sl_sdf = indicator.run_srln('ZI001_SDP', 'ZI001_SPS', apply_edits=False)
print('SRLN: {}'.format(type(sl_sdf)))
sl_sdf.to_featurelayer('SRLN', gis=the_gis)

te_sdf = indicator.run_curr('ZI001_SDV', apply_edits=False)
print('CURR: {}'.format(type(te_sdf)))
te_sdf.to_featurelayer('CURR', gis=the_gis)

th_sdf = indicator.run_them('ZI026_CTUU', apply_edits=False)
print('THEM: {}'.format(type(th_sdf)))
th_sdf.to_featurelayer('THEM', gis=the_gis)

co_sdf = indicator.run_cmpl([], apply_edits=False)
print('CMPL: {}'.format(type(co_sdf)))
co_sdf.to_featurelayer('CMPL', gis=the_gis)

lo_sdf = indicator.run_logc(
    'DEFICIENCY_CNT',
    'DEFICIENCY',
    'HADR',
    r'C:\Users\jeff8977\Documents\My Received Files\attributes.json',
    apply_edits=False
)
print('LOGC: {}'.format(type(lo_sdf)))
lo_sdf.to_featurelayer('LOGC', gis=the_gis)

elapsed = (time.time() - start)
print(elapsed)
print("Done Running Indicators")