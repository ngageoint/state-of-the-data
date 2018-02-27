from sotd_indicators.Indicator import Indicator
from osm_runner import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Get OSM SDF
bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')
print('OSM SDF: {}'.format(len(osm_sdf)))

# Indicator Instance From Configuration File
config_file = r'C:\Users\jeff8977\Desktop\SOTD\src\sotd_indicators\indicator_config.ini'
indicator = Indicator(config_file)

# OSM SDF As Only Input Not Derived From Configuration File
print("Running Indicators")
import time
start = time.time()
indicator.run_indicators(osm_sdf)
elapsed = (time.time() - start)
print(elapsed)
print("Done Running Indicators")

# Checks
print('PA SDF: {}'.format(type(indicator.pa_sdf)))
print('CO SDF: {}'.format(type(indicator.co_sdf)))
print('LO SDF: {}'.format(type(indicator.lo_sdf)))
print('TE SDF: {}'.format(type(indicator.te_sdf)))
print('TH SDF: {}'.format(type(indicator.th_sdf)))
print('SL SDF: {}'.format(type(indicator.sl_sdf)))