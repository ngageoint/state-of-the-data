from sotd_indicators.Indicator import Indicator
from osm_runner import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Get OSM SDF
bbox = '(37.708132, -122.513617, 37.832132, -122.349607)'
osm_sdf = gen_osm_sdf('line', bbox, osm_tag='highway')
print('OSM SDF: {}'.format(len(osm_sdf)))

# Indicator Instance From Configuration File
config_file = r'C:\PROJECTS\state-of-the-data\state-of-the-data-for-webgis\config_topo.ini'
#indicator = Indicator(config_file)

# OSM SDF As Only Input Not Derived From Configuration File
print("Running Indicators")
import time
start = time.time()
indicator = Indicator()
indicator.load_config(config_file)
indicator.set_grid_sdf()
indicator.set_features()




pa_sdf = indicator.run_poac('ZI001_SDP', apply_edits=False)
print(pa_sdf)
sl_sdf = indicator.run_srln('ZI001_SDP', 'ZI001_SPS', apply_edits=False)
print(sl_sdf)
te_sdf = indicator.run_curr('ZI001_SDV', apply_edits=False)
print(te_sdf)
th_sdf = indicator.run_them('ZI026_CTUU', apply_edits=False)
print(th_sdf)
co_sdf = indicator.run_cmpl(osm_sdf,apply_edits=False)
print(co_sdf)
lo_sdf = indicator.run_logc('DEFICIENCY_CNT',
            'DEFICIENCY',
            'HADR',
            r'C:\PROJECTS\state-of-the-data\state-of-the-data-for-webgis\sotd_indicators\sample.json',
            apply_edits=False
        )
print(lo_sdf)

elapsed = (time.time() - start)
print(elapsed)
print("Done Running Indicators")