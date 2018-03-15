from sotd_indicators.Indicator import Indicator
from osm_runner import *
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Indicator Instance From Configuration File
config_file = r'C:\PROJECTS\state-of-the-data\state-of-the-data-for-webgis\devconfig_maritime.ini'
#indicator = Indicator(config_file)

# OSM SDF As Only Input Not Derived From Configuration File
print("Running Indicators")
import time
start = time.time()
indicator = Indicator()
indicator.load_config(config_file)
indicator.set_gis()
indicator.set_grid_sdf()
indicator.set_features()

pa_sdf = indicator.run_poac('acc', apply_edits=False)
print(pa_sdf)
sl_sdf = indicator.run_srln('nis_editor', apply_edits=False)
print(sl_sdf)
te_sdf = indicator.run_curr('nis_last_mod', apply_edits=False)
print(te_sdf[0])

#GET OSM SDF for Waterways
for idx, row in enumerate(te_sdf[0].iterrows()):
    bbox =row[1].SHAPE.extent  #'(37.708132, -122.513617, 37.832132, -122.349607)'
    ex = "("+str(bbox.YMin)+ ", " + str(bbox.XMin)+ ", " + str(bbox.YMax) + ", " + str(bbox.XMax) +")"
    print(idx)
    print(ex)
    try:
        temp_sdf = gen_osm_sdf('line', ex, osm_tag='waterway')
        if idx == 0:
            osm_sdf = temp_sdf
        else:
            osm_sdf = osm_sdf.merge_datasets(temp_sdf)
    except:
        osm_sdf=osm_sdf

print("OSM")
print(osm_sdf)
co_sdf = indicator.run_cmpl(osm_sdf,apply_edits=False)
print(co_sdf)
#th_sdf = indicator.run_them('ZI026_CTUU', apply_edits=False)
#print(th_sdf)
#lo_sdf = indicator.run_logc('DEFICIENCY_CNT',
#            'DEFICIENCY',
#            'HADR',
#            r'C:\PROJECTS\state-of-the-data\state-of-the-data-for-webgis\sotd_indicators\attributes.json',
#            apply_edits=False
#        )
#print(lo_sdf)

elapsed = (time.time() - start)
print(elapsed)
print("Done Running Indicators")
