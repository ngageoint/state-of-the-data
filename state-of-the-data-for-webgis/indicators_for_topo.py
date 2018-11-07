from sotd_indicators.Indicator import Indicator
#from sotd_indicators.utilities import update_img_service

import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Indicator Instance From Configuration File
config_file = r'config_topo.ini'

print("Running Indicators")
start = time.time()
indicator = Indicator()
indicator.load_config(config_file)
indicator.set_gis()

# GIS Object for Publishing Output
the_gis = indicator.pub_gis_conn
print(the_gis)

indicator.set_grid_sdf(use_query=True)
indicator.set_features()

pa_sdf = indicator.run_poac('ZI001_SDP'.lower(), apply_edits=False)
print('POAC: {}'.format(type(pa_sdf)))
# pa_sdf.to_featurelayer('POAC', gis=the_gis)
#
sl_sdf = indicator.run_srln('ZI001_SDP'.lower(), 'ZI001_SPS'.lower(), apply_edits=False)
print('SRLN: {}'.format(type(sl_sdf)))
#sl_sdf.to_featurelayer('SRLN', gis=the_gis)

te_sdf = indicator.run_curr('ZI001_SDV'.lower(), apply_edits=False)
print('CURR: {}'.format(type(te_sdf)))
#te_sdf.to_featurelayer('CURR', gis=the_gis)

if len(te_sdf)>0:
    ta_sdf = indicator.run_temp_acc()
    print('TEMP ACC: {}'.format(type(ta_sdf)))

th_sdf = indicator.run_them('ZI026_CTUU'.lower(), apply_edits=False)
print('THEM: {}'.format(type(th_sdf)))
#th_sdf.to_featurelayer('THEM', gis=the_gis)

# co_sdf = indicator.run_cmpl([], apply_edits=False)
# print('CMPL: {}'.format(type(co_sdf)))
# co_sdf.to_featurelayer('CMPL', gis=the_gis)
#
lo_sdf = indicator.run_logc(
    'DEFICIENCY_CNT',
    'DEFICIENCY',
    'XL_TAB',
    r'xl.json',
    apply_edits=False
)
print('LOGC: {}'.format(type(lo_sdf)))
#lo_sdf.to_featurelayer('LOGC', gis=the_gis)

elapsed = (time.time() - start)
print(elapsed)
print("Done Running Indicators")