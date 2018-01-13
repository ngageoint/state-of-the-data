# sotd_config

# portal params

portal = "https://www.arcgis.com"
un = ''
pw = ''


# services
# Services used as Input Variables
features_url =              'https://tfdmdevtdw1.esri.com:6443/arcgis/rest/services/WFS/TDSG_WFS/FeatureServer/32'
grid_url =                  'https://tfdmdevtdw1.esri.com:6443/arcgis/rest/services/WFS/TDSG_WFS/FeatureServer/58'
comparison_features_url =   'https://tfdmdevtdw1.esri.com:6443/arcgis/rest/services/WFS/TDSG_WFS/FeatureServer/32'

# Services to be Updated
source_lineage_url =        'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/SourceLineage/FeatureServer/0'
currency_url =              'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/TemporalCurrency/FeatureServer/0'
temporal_accuracy = ''
positional_acc_url =        'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/PositionalAccuracy/FeatureServer/0'
positional_offset_url = ''
thematic_url =              'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/ThematicAccuracy/FeatureServer/0'
logical_consistency_url =   'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/LogicalConsistency/FeatureServer/0'
completeness_url =          'https://services1.arcgis.com/g2TonOxuRkIqSOFx/arcgis/rest/services/Completeness/FeatureServer/0'

# Ancillary data
population =        r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\GPT_2012\gpt_2012'
grid =              r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\TFDM_PythonAPI.gdb\ThematicAccuracy'
output_fc =         r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\TFDM_PythonAPI.gdb\ThematicAccuracy_1'
template_fc =       r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\TDS\TDS_v6.1_LAO_FGCM_sub1\TDS_v6.1_LAO_FGCM_sub1.gdb\TDS\TransportationGroundCrv'
template_gdb =      r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\TDS\TDS_v6.1_LAO_FGCM_sub1\TDS_v6.1_LAO_FGCM_sub1.gdb'
attr_check_file =   r'C:\PROJECTS\STATE_OF_THE_DATA\DATA\MEEI_Logical_Consistency_Python_v2_20151119_Greg_Edits.xlsx'
attr_check_tab =    'HADR'

# Data fields
currency_field =            'zi001_sdv'.upper()
search_field =              'zi001_sps'.upper()
value_field  =              'zi001_sdp'.upper()
positional_acc_field =      'zi001_vsn'.upper()
thematic_acc_field =        'zi026_ctuu'.upper()
attr_error_field_count =    'DEFICIENCY_CNT'
attr_error_field_def =      'DEFICIENCY'

# params
look_back_days = 1000#900

# Header Logic For Filter Generation
sr = {'wkid': 4326}
sp_rel = "esriSpatialRelIntersects"
