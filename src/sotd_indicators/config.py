# sotd_config

# portal params

portal = "https://www.arcgis.com"
un = ''
pw = ''


# services
# Services used as Input Variables
features_url =              ''
grid_url =                  ''
comparison_features_url =   ''

# Services to be Updated
source_lineage_url =        ''
currency_url =              ''
temporal_accuracy = ''
positional_acc_url =        ''
positional_offset_url = ''
thematic_url =              ''
logical_consistency_url =   ''
completeness_url =          ''

# Ancillary data
population =        ''
grid =              ''
output_fc =         ''
template_fc =       ''
template_gdb =      ''
attr_check_file =   ''
attr_check_tab =    ''

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
