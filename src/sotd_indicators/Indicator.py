from src.sotd_indicators.indicators import *

from arcgis.features import FeatureLayer
from arcgis.gis import GIS

import configparser


class Indicator:

    def __init__(self, config_file):

        # Read Input Configuration
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        # AGOL/Portal Credentials
        self.agol_url = config.get('AGOL', 'agol_url')
        self.username = config.get('AGOL', 'username')
        self.password = config.get('AGOL', 'password')

        # Consumed Services
        self.feat_url = config.get('Services', 'feat_url')
        self.grid_url = config.get('Services', 'grid_url')
        self.cmpr_url = config.get('Services', 'cmpr_url')
        self.srln_url = config.get('Services', 'srln_url')
        self.curr_url = config.get('Services', 'curr_url')
        self.temp_url = config.get('Services', 'temp_url')
        self.poac_url = config.get('Services', 'poac_url')
        self.poof_url = config.get('Services', 'poof_url')
        self.them_url = config.get('Services', 'them_url')
        self.logc_url = config.get('Services', 'logc_url')
        self.cmpl_url = config.get('Services', 'cmpl_url')

        # Data Fields
        self.f_value       = config.get('Fields', 'f_value')
        self.f_search      = config.get('Fields', 'f_search')
        self.f_pos_acc     = config.get('Fields', 'f_pos_acc')
        self.f_thm_acc     = config.get('Fields', 'f_thm_acc')
        self.f_currency    = config.get('Fields', 'f_currency')
        self.f_att_err_cnt = config.get('Fields', 'f_att_err_cnt')
        self.f_att_err_def = config.get('Fields', 'f_att_err_def')

        # Other Inputs
        self.sp_rela         = config.get('Other', 'sp_rela')
        self.population      = config.get('Other', 'population')
        self.grid            = config.get('Other', 'grid')
        self.output_fc       = config.get('Other', 'output_fc')
        self.template_fc     = config.get('Other', 'template_fc')
        self.template_gdb    = config.get('Other', 'template_gdb')
        self.attr_check_file = config.get('Other', 'attr_check_file')
        self.attr_check_tab  = config.get('Other', 'attr_check_tab')
        self.non_std_date    = config.get('Other', 'non_std_date')

        # Computed Config Values (Conversion Should Be Moved To Function)
        self.lb_days    = int(config.get('Other', 'lb_days'))
        self.sp_wkid    = {'wkid': int(config.get('Other', 'srid_id'))}
        self.search_val = int(config.get('Other', 'search_val'))

        # Indicator SpatialDataFrames
        self.pa_sdf = None
        self.co_sdf = None
        self.lo_sdf = None
        self.te_sdf = None
        self.th_sdf = None
        self.sl_sdf = None

        # Set Initial GIS Object
        self.gis = self.set_gis(
            self.agol_url,
            self.username,
            self.password
        )

        # Set Initial Grid SpatialDataFrame
        self.grid_sdf = self.set_grid(
            self.grid_url,
            self.lb_days
        )

    @staticmethod
    def set_gis(url, username, password):

        # Handle Empty Inputs
        for item in [url, username, password]:
            if len(item) == 0:
                return GIS()

        # Handle Invalid Inputs (Wrong Username)
        try:
            return GIS(url, username, password)
        except RuntimeError:
            return GIS()

    @staticmethod
    def set_grid(grid_url, lb_days):

        dates = get_dates_in_range(lb_days)
        where_clause = form_query_string(dates)

        grid_fl = FeatureLayer(url=grid_url)
        return grid_fl.query(where=where_clause).df

    def run_indicators(self, osm_sdf):

        try:
            self.pa_sdf = positional_accuracy(
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.poac_url,
                self.f_value
            )
        except Exception as e:
            print('PA Exception: {}'.format(str(e)))

        try:
            self.co_sdf = completeness(
                osm_sdf,
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.cmpl_url
            )
        except Exception as e:
            print('CO Exception: {}'.format(str(e)))

        try:
            self.lo_sdf = logical_consistency(
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.logc_url,
                self.f_att_err_cnt,
                self.f_att_err_def,
                self.template_fc,
                self.template_gdb,
                self.attr_check_tab,
                self.attr_check_file
            )
        except Exception as e:
            print('LO Exception: {}'.format(str(e)))

        try:
            self.te_sdf = temporal_currency(
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.curr_url,
                self.f_currency,
                self.non_std_date
            )
        except Exception as e:
            print('TE Exception: {}'.format(str(e)))

        try:
            self.th_sdf = thematic_accuracy(
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.them_url,
                self.f_thm_acc
            )
        except Exception as e:
            print('TH Exception: {}'.format(str(e)))

        try:
            self.sl_sdf = source_lineage(
                self.grid_sdf,
                self.gis,
                self.sp_wkid,
                self.sp_rela,
                self.feat_url,
                self.srln_url,
                self.f_value,
                self.f_search,
                self.search_val
            )
        except Exception as e:
            print('SL Exception: {}'.format(str(e)))
