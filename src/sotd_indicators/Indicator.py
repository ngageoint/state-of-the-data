from src.sotd_indicators.indicators import *

<<<<<<< HEAD
from arcgis.features import FeatureLayer, SpatialDataFrame
=======
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca
from arcgis.gis import GIS

import configparser


class Indicator:

    def __init__(self):

        self.gis_conn = GIS()

        # Selection Drivers
        self.grid_url = None
        self.feat_url = None

        # Positional Accuracy
        self.poac_sdf = None
        self.poac_url = None

        # Completeness
        self.cmpl_sdf = None
        self.cmpl_url = None

        # Logical Consistency
        self.logc_sdf = None
        self.logc_url = None

        # Temporal Currency
        self.curr_sdf = None
        self.curr_url = None

        # Thematic Accuracy
        self.them_sdf = None
        self.them_url = None

        # Source Lineage
        self.srln_sdf = None
        self.srln_url = None

        # Values Derived From Set Functions
        self.grid_sdf  = None
        self.grid_wkid = None
        self.features  = None
        self.selected  = None

        # String Indicators
        self.indi_lst = ['poac', 'cmpl', 'logc', 'curr', 'them', 'srln']

    def load_config(self, config_file):

        # Read Incoming Config File
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

<<<<<<< HEAD
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

        self.feat_df_list = self.get_update_features(
            self.grid_sdf,
            self.feat_url
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
=======
        for section in config.sections():

            print('Loading Section: {}'.format(section))

            for k, v in dict(config.items(section)).items():
                self.__setattr__(k, v)

    def set_grid_sdf(self, lb_days=1000):

        if not self.grid_url:
            raise Exception('Grid URL Not Set')

        else:
            dates = get_dates_in_range(lb_days)

            grid_fl = FeatureLayer(url=self.grid_url)

            self.grid_wkid = grid_fl.properties.extent.spatialReference.wkid

            self.grid_sdf = grid_fl.query(where=form_query_string(dates)).df

    def set_features(self):

        df_list = []

        for idx, row in enumerate(self.grid_sdf.iterrows()):

            geom = Geometry(row[1].SHAPE)

            sp_filter = filters.intersects(geom, self.grid_wkid)

            data_fl = FeatureLayer(gis=self.gis_conn, url=self.feat_url)

            df_list.append(
                data_fl.query(geometry_filter=sp_filter, return_all_records=False).df
            )

        self.features = df_list

    def set_selected(self, indicator):

        if indicator.lower() not in self.indi_lst:
            raise Exception('Input Indicator Does Not Match Available Options')

        else:
            if not self.__getattribute__(indicator + '_url'):
                raise Exception('Attribute Not Set For Indicator: {}'.format(indicator))

            out_sdf = None

            for idx, row in enumerate(self.grid_sdf.iterrows()):

                geom = Geometry(row[1].SHAPE)
                buff = geom.buffer(-.1)
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

                sp_filter = filters.intersects(buff, self.grid_wkid)

<<<<<<< HEAD
        dates = get_dates_in_range(lb_days)
        search_field = 'CID'
        where_clause = search_field + '=' + "'36N122WM'" + " OR " + search_field + '=' + "'36N122WJ'" + " OR "+search_field+'='+"'36N122WI'" + " OR "+search_field+'='+"'36N122WN'" # str(search_val)
        #where_clause = form_query_string(dates)
=======
                data_fl = FeatureLayer(gis=self.gis_conn, url=self.__getattribute__(indicator + '_url'))
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

                df_current = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

<<<<<<< HEAD
    #Update features method - May move this
    def update_features(self, grid_sdf, url):
        grid_fl = FeatureLayer(gis=self.gis, url=url)
        out_sdf_as_featureset = grid_sdf.to_featureset()
        grid_fl.edit_features(updates=out_sdf_as_featureset)

        return grid_fl

    #Get the features that need to be updated
    def get_update_features(self, grid_sdf, feat_url):
        df_list = []
        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = Geometry(row[1].SHAPE)
            # buff = geom.buffer(-.1)

            sp_filter = filters._filter(geom, self.sp_wkid, self.sp_rela)

            data_fl = FeatureLayer(gis=self.gis, url=feat_url)

            df_current = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

            df_list.append(df_current)

        return df_list


    #Don't think this is a statis methof
    def get_update_grids(self, grid_sdf, indicator_url):

        val_list = []
        FIELDS = []

        for idx, row in enumerate(grid_sdf.iterrows()):
            geom = Geometry(row[1].SHAPE)
            buff = geom.buffer(-.1)

            sp_filter = filters._filter(buff, 4326, self.sp_rela)

            data_fl = FeatureLayer(url=indicator_url)

            df_current = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

            d = df_current.to_dict()

            val_list.append([d[val][0] for val in d.keys()])

            FIELDS = [col for col in d.keys()]

        feat_dict = dict(zip(FIELDS, map(list, zip(*val_list))))
        out_sdf = SpatialDataFrame(feat_dict)
        out_sdf.convert_objects(convert_numeric=True)

        return out_sdf

    def run_indicators(self, osm_sdf):

        try:
            poac_sdf = self.get_update_grids(
                self.grid_sdf,
                self.poac_url
            )

            self.pa_sdf = positional_accuracy(
                poac_sdf,
                self.feat_df_list,
                self.f_value
            )

            self.update_features(self.pa_sdf, self.poac_url)

        except Exception as e:
           print('PA Exception: {}'.format(str(e)))

        try:
            cmpl_sdf = self.get_update_grids(
                self.grid_sdf,
                self.cmpl_url
            )

            self.co_sdf = completeness(
                cmpl_sdf,
                self.feat_df_list,
                osm_sdf
            )

            self.update_features(self.co_sdf, self.cmpl_url)
=======
                if idx == 0:
                    out_sdf = df_current
                else:
                    out_sdf.merge(df_current)

            self.selected = out_sdf

    def update_layer(self, df, url):

        feat_layer = FeatureLayer(gis=self.gis_conn, url=url)

        res = feat_layer.edit_features(updates=df.to_featureset())

        if 'updateResults' not in res:
            raise Exception('Edit Features Returned Issues: {}'.format(res))

        else:
            return res['updateResults']

    def run_poac(self, d1, apply_edits=True):

        try:
            self.set_selected('poac')

            df = positional_accuracy(
                self.selected,
                self.features,
                d1
            )

            if apply_edits:
                return [df, self.update_layer(df, self.poac_url)]
            else:
                return df

        except Exception as e:
            print('Exception Running Positional Accuracy: {}'.format(str(e)))

    def run_cmpl(self, comparison_sdf, apply_edits=True):

        try:
            self.set_selected('cmpl')

            df = completeness(
                self.selected,
                self.features,
                comparison_sdf
            )

            if apply_edits:
                return [df, self.update_layer(df, self.cmpl_url)]
            else:
                return df
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

        except Exception as e:
            print('Exception Running Completeness: {}'.format(str(e)))

    def run_curr(self, d1, date='1901-1-1', apply_edits=True):

        try:
<<<<<<< HEAD
            logc_sdf = self.get_update_grids(
                self.grid_sdf,
                self.logc_url
            )

            self.lo_sdf = logical_consistency(
                logc_sdf,
                self.feat_df_list,
                self.feat_url,
                self.f_att_err_cnt,
                self.f_att_err_def,
                self.template_fc,
                self.template_gdb,
                self.attr_check_tab,
                self.attr_check_file
            )

            self.update_features(self.lo_sdf, self.logc_url)
=======
            self.set_selected('curr')

            df = temporal_currency(
                self.selected,
                self.features,
                d1,
                date
            )

            if apply_edits:
                return [df, self.update_layer(df, self.curr_url)]
            else:
                return df
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

        except Exception as e:
            print('Exception Running Temporal Currency: {}'.format(str(e)))

    def run_them(self, d1, apply_edits=True):


        try:
<<<<<<< HEAD
            curr_sdf = self.get_update_grids(
                self.grid_sdf,
                self.curr_url
            )

            self.te_sdf = temporal_currency(
                curr_sdf,
                self.feat_df_list,
                self.f_currency,
                self.non_std_date
            )

            self.update_features(self.te_sdf, self.curr_url)
=======
            self.set_selected('them')

            df = thematic_accuracy(
                self.selected,
                self.features,
                d1
            )

            if apply_edits:
                return [df, self.update_layer(df, self.them_url)]
            else:
                return df
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

        except Exception as e:
            print('Exception Running Thematic Accuracy: {}'.format(str(e)))

    def run_srln(self, d1, d2, search_value=1001, apply_edits=True):

        try:
<<<<<<< HEAD
            them_sdf = self.get_update_grids(
                self.grid_sdf,
                self.them_url
            )

            self.th_sdf = thematic_accuracy(
                them_sdf,
                self.feat_df_list,
                self.f_thm_acc
            )

            self.update_features(self.th_sdf, self.them_url)
=======
            self.set_selected('srln')

            df = source_lineage(
                self.selected,
                self.features,
                d1,
                d2,
                search_value
            )

            if apply_edits:
                return [df, self.update_layer(df, self.srln_url)]
            else:
                return df
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

        except Exception as e:
            print('Exception Running Source Lineage: {}'.format(str(e)))

    def run_logc(self, d1, d2, d3, d4, d5, d6, apply_edits=True):

        try:
<<<<<<< HEAD
            srln_sdf = self.get_update_grids(
                self.grid_sdf,
                self.srln_url
            )

            self.sl_sdf = source_lineage(
                srln_sdf,
                self.feat_df_list,
                self.f_value,
                self.f_search,
                self.search_val
            )

            self.update_features(self.sl_sdf, self.srln_url)
=======
            self.set_selected('logc')

            df = logical_consistency(
                self.selected,
                self.features,
                self.feat_url,
                d1,
                d2,
                d3,
                d4,
                d5,
                d6
            )

            if apply_edits:
                return [df, self.update_layer(df, self.logc_url)]
            else:
                return df
>>>>>>> 5e86fb9de16eced6542e2312a232c5d9a861b3ca

        except Exception as e:
            print('Exception Running Source Lineage: {}'.format(str(e)))
