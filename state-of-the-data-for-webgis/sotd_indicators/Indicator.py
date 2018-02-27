from src.sotd_indicators.indicators import *

from arcgis.gis import GIS

import configparser
import time


class Indicator:

    def __init__(self):

        # Publicly Available Resources
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

    def load_config(self, config_file):

        # Read Incoming Config File
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        for section in config.sections():

            print('Loading Section: {}'.format(section))

            for k, v in dict(config.items(section)).items():
                self.__setattr__(k, v)

    def set_grid_sdf(self, lb_days=1000):

        if not self.grid_url:
            raise Exception('Grid URL Not Set')

        else:
            dates = get_dates_in_range(lb_days)

            grid_fl = FeatureLayer(url=self.grid_url, gis=self.gis_conn)

            self.grid_wkid = grid_fl.properties.extent.spatialReference.wkid

            self.grid_sdf = grid_fl.query(where=form_query_string(dates)).df

    def set_features(self):

        df_list = []

        for idx, row in enumerate(self.grid_sdf.iterrows()):

            geom = Geometry(row[1].SHAPE)

            sp_filter = filters.intersects(geom, self.grid_wkid)

            data_fl = FeatureLayer(url=self.feat_url, gis=self.gis_conn)

            df_list.append(
                data_fl.query(geometry_filter=sp_filter, return_all_records=False).df
            )

        self.features = df_list

    def set_selected(self, indicator):

        created = False
        out_sdf = None

        for idx, row in enumerate(self.grid_sdf.iterrows()):

            if not self.__getattribute__(indicator + '_url'):

                df_current = SpatialDataFrame(
                    columns=field_schema.get(indicator),
                    geometry=[Geometry(json.loads(row[1].SHAPE.JSON))]
                )

                created = True

            else:
                # Negative Buffer to Select a Single Grid Cell
                sp_filter = filters.intersects(
                    Geometry(row[1].SHAPE).buffer(-.1),
                    self.grid_wkid
                )

                data_fl = FeatureLayer(
                    url=self.__getattribute__(indicator + '_url'),
                    gis=self.gis_conn
                )

                df_current = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

            if idx == 0:
                out_sdf = df_current

            else:
                out_sdf.merge_datasets(df_current)

        self.selected = out_sdf
        return created

    def create_layer(self, df, title):

        print('Creating New Hosted Feature Layer: {}'.format(title))

        new_layer = df.to_featurelayer(
            title,
            gis=self.gis_conn
        )

        return new_layer.id

    def update_layer(self, df, url):

        feat_layer = FeatureLayer(url=url, gis=self.gis_conn)

        res = feat_layer.edit_features(updates=df.to_featureset())

        if 'updateResults' not in res:
            raise Exception('Edit Features Returned Issues: {}'.format(res))

        else:
            return res['updateResults']

    def run_poac(self, p1, apply_edits=True):

        try:
            new_flag = self.set_selected('poac')

            df = positional_accuracy(
                self.selected,
                self.features,
                p1
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Positional Accuracy {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.poac_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Positional Accuracy: {}'.format(str(e)))

    def run_cmpl(self, comparison_sdf, apply_edits=True):

        try:
            new_flag = self.set_selected('cmpl')

            df = completeness(
                self.selected,
                self.features,
                comparison_sdf
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Completeness {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.cmpl_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Completeness: {}'.format(str(e)))

    def run_curr(self, p1, date='1901-1-1', apply_edits=True):

        try:
            new_flag = self.set_selected('curr')

            df = temporal_currency(
                self.selected,
                self.features,
                p1,
                date
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Temporal Currency {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.curr_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Temporal Currency: {}'.format(str(e)))

    def run_them(self, p1, apply_edits=True):

        try:
            new_flag = self.set_selected('them')

            df = thematic_accuracy(
                self.selected,
                self.features,
                p1
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Thematic Accuracy {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.them_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Thematic Accuracy: {}'.format(str(e)))

    def run_srln(self, p1, p2, search_value=1001, apply_edits=True):

        try:
            new_flag = self.set_selected('srln')

            df = source_lineage(
                self.selected,
                self.features,
                p1,
                p2,
                search_value
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Source Lineage {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.srln_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Source Lineage: {}'.format(str(e)))

    def run_logc(self, p1, p2, p3, p4, apply_edits=True):

        try:
            new_flag = self.set_selected('logc')

            df = logical_consistency(
                self.selected,
                self.features,
                self.feat_url,
                p1,
                p2,
                p3,
                p4
            )

            if new_flag:
                return [
                    df,
                    self.create_layer(
                        df,
                        'Logical Consistency {}'.format(round(time.time()))
                    )
                ]

            else:
                if apply_edits:
                    return [
                        df,
                        self.update_layer(
                            df,
                            self.logc_url
                        )
                    ]

                else:
                    return df

        except Exception as e:
            print('Exception Running Source Lineage: {}'.format(str(e)))
