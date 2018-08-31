from sotd_indicators.indicators import *

from arcgis.gis import GIS

import configparser
import time


class Indicator:

    def __init__(self):

        # GIS Resources
        self.pem      = None
        self.key      = None
        self.username = None
        self.password = None
        self.portal   = None
        self.debug    = None
        self.gis_conn = None

        # Publishing GIS
        self.pub_pem      = None
        self.pub_key      = None
        self.pub_username = None
        self.pub_password = None
        self.pub_portal   = None
        self.pub_gis_conn = None

        # GeoEnrichment GIS
        self.geo_pem      = None
        self.geo_key      = None
        self.geo_username = None
        self.geo_password = None
        self.geo_portal   = None
        self.geo_gis_conn = None

        # Thematic GIS
        self.them_pem      = None
        self.them_key      = None
        self.them_username = None
        self.them_password = None
        self.them_portal   = None
        self.them_gis_conn = None

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

        # Extra Values
        self.them_pop = None

    def load_config(self, config_file):

        # Read Incoming Config File
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        for section in config.sections():

            print('Loading Section: {}'.format(section))

            for k, v in dict(config.items(section)).items():
                self.__setattr__(k, v)

    def set_gis(self):

        # Set Publication GIS Object
        if self.pub_password != None and self.pub_username != None:
            self.pub_gis_conn = GIS(
                url=self.pub_portal,
                username=self.pub_username,
                password=self.pub_password
            )
        else:
            self.pub_gis_conn = None

        # Set GeoEnrichment GIS Object
        if self.geo_password != None and self.geo_username != None:
            self.geo_gis_conn = GIS(
                url=self.geo_portal,
                username=self.geo_username,
                password=self.geo_password
            )
        else:
            self.geo_gis_conn = None

        # Set GeoEnrichment GIS Object
        if self.them_password != None and self.them_username != None:
            self.them_gis_conn = GIS(
                url=self.them_portal,
                username=self.them_username,
                password=self.them_password
            )
        else:
            self.them_gis_conn = None

        if self.key != None and self.pem != None:
            import ssl
            ssl._create_default_https_context = ssl._create_unverified_context
            self.gis_conn = GIS(
                url=self.portal,
                key_file=self.key,
                cert_file=self.pem,
                verify_cert=False
            )

        elif self.username != None and self.password != None:
            self.gis_conn = GIS(
                url=self.portal,
                username=self.username,
                password=self.password,
                verify_cert=False
            )

        else:
            self.gis_conn = GIS()

    def set_grid_sdf(self, lb_days=1000, use_query=False):

        if not self.grid_url:
            raise Exception('Grid URL Not Set')
        else:

            if use_query:

                dates = get_dates_in_range(lb_days)

                grid_fl = FeatureLayer(url=self.grid_url, gis=self.gis_conn)

                self.grid_wkid = grid_fl.properties.extent.spatialReference.wkid

                self.grid_sdf = grid_fl.query(where=form_query_string(dates)).df

            else:

                grid_fl = FeatureLayer(url=self.grid_url, gis=self.gis_conn)

                self.grid_wkid = grid_fl.properties.extent.spatialReference.wkid

                self.grid_sdf = grid_fl.query(return_all_records=False).df

    def set_features(self):

        df_list = []

        for idx, row in enumerate(self.grid_sdf.iterrows()):#self.grid_sdf.iterrows()):

            geom = Geometry(row[1].SHAPE)

            sp_filter = filters.intersects(geom, self.grid_wkid)

            data_fl = FeatureLayer(url=self.feat_url, gis=self.gis_conn)

            ## Change return all records to True
            df_list.append(
                data_fl.query(geometry_filter=sp_filter, return_all_records=False).df
            )

        self.features = df_list

    def set_selected(self, indicator):

        created = False
        out_sdf = None

        # Indicator Feature Layer
        indicator_url = self.__getattribute__(indicator + '_url')
        print(indicator_url)
        #data_fl = FeatureLayer(url=indicator_url, gis=self.gis_conn)

        # Enumerate Used to Leverage the Merge Method on the Data Frame.
        # Set the First and Merge the Remainder to the First.
        for idx, row in enumerate(self.grid_sdf.iterrows()):

            # Negative Buffer Used to Avoid Selecting More Than 1 Cell
            sp_filter = filters.intersects(
                Geometry(row[1].SHAPE).buffer(-.1),
                self.grid_wkid
            )

            if not indicator_url:
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
                df_current = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

            # Set The First Instance
            if idx == 0:

                # Check If Cell Found in Target Indicator Layer
                if df_current.empty:

                    # Use Grid SDF Row Geom to Insert Empty Record for Target Indicator
                    data_fl.edit_features(adds=[
                        {
                            'attributes': {},
                            'geometry': json.loads(row[1].SHAPE.JSON)
                        }
                    ])

                    # Select Newly Created Cell As Input
                    out_sdf = data_fl.query(geometry_filter=sp_filter, return_all_records=False).df

                else:
                    # Use Matched Grid Cell
                    out_sdf = df_current

            # Append Additional Data
            else:

                # Check If Cell Found in Target Indicator Layer
                if df_current.empty:

                    # Use Grid SDF Row Geom to Insert Empty Record for Target Indicator
                    data_fl.edit_features(adds=[
                        {
                            'attributes': {},
                            'geometry': json.loads(row[1].SHAPE.JSON)
                        }
                    ])

                    # Select Newly Created Cell As Input
                    out_sdf.merge(data_fl.query(geometry_filter=sp_filter, return_all_records=False).df)

                else:
                    # Use Matched Grid Cell
                    out_sdf = out_sdf.merge_datasets(df_current)

        self.selected = out_sdf.reset_index(drop=False)
        print("Selected: " + str(len(out_sdf)))
        return created

    def create_layer(self, df, title):

        print('Creating New Hosted Feature Layer: {}'.format(title))

        if self.pub_gis_conn==None:
            new_layer = df.to_featurelayer(
                title,
                gis=self.gis_conn
            )
        else:
            new_layer = df.to_featurelayer(
                title,
                gis=self.pub_gis_conn
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

        #try:

        print("HERERERERRE")
        new_flag = self.set_selected('poac')

        print("positional_accuracy")
        df = positional_accuracy(
            self.selected,
            self.features,
            p1
        )

        print('DF Records: {}'.format(len(df)))
        if self.debug:
            df.to_featureclass(self.debug, 'poac', overwrite=True)
            return df
        if new_flag:
            print(df.to_featureclass)
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

        #except Exception as e:
        #    print('Exception Running Positional Accuracy: {}'.format(str(e)))

    def run_cmpl(self, comparison_sdf, apply_edits=True):

        try:
            new_flag = self.set_selected('cmpl')

            df = completeness(
                self.selected,
                self.features,
                comparison_sdf
            )
            if self.debug:
                df.to_featureclass(self.debug, 'cmpl', overwrite=True)
                return df
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
            if self.debug:
                df.to_featureclass(self.debug, 'curr', overwrite=True)
                return df
            if new_flag:
                print(df)
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

            # Determine If Configured GIS Objects Support GeoEnrichment/getSamples
            # This only will work with AGOL accounts
            #validate_geo_gis(self.geo_gis_conn)
            #validate_img_gis(self.geo_gis_conn, self.them_pop)

            #df = thematic_accuracy(
            #    self.selected,
            #    self.features,
            #    p1,
            #    self.geo_gis_conn,
            #    self.them_pop
            #)

            df = thematic_accuracy(
                self.selected,
                self.features,
                p1,
                self.them_gis_conn,
                self.them_pop
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

    def run_srln(self, p1, p2=None, search_value=1001, apply_edits=True):

        try:
            new_flag = self.set_selected('srln')

            df = source_lineage(
                self.selected,
                self.features,
                p1,
                p2,
                search_value
            )
            if self.debug:
                df.to_featureclass(self.debug, 'srln', overwrite=True)
                return df
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
                FeatureLayer(self.feat_url, gis=self.gis_conn),
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
