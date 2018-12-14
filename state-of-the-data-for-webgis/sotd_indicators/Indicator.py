from sotd_indicators.indicators import *

from arcgis.gis import GIS
from arcgis.geometry import Geometry, filters

import configparser
import time
import datetime
import shutil

import ssl
ssl._create_default_https_context = ssl._create_unverified_context

class Indicator:

    def __init__(self):

        # GIS Resources
        self.pem                = None
        self.key                = None
        self.username           = None
        self.password           = None
        self.portal             = None
        self.write_to_gdb       = None
        self.temp_gdb_location  = None
        self.temp_gdb           = None
        self.gis_conn           = None
        self.return_all_records = False #True #None

        # Publishing GIS
        self.pub_pem      = None
        self.pub_key      = None
        self.pub_username = None
        self.pub_password = None
        self.pub_portal   = None
        self.pub_gis_conn = None

        # Thematic GIS
        self.them_pem      = None
        self.them_key      = None
        self.them_username = None
        self.them_password = None
        self.them_portal   = None
        self.them_gis_conn = None

        # Temporal Acuracy
        self.years              = None
        self.c_features         = None
        self.temp_acc_url       = None
        self.temp_acc_workspace = None
        self.temp_acc_features  = None

        # Selection Drivers
        self.grid_url = None
        self.feat_url = None
        self.lb_days  = None

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

        #Filter Parameters
        self.aoi_filter_fc = None
        self.aoi_filter_url = None
        self.aoi_username = None
        self.aoi_password = None
        self.aoi_portal = None
        self.aoi_key = None
        self.aoi_pem = None

        self.timestamp = self.create_datetimestamp()

    def create_datetimestamp(self):
        timestamp = '{:%Y_%b_%d_%H_%M_%S}'.format(datetime.datetime.now())
        return timestamp


    def load_config(self, config_file):

        # Read Incoming Config File
        config = configparser.ConfigParser()
        config.read_file(open(config_file))

        for section in config.sections():

            print('Loading Section: {}'.format(section))

            for k, v in dict(config.items(section)).items():
                self.__setattr__(k, v)

    def set_gis(self):

        # Set GeoEnrichment GIS Object
        if self.them_key != None and self.them_pem != None:

            self.them_gis_conn = GIS(
                url=self.them_portal,
                key_file=self.them_key,
                cert_file=self.them_pem,
                verify_cert=False
            )

        elif self.them_password != None and self.them_username != None:
            self.them_gis_conn = GIS(
                url=self.them_portal,
                username=self.them_username,
                password=self.them_password
            )

        else:
            self.them_gis_conn = GIS()

        if self.key != None and self.pem != None:

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
            )

        else:
            self.gis_conn = GIS()

        # Setting Publication GIS
        if self.pub_key != None and self.pub_pem != None:

            self.pub_gis_conn = GIS(
                url=self.pub_portal,
                key_file=self.pub_key,
                cert_file=self.pub_pem,
                verify_cert=False
            )
        elif self.pub_username != None and self.pub_password != None:

            self.pub_gis_conn = GIS(
                url=self.pub_portal,
                username=self.pub_username,
                password=self.pub_password,
                verify_cert=False
            )

        else:
            self.pub_gis_conn = GIS()



    def set_grid_sdf(self, use_sp_query=False):


        if self.aoi_filter_fc:

            import arcpy

            grid_desc = arcpy.Describe(self.aoi_filter_fc)
            grid_sr = grid_desc.spatialReference
            grid_extent = grid_desc.extent

            sp_filter = filters.intersects(
                grid_extent,
                grid_sr
            )

            self.geometry_filter = sp_filter

        elif self.aoi_filter_url:

            if self.aoi_key != None and self.aoi_pem != None:
                self.aoi_gis = GIS(
                    url=self.aoi_portal,
                    key_file=self.aoi_key,
                    cert_file=self.aoi_pem,
                    verify_cert=False
                )
            elif self.aoi_username != None and self.aoi_password != None:
                self.aoi_gis = GIS(
                    self.aoi_portal,
                    self.aoi_username,
                    self.aoi_password
                )
            else:
                self.aoi_gis = GIS()


            fl = FeatureLayer(
                self.aoi_filter_url,
                gis=self.aoi_gis
            )

            geometry = dict(fl.properties.extent)

            sp_filter = filters.intersects(
                geometry,
                geometry['spatialReference']
            )

            self.geometry_filter = sp_filter

        else:
            self.geometry_filter = None

        if not self.grid_url:
            raise Exception('Grid URL Not Set')
        else:

            if use_sp_query:

                dates = get_dates_in_range(int(self.lb_days))
                query_string = form_query_string(dates)

                grid_fl = FeatureLayer(url=self.grid_url, gis=self.gis_conn)

                print(self.geometry_filter)

                print("getting grid_sdf")

                # Potential Bug in 1.4.1 and previous where the query needs to be executed more than once
                # for it to start working
                try:
                    print("Running Query - First Attempt")
                    self.grid_sdf = grid_fl.query(
                        where=query_string,
                        geometry_filter=self.geometry_filter,
                        return_all_records=self.return_all_records,
                    ).df
                except:
                    print("Running Query - Second Attempt")
                    self.grid_sdf = grid_fl.query(
                        where=query_string,
                        geometry_filter=self.geometry_filter,
                        return_all_records=self.return_all_records,
                    ).df

                print(len(self.grid_sdf))

                print("Done Querying for Records to Update.")

            else:

                print("Using Geometry Filter.")

                grid_fl = FeatureLayer(url=self.grid_url, gis=self.gis_conn)

                try:
                    print("Running Query - First Attempt")
                    self.grid_sdf = grid_fl.query(
                        return_all_records=self.return_all_records,
                    ).df
                except:
                    print("Running Query - Second Attempt")
                    self.grid_sdf = grid_fl.query(
                        return_all_records=self.return_all_records,
                    ).df

                print(len(self.grid_sdf))

                print("Done Querying for Records to Update.")

        if len(self.grid_sdf)>0:
            self.temp_gdb = create_temp_gdb(self.temp_gdb_location, self.timestamp)

    def set_features(self):

        df_list = []

        for idx, row in enumerate(self.grid_sdf.iterrows()):#self.grid_sdf.iterrows()):

            geom = Geometry(row[1].SHAPE)

            print(idx)

            sp_filter = filters.intersects(geom, self.grid_wkid)

            data_fl = FeatureLayer(url=self.feat_url, gis=self.gis_conn)

            ## Change return all records to True
            df_list.append(
                data_fl.query(geometry_filter=sp_filter,
                              return_all_records=self.return_all_records).df
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
                df_current = data_fl.query(geometry_filter=sp_filter,
                                           return_all_records=self.return_all_records).df

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
                    out_sdf = data_fl.query(geometry_filter=sp_filter,
                                            return_all_records=self.return_all_records).df

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
                    out_sdf.merge(data_fl.query(geometry_filter=sp_filter,
                                                return_all_records=self.return_all_records).df)

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

        self.set_selected('poac')
        new_flag = False#self.set_selected('poac')

        #print(p1)
        print("positional_accuracy")
        df = positional_accuracy(
            self.selected,
            self.features,
            p1
        ).drop(['index'], axis=1, inplace=False)

        print('DF Records: {}'.format(len(df)))
        if self.write_to_gdb:
            df.to_featureclass(self.temp_gdb, 'poac_' + self.timestamp, overwrite=True)
            update_insert_features(
                os.path.join(self.temp_gdb, 'poac_' + self.timestamp),
                os.path.join(self.write_to_gdb, 'poac '),
                'poac'
            )
            return df
        if new_flag:
            #print(df.to_featureclass)
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
            self.set_selected('cmpl')
            new_flag = False#self.set_selected('cmpl')

            df = completeness(
                self.selected,
                self.features,
                comparison_sdf
            ).drop(['index'], axis=1, inplace=False)

            if self.write_to_gdb:
                df.to_featureclass(self.temp_gdb, 'cmpl_' + self.timestamp, overwrite=True)
                update_insert_features(
                    os.path.join(self.temp_gdb, 'cmpl_' + self.timestamp),
                    os.path.join(self.write_to_gdb, 'cmpl'),
                    'cmpl'
                )
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
            self.set_selected('curr')
            new_flag = False#self.set_selected('curr')

            df = temporal_currency(
                self.selected,
                self.features,
                p1,
                date
            ).drop(['index'], axis=1, inplace=False)

            if self.write_to_gdb:
                df.to_featureclass(self.temp_gdb, 'curr_' + self.timestamp, overwrite=True)
                update_insert_features(
                    os.path.join(self.temp_gdb, 'curr_' + self.timestamp),
                    os.path.join(self.write_to_gdb, 'curr'),
                    'curr'
                )
                return df
            if new_flag:
                #print(df)
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

    def run_them(self, p1, apply_edits=False):

        #try:
        self.set_selected('them')
        new_flag = False#self.set_selected('them')

        df = thematic_accuracy(
            self.selected,
            self.features,
            p1,
            self.them_gis_conn,
            self.them_pop
        ).drop(['index'], axis=1, inplace=False)

        if self.write_to_gdb:
            df.to_featureclass(self.temp_gdb, 'them_' + self.timestamp, overwrite=True)
            update_insert_features(
                os.path.join(self.temp_gdb, 'them_' + self.timestamp),
                os.path.join(self.write_to_gdb, 'them'),
                'them'
            )
            return df

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

        #except Exception as e:
        #    print('Exception Running Thematic Accuracy: {}'.format(str(e)))

    def run_srln(self, p1, p2=None, search_value=1001, apply_edits=True):

        try:
            self.set_selected('srln')
            new_flag = False#self.set_selected('srln')

            df = source_lineage(
                self.selected,
                self.features,
                p1,
                p2,
                search_value
            ).drop(['index'], axis=1, inplace=False)

            if self.write_to_gdb:
                df.to_featureclass(self.temp_gdb, 'srln_' + self.timestamp, overwrite=True)
                update_insert_features(
                    os.path.join(self.temp_gdb, 'srln_' + self.timestamp),
                    os.path.join(self.write_to_gdb, 'srln'),
                    'srln'
                )
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
            self.set_selected('logc')
            new_flag = False#self.set_selected('logc')

            df = logical_consistency(
                self.selected,
                self.features,
                FeatureLayer(self.feat_url, gis=self.gis_conn),
                p1,
                p2,
                p3,
                p4
            ).drop(['index'], axis=1, inplace=False)

            if self.write_to_gdb:
                df.to_featureclass(self.temp_gdb, 'logc_' + self.timestamp, overwrite=True)
                update_insert_features(
                    os.path.join(self.temp_gdb, 'logc_' + self.timestamp),
                    os.path.join(self.write_to_gdb, 'logc'),
                    'logc'
                )
                return df

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


    def run_temp_acc(self, apply_edits=True):

        try:
            #new_flag = self.set_selected('temp_acc')

            years = [int(y) for y in self.years.split(',')]

            ## If features are online
            # temporal_accuracy(
            #     self.c_features,
            #     self.curr_url,
            #     self.temp_acc_workspace,
            #     self.temp_acc_features + "_" + self.timestamp,
            #     years,
            #     self.pub_gis_conn
            # )

            #If pulling features from GDB
            self.temp_acc_features = os.path.join(self.temp_gdb, 'temp_acc' + "_" + self.timestamp)
            temporal_accuracy_from_currency_fc(
                c_features = self.c_features,
                curr_features = os.path.join(self.temp_gdb, 'curr_' + self.timestamp),
                output_features = self.temp_acc_features,
                years = years
            )#.drop(['index'], axis=1, inplace=False)

            update_insert_features(
                self.temp_acc_features,
                os.path.join(self.write_to_gdb, 'temp_acc'),
                'curr'
            )

            # zip temp_acc_features
            zip_name = os.path.dirname(self.temp_acc_features)
            zipped_gdb = zip_name + ".zip"
            zip_folder(zip_name,
                       zipped_gdb)

            # Check for existing GDB in Portal
            if apply_edits:
                temporal_accuracy_gdb = self.pub_gis_conn.content.search(
                    "Temporal Accuracy",
                    item_type="File Geodatabase"
                )

                if len(temporal_accuracy_gdb) > 0:
                    temporal_accuracy_gdb[0].delete()

                #if new_flag:
                print("Publishing a new Temporal Accuracy service")
                gdb = self.pub_gis_conn.content.add({"title": "Temporal Accuracy"}, zipped_gdb)
                published_service = gdb.publish(overwrite=True)
                gdb.delete()

            return published_service

        except Exception as e:
            print('Exception Running Temporal Accuracy: {}'.format(str(e)))