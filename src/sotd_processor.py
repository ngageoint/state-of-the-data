#Using the ArcGIS API for Python
import arcgis
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.geometry import filters
from arcgis.geometry import Geometry

#Import logic to create layer selection
import temporal_currency as tc
import source_lineage as sl
import positional_accuracy as pa
import thematic_accuracy as them_acc
import completeness as comp
import logical_consistency as lc

import create_selection_layers as csl
import sotd_config as config

import datetime

def create_layers(grid, population, output_fc):
    them_acc_fc = them_acc.create_grls(grid,population, output_fc)
    them_acc.extend_table(them_acc_fc)

    return them_acc_fc

def processor():

    master_times = datetime.datetime.now()

    gis = GIS(config.portal, config.un, config.pw)

    fc = config.features_url
    polygon_grid = config.grid_url
    output_features = config.currency_url

    #in_fields = ",".join([config.currency_field, config.search_field,
    #            config.value_field, config.positional_acc_field,
    #            config.thematic_acc_field])

    return_all_records = False
    look_back_days = config.look_back_days

    dates = csl.get_dates_in_range(look_back_days)
    where_clause = csl.form_query_string(dates)

    grid_fl = FeatureLayer(url=polygon_grid)
    grid_sdf = grid_fl.query(return_all_records=False, where=where_clause).df

    geometry = grid_sdf.geometry
    sr = {'wkid':4326}
    sp_rel = "esriSpatialRelIntersects"

    for idx, row in enumerate(grid_sdf.iterrows()):
        geom = row[1].SHAPE
        ext = [geom.extent.lowerLeft.X+.1, geom.extent.lowerLeft.Y+.1,
               geom.extent.upperRight.X-.1, geom.extent.upperRight.Y-.1]

        new_geom = Geometry({
            "rings" : [[[geom.extent.upperRight.X-.1, geom.extent.lowerLeft.Y+.1], [geom.extent.lowerLeft.X+.1, geom.extent.lowerLeft.Y+.1], [geom.extent.lowerLeft.X+.1, geom.extent.upperRight.Y-.1], [geom.extent.upperRight.X-.1, geom.extent.upperRight.Y-.1], [geom.extent.upperRight.X-.1, geom.extent.lowerLeft.Y+.1]]],
            "spatialReference" : {"wkid" : 4326}
        })

        print(new_geom.extent)
        grid_filter = filters._filter(new_geom, sr, sp_rel)
        sp_filter = filters._filter(geom, sr, sp_rel)

        data_fl = FeatureLayer(url=fc)
        #out_fields=in_fields,
        data_sdf = data_fl.query(geometry_filter=sp_filter,return_geometry=True,
            return_all_records=return_all_records).df

        print('Processing Logical Consistency')
        lc_sdf, lc_fl = lc.logical_consisitency(gis, config.template_fc, config.template_gdb,
            config.attr_check_file, config.attr_check_tab,
            data_sdf, config.features_url,
            config.logical_consistency_url,
            grid_filter, geom,
            config.attr_error_field_count, config.attr_error_field_def)
        print(lc_sdf)
        update_features(lc_sdf, lc_fl)
        print('Logical Consistency Updated.')

        print('Processing temporal currency')
        tc_sdf, tc_fl = tc.temporal_currency(gis, data_sdf,
                        config.currency_url, grid_filter, geom,
                        config.currency_field)
        print(tc_sdf)
        #update_features(tc_sdf, tc_fl)
        print('Temporal Currency Updated')

        print('Processing source lineage')
        sl_sdf, sl_fl = sl.source_lineage(gis, data_sdf,
                        config.source_lineage_url, grid_filter, geom,
                        config.search_field, config.value_field)
        print(sl_sdf)
        #update_features(sl_sdf, sl_fl)
        print('Source Lineage Updated')

        print('Processing Positional Accuracy')
        pa_sdf, pa_fl = pa.positional_accuracy(gis, data_sdf, config.positional_acc_url, grid_filter, geom, config.positional_acc_field)
        print(pa_sdf)
        #update_features(pa_sdf, pa_fl)
        print('Positional Accuracy Updated')

        print('Processing Thematic Accuracy')
        them_acc_sdf, them_acc_fl = them_acc.thematic_accuracy(gis, data_sdf,
                        config.thematic_url, grid_filter, geom,
                        config.thematic_acc_field)
        print(them_acc_sdf)
        #update_features(them_acc_sdf, them_acc_fl)
        print('Theamatic Accuracy Updated')

        print('Processing Completeness')
        #osm_sdf = osm_gen('line', bbox, tag)
        completeness_sdf, completeness_fl = comp.completeness(gis, data_sdf,
                        osm_sdf, config.completeness_url, grid_filter, geom)
        print(completeness_sdf)
        #update_features(them_acc_sdf, them_acc_fl)
        print('Completeness Updated')

    print("Total Time %s" % (datetime.datetime.now() - master_times))

def update_features(df, feature_layer):
    out_sdf_as_featureset = df.to_featureset()
    #print(out_sdf_as_featureset)
    feature_layer.edit_features(updates=out_sdf_as_featureset)
    print(feature_layer)

#-------------------------------------------------------------------------------
if __name__ == "__main__":
    #env.overwriteOutput = True
    processor()