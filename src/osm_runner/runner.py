from .utils import Format, Output, Filters, Elements

from arcgis.geometry import Point, Polyline, Polygon
from arcgis.features import SpatialDataFrame

from datetime import date
import requests


def gen_osm_sdf(geom_type, bound_box, osm_tag=None, time_one=None, time_two=None, present=False):

    geom_type = geom_type.lower()

    if geom_type not in ['point', 'line', 'polygon']:
        raise Exception('Geometry Type "{0}" Does Not Match Input Options: point|line|polygon'.format(geom_type))

    else:
        osm_element = Elements.get(geom_type)

        query = get_query(osm_element, bound_box, osm_tag, time_one, time_two, present)

        osm_response = get_osm_elements(query)

        if geom_type == 'point':
            base_sdf = build_node_sdf(osm_response)

        else:
            base_sdf = build_ways_sdf(osm_response, geom_type)

        sdf = fields_cleaner(base_sdf)

        return sdf


def get_query(osm_el, b_box, o_tag, t1, t2, present_flag):

    if osm_el.lower() not in ['node', 'way']:
        raise Exception('OSM Element {0} Does Not Match Configuration Options: node|way'.format(osm_el))

    head = get_query_head(Format, t1, t2, present_flag)

    if o_tag:

        o_tag = o_tag.lower()
        filters = Filters.get(o_tag)

        if filters:
            filters = [f.lower() for f in filters]
            f = '|'.join(filters)
            f_clause = '["' + o_tag + '"~"' + f + '"]'
            return ';'.join([
                head,
                ''.join([str(osm_el), f_clause, str(b_box)]),
                Output
            ])
            # E.G. [out:json];way["highway"~"primary|residential"](bounding_box);(._;>;);out geom qt;

        else:
            f_clause = '["' + o_tag + '"]'
            return ';'.join([
                head,
                ''.join([str(osm_el), f_clause, str(b_box)]),
                Output
            ])
            # E.G. [out:json];way["highway"](bounding_box);(._;>;);out geom qt;

    else:
        return ';'.join([
            head,
            ''.join([str(osm_el), str(b_box)]),
            Output
        ])
        # E.G. [out:json];way(bounding_box);(._;>;);out geom qt;


def get_query_head(f, t_1, t_2, p_flag):

    if not t_1 and not t_2:
        return f

    else:
        if p_flag:
            if t_1 and not t_2:
                d = '[diff: "' + t_1 + '", "' + date.today().strftime('%Y-%m-%d') + '"]'

            elif t_2 and not t_1:
                d = '[diff: "' + t_2 + '", "' + date.today().strftime('%Y-%m-%d') + '"]'

            else:
                raise Exception('Invalid Parameters - Please Only Specify One Time Parameter When Using Present')

        else:
            if t_1 and not t_2:
                d = '[date: "' + t_1 + '"]'

            elif t_2 and not t_1:
                d = '[date: "' + t_2 + '"]'

            else:
                d = '[diff: "' + t_1 + '", "' + t_2 + '"]'

    return ''.join([f, d])


def get_osm_elements(osm_query):

    osm_api = 'https://overpass-api.de/api/interpreter'

    r = requests.get(osm_api, data=osm_query)

    if r.status_code == 200:

        if len(r.json()['elements']) == 0:

            try:
                raise Exception('OSM Returned Zero Results with Remark: {}'.format(r.json()['remark']))

            except KeyError:
                raise Exception('OSM Returned Zero Results for Query: {}'.format(osm_query))

        else:
            return r.json()['elements']

    elif r.status_code == 429:
        raise Exception('OSM Request Limit Reached. Please Try Again in a Few Minutes . . .')

    else:
        raise Exception('OSM Returned Status Code: {0}'.format(r.status_code))


def build_node_sdf(n_list):

    # Dictionary For Geometries & IDs
    geo_dict = {"geo": []}
    val_dict = {'osm_id': []}

    # Dictionary For Incoming Tags
    for n in n_list:
        n_tags = n['tags'].keys()
        for tag in n_tags:
            if tag not in val_dict.keys():
                val_dict[tag] = []

    # Build Lists
    for n in n_list:
        try:
            # Populate Tags
            for tag in [key for key in val_dict.keys() if key != 'osm_id']:
                val_dict[tag].append(str(n['tags'].get(tag, 'Null')))

            # Populate Geometries & IDs
            point = Point({
                "x": n['lon'],
                "y": n['lat'],
                "spatialReference": {"wkid": 4326}
            })
            geo_dict['geo'].append(point)
            val_dict['osm_id'].append(str(n['id']))

        except Exception as ex:
            print('Node ID {0} Raised Exception: {1}'.format(n['id'], str(ex)))

    try:
        return SpatialDataFrame(val_dict, geometry=geo_dict['geo'])

    except TypeError:
            raise Exception('Ensure ArcPy is Included in Python Interpreter')


def build_ways_sdf(o_response, g_type):

    # Extract Relevant Way Elements from OSM Response
    if g_type == 'polygon':
        ways = [e for e in o_response if e['type'] == 'way' and e['nodes'][0] == e['nodes'][-1]]
    else:
        ways = [e for e in o_response if e['type'] == 'way' and e['nodes'][0] != e['nodes'][-1]]

    # Dictionary For Geometries & IDs
    geo_dict = {'geo': []}
    val_dict = {'osm_id': []}

    # Dictionary For Incoming Tags
    for w in ways:
        w_tags = w['tags'].keys()
        for tag in w_tags:
            if tag not in val_dict.keys():
                val_dict[tag] = []

    # Build Lists
    for w in ways:
        try:
            # Populate Tags
            for tag in [key for key in val_dict.keys() if key != 'osm_id']:
                val_dict[tag].append(str(w['tags'].get(tag, 'Null')))

            # Populate Geometries & IDs
            coords = [[e['lon'], e['lat']] for e in w.get('geometry')]
            if g_type == 'polygon':
                poly = Polygon({"rings":  [coords], "spatialReference": {"wkid": 4326}})
            else:
                poly = Polyline({"paths": [coords], "spatialReference": {"wkid": 4326}})

            geo_dict['geo'].append(poly)
            val_dict['osm_id'].append(str(w['id']))

        except Exception as ex:
            print('Way ID {0} Raised Exception: {1}'.format(w['id'], str(ex)))

    try:
        return SpatialDataFrame(val_dict, geometry=geo_dict['geo'])

    except TypeError:
        raise Exception('Ensure ArcPy is Included in Python Interpreter')


def fields_cleaner(b_sdf):

    # Set Cutoff & Collect Field List
    cutoff = int(len(b_sdf) * .99)
    f_list = list(b_sdf)

    # Flag Fields Where >= 99% of Data is Null
    fields = []
    for f in f_list:
        try:
            if b_sdf[f].dtype == 'object' and f != 'SHAPE':
                null_count = b_sdf[f].value_counts().get('Null', 0)
                if null_count >= cutoff:
                    fields.append(f)
        except:
            print('Cannot Determine Null Count for Field {0}'.format(str(f)))
            continue

    # Drop Flagged Fields & Return
    if fields:
        b_sdf.drop(fields, axis=1, inplace=True)
        return b_sdf

    else:
        return b_sdf
