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

        osm_response = get_osm(query)

        if len(osm_response) == 0:
            raise Exception('OSM Returned Zero Results for Query: {0}'.format(query))

        if geom_type == 'point':
            val, geo = build_node_dicts(osm_response)

        else:
            val, geo = build_ways_dicts(osm_response, geom_type)

        try:
            return SpatialDataFrame(val, geometry=geo['geo'])

        except TypeError:
            raise Exception('Ensure ArcPy is Included in Python Interpreter')


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
            if t_1:
                diff = '[diff: "' + t_1 + '", "' + date.today().strftime('%Y-%m-%d') + '"]'
            else:
                raise Exception('Present Flag Requires a Value for Time One')

        else:
            if t_1 and not t_2:
                diff = '[diff: "' + t_1 + '"]'

            elif t_2 and not t_1:
                diff = '[diff: "' + t_2 + '"]'

            else:
                diff = '[diff: "' + t_1 + '", "' + t_2 + '"]'

        return ''.join([f, diff])


def get_osm(osm_query):

    osm_api = 'https://overpass-api.de/api/interpreter'

    r = requests.get(osm_api, data=osm_query)

    if r.status_code == 200:
        try:
            return r.json()['elements']
        except KeyError:
            raise Exception('OSM JSON Response Did Not Include Elements Key')
    else:
        raise Exception('OSM Returned Status Code: {0}'.format(r.status_code))


def build_node_dicts(n_list):

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

    return val_dict, geo_dict


def build_ways_dicts(o_response, g_type):

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

    return val_dict, geo_dict
