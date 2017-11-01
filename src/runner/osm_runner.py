from runner.osm_utils import Format, Period, Filters, OSM_Features

from arcgis.geometry import Point, Polyline, Polygon
from arcgis.features import SpatialDataFrame

from multiprocessing import Pool, cpu_count
from collections import defaultdict
from functools import partial
import requests
import time


def gen_osm_sdf(geom_type, bounding_box, osm_tag=None):

    geom_type = geom_type.lower()

    if geom_type not in ['point', 'line', 'polygon']:
        raise Exception('Geometry Type {0} Does Not Match Input Options: point|line|polygon'.format(geom_type))

    else:
        osm_element = OSM_Features.get(geom_type)

        query = get_query(osm_element, bounding_box, osm_tag)

        osm_elements = get_osm(query)

        node_list = [node for node in osm_elements if node['type'] == 'node']

        if geom_type == 'point':
            sdf = setup_node_pool(node_list)
            if isinstance(sdf, SpatialDataFrame):
                return sdf
            else:
                raise Exception('OSM Runner Failed to Generate Valid SpatialDataFrame')

        else:
            if geom_type == 'polygon':
                way_list = [
                    {'id': e['id'], 'tags': e.get('tags', None), 'nodes': e['nodes']}
                    for e in osm_elements if e['type'] == 'way' and e['nodes'][0] == e['nodes'][-1]
                ]

            else:
                way_list = [
                    {'id': e['id'], 'tags': e.get('tags', None), 'nodes': e['nodes']}
                    for e in osm_elements if e['type'] == 'way' and e['nodes'][0] != e['nodes'][-1]
                ]

            sdf = setup_ways_pool(node_list, geom_type, way_list)
            if isinstance(sdf, SpatialDataFrame):
                return sdf
            else:
                raise Exception('OSM Runner Failed to Generate Valid SpatialDataFrame')


def get_query(osm_el, b_box, o_tag=None):

    if osm_el not in ['node', 'way']:
        raise Exception('OSM Element {0} Does Not Match Configuration Options: node|way'.format(osm_el))

    if o_tag:

        filters = Filters.get(o_tag)

        if filters:
            # E.G. [out:json];way(bounding_box)["highway"~"primary|residential"];(._;>;);out body;
            f = '|'.join(filters)
            f_clause = '["' + o_tag + '"~"' + f + '"]'
            return ';'.join([
                Format,
                ''.join([ str(osm_el), str(b_box), f_clause ]),
                Period
            ])

        else:
            # E.G. [out:json];way(bounding_box)["highway"];(._;>;);out body;
            f_clause = '["' + o_tag + '"]'
            return ';'.join([
                Format,
                ''.join( [str(osm_el), str(b_box), f_clause ]),
                Period
            ])

    else:
        # E.G. [out:json];way(bounding_box);(._;>;);out body;
        return ';'.join([
            Format,
            ''.join( [str(osm_el), str(b_box)] ),
            Period
        ])


def get_osm(osm_query):

    osm_api = 'https://overpass-api.de/api/interpreter'

    r = requests.get(osm_api, data=osm_query)

    if r.status_code == 200:
        try:
            return r.json()['elements']
        except:
            raise Exception('OSM Failed to Return Elements For Processing')
    else:
        raise Exception('OSM Returned Status Code: {0}'.format(r.status_code))


def setup_node_pool(n_list):

    # Prepare Iterable For Multiprocessing Pool
    if cpu_count() < 4:
        raise Exception('At Least 4 Cores Required for setup_node_pool()')
    chunk = cpu_count() - 2
    steps = len(n_list) // chunk
    if steps < 1: steps = 1
    lists = [n_list[i:i + steps] for i in range(0, len(n_list), steps)]

    pool = Pool(processes=chunk)
    mp_result = pool.map_async(build_node_dict, lists)
    pool.close()
    pool.join()

    if mp_result.ready():
        res = mp_result.get()
        d_dict = defaultdict(list)
        for d in res:
            for key, value in d.items():
                d_dict[key].extend(value)

        return SpatialDataFrame({'ID': d_dict['ids'], 'NAME': d_dict['names']}, geometry=d_dict['geoms'])


def build_node_dict(n_list):

    node_dict = {"ids": [], "names": [], "geoms": []}

    for n in n_list:
        try:
            point = Point({
                "x": n['lon'],
                "y": n['lat'],
                "spatialReference": {"wkid": 4326}
            })

            _name = 'Undefined'
            tags = n.get("tags", None)
            if tags:
                name = tags.get("name", None)
                if name:
                    _name = name

            node_dict['ids'].append(str(n['id']))
            node_dict['names'].append(_name)
            node_dict['geoms'].append(point)

        except Exception as ex:
            print('Node ID {0} Raised Exception: {1}'.format(n['id'], str(ex)))

    return node_dict


def setup_ways_pool(n_list, g_type, w_list):

    # Prepare Iterable For Multiprocessing Pool
    if cpu_count() < 4:
        raise Exception('At Least 4 Cores Required for setup_ways_pool()')
    chunk = cpu_count() - 2
    steps = len(w_list) // chunk
    if steps < 1: steps = 1
    way_chunks = [w_list[i:i + steps] for i in range(0, len(w_list), steps)]

    pool = Pool(processes=chunk)
    mp_result = pool.map_async(partial(build_ways_dict, n_list, g_type), way_chunks)
    pool.close()
    pool.join()

    if mp_result.ready():
        res = mp_result.get()
        d_dict = defaultdict(list)
        for d in res:
            for key, value in d.items():
                d_dict[key].extend(value)

        return SpatialDataFrame({'ID': d_dict['ids'], 'NAME': d_dict['names']}, geometry=d_dict['geoms'])


def build_ways_dict(n_list, g_type, w_list):

    way_dict = {"ids": [], "names": [], "geoms": []}

    for w in w_list:
        try:
            coords = []
            for w_n in w['nodes']:
                for a_n in n_list:
                    if a_n['id'] == w_n:
                        coords.append([a_n['lon'], a_n['lat']])

            if g_type == 'polygon':
                poly = Polygon({
                    "rings": [coords],
                    "spatialReference": {"wkid": 4326}
                })

            else:
                poly = Polyline({
                    "paths": [coords],
                    "spatialReference": {"wkid": 4326}
                })

            _name = 'Undefined'
            tags = w.get("tags", None)
            if tags:
                name = tags.get("name", None)
                if name:
                    _name = name

            way_dict['ids'].append(str(w['id']))
            way_dict['names'].append(_name)
            way_dict['geoms'].append(poly)

        except Exception as ex:
            print('Way ID {0} Raised Exception: {1}'.format(w['id'], str(ex)))

    return way_dict


##if __name__ == "__main__":
##
##    start = time.time()
##
##    box = '(37.708132, -122.513617, 37.832132, -122.349607)'
##    tag = 'historic'
##
##    point_sdf = gen_osm_sdf('point', box, tag)
##    point_sdf.to_featureclass(r'C:\Temp\Examples.gdb', 'Point', True)
##
##    line_sdf = gen_osm_sdf('line', box, tag)
##    line_sdf.to_featureclass(r'C:\Temp\Examples.gdb', 'Line', True)
##
##    polygon_sdf = gen_osm_sdf('polygon', box, tag)
##    polygon_sdf.to_featureclass(r'C:\Temp\Examples.gdb', 'Polygon', True)
##
##    print('Execution in Seconds: ', time.time() - start)
