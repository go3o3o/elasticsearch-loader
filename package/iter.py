from .parsers import json, parquet, zip_longest
from .logger import log

from mapper.articles import mapper as articles
from mapper.comments import mapper as comments
from mapper.users import mapper as users

def grouper(iterable, n, fillvalue=None):
    'Collect data into fixed-length chunks or blocks'
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)


def bulk_builder(bulk, config):
    for item in filter(None, bulk):
        try:
            source = item
            if config['keys']:
                source = {x: y for x, y in item.items() if x in config['keys']}

            if config['keyName']:
                if config['keyName'] == 'articles':
                    source = articles(item, config)
                if config['keyName'] == 'comments':
                    source = comments(item, config)
                if config['keyName'] == 'users':
                    source = users(item, config)

            body = {'_index': config['index'],
                    '_type': config['type'],
                    '_source': source}
        
            if config['id_field']:
                if type(item[config['id_field']]) is float:
                    _id = int(float(item[config['id_field']]))
                else:
                    _id = item[config['id_field']]
                body['_id'] = _id
                body['_routing'] = _id

                if config['as_child']:
                    # body['_parent'] = item[config['parent_id_field']]
                    body['_routing'] = item[config['parent_id_field']]

            if config['update']:
                body['_op_type'] = 'update'
                body['doc'] = source
                del body['_source']

            if config['pipeline']:
                body['pipeline'] = config['pipeline']

            yield body
        except Exception as e:
            # print('error')
            log('error', 'reason: %s / item: %s' % (e, item))




def json_lines_iter(fle):
    for line in fle:
        yield json.loads(line)