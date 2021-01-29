from datetime import datetime

def mapper(item, config):
    if item['CREATED_AT']:
        ms = item['CREATED_AT'] / 1000
        timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
        item['CREATED_AT'] = timestamp
    if item['UPDATED_AT']:
        ms = item['UPDATED_AT'] / 1000
        timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
        item['UPDATED_AT'] = timestamp
    if item['ID']:
        article_id = 'article-' + str(int(float(item['ID'])))
        item['article_id'] = article_id

    item['article_join'] = {}
    item['article_join']['name'] = 'article'
    return item
   