from datetime import datetime

def mapper(item, config):
    if 'CREATED_AT' in item:
        try:
            ms = item['CREATED_AT'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['CREATED_AT'] = timestamp
        except Exception as e:
            item['CREATED_AT'] = item['CREATED_AT']
    if 'UPDATED_AT' in item:
        try:
            ms = item['UPDATED_AT'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['UPDATED_AT'] = timestamp
        except Exception as e:
            item['UPDATED_AT'] = item['UPDATED_AT']
    
    if item['ID']:
        article_id = 'article-' + str(int(float(item['ID'])))
        item['article_id'] = article_id

    item['article_join'] = {}
    item['article_join']['name'] = 'article'
    return item
   