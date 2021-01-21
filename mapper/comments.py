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

    item['article_join'] = {}
    item['article_join']['name'] = 'comment'
    item['article_join']['parent'] = item['ARTICLES_ID']
    return item