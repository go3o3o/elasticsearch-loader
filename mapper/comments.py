from datetime import datetime

def mapper(item, config):
    if item['CREATED_AT']:
        ms = item['CREATED_AT'] / 1000000
        timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
        item['CREATED_AT'] = timestamp
    if item['UPDATED_AT']:
        ms = item['UPDATED_AT'] / 1000000
        timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
        item['UPDATED_AT'] = timestamp
    if item['ID']:
        comment_id = 'comment-' + str(item['ID'])
        item['comment_id'] = comment_id

    item['article_join'] = {}
    item['article_join']['name'] = 'comment'
    item['article_join']['parent'] = 'article-' + str(int(float(item['ARTICLES_ID'])))
    return item