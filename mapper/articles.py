from datetime import datetime
import re

def remove_tag(content):
   regex = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
   
   cleantext = re.sub(regex, '', content , 0).strip()
   return cleantext

def mapper(item, config):
    if 'CONTENT' in item and item['CONTENT']:
        # content_utf8 = item['CONTENT'].encode('utf-8').strip().rstrip('\n').rstrip('&nbsp').rstrip('&gt;').rstrip('&lt;')
        content_utf8 = item['CONTENT'].encode('utf-8').strip()
        content_clean = remove_tag(content_utf8)
        content = content_clean.decode('utf-8')

        item['CONTENT'] = content
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
   