import hashlib
from datetime import datetime

def mapper(item, config):
    if 'CREATED_AT' in item:
        try:
            ms = item['CREATED_AT'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['CREATED_AT'] = timestamp
        except Exception as e:
            item['CREATED_AT'] = item['CREATED_AT']
    if 'LAST_LOGIN' in item:
        try:
            ms = item['LAST_LOGIN'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['LAST_LOGIN'] = timestamp
        except Exception as e:
            item['LAST_LOGIN'] = item['LAST_LOGIN']
    if 'BLOCKED_WHEN' in item:
        try:
            ms = item['BLOCKED_WHEN'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['BLOCKED_WHEN'] = timestamp
        except Exception as e:
            item['BLOCKED_WHEN'] = item['BLOCKED_WHEN']

    encryption_key = config['encryption_key']
    encryptText = encryption_key + str(item['USER_ID'])
    item['md5_id'] = hashlib.md5(encryptText).hexdigest()
    return item
   