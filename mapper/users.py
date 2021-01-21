import hashlib
from datetime import datetime

def mapper(item):
    if 'created_at' in item:
        try:
            ms = item['created_at'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['created_at'] = timestamp
        except Exception as e:
            item['created_at'] = item['created_at']
    if 'last_login' in item:
        try:
            ms = item['last_login'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['last_login'] = timestamp
        except Exception as e:
            item['last_login'] = item['last_login']
    if 'blocked_when' in item:
        try:
            ms = item['blocked_when'] / 1000000
            timestamp = datetime.fromtimestamp(ms).strftime("%Y-%m-%dT%H:%M:%S")
            item['blocked_when'] = timestamp
        except Exception as e:
            item['blocked_when'] = item['blocked_when']

    ENCRYPTION_KEY = 'medistreamkingwangjjang'
    encryptText = ENCRYPTION_KEY + str(item['user_id'])
    item['md5_id'] = hashlib.md5(encryptText).hexdigest()
    return item
   