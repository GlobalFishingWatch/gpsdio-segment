import json
import dateutil.parser
import random

def read_json(src, add_msgid=True):
    for x in src:
        x = x.strip()
        if x:
            obj = json.loads(x)
            if 'timestamp' in obj:
                obj['timestamp'] = dateutil.parser.parse(obj['timestamp'])
            if add_msgid and 'msgid' not in obj:
                obj['msgid'] = random.random()
            yield obj