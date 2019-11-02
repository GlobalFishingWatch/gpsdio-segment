import json
import dateutil.parser

def read_json(src):
    for x in src:
        x = x.strip()
        if x:
            obj = json.loads(x)
            if 'timestamp' in obj:
                obj['timestamp'] = dateutil.parser.parse(obj['timestamp'])
            yield obj