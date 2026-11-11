import json


def parse_line(raw, source, start):
    value = json.loads(raw.decode('utf-8'))
    if not value['device'] or not value['epoch'] or int(value['sequence']) < 0:
        raise ValueError('设备记录标识不合法')
    return dict(key=f"{value['device']}:{value['epoch']}:{value['sequence']}",
                value=value['value'], source=source, byte_start=start, byte_end=start + len(raw))


def import_file(raw, source):
    records = []
    position = 0
    for line in raw.splitlines(keepends=True):
        if not line.endswith(b'\n'):
            raise ValueError('文件含未结束行')
        records.append(parse_line(line, source, position))
        position += len(line)
    return records


def collect(payload):
    return import_file(payload['text'].encode('utf-8'), payload['source'])
