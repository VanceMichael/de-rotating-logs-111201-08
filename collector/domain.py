import json


def _reject_constant(name):
    raise ValueError(f'测值数值非法: {name}')


def parse_line(raw, source, start, identity=None):
    """解析单行字节为领域记录；任何行级问题都抛出带原因的 ValueError。"""
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as error:
        raise ValueError(f'行不是有效 UTF-8（{error.reason}）') from error
    try:
        value = json.loads(text, parse_constant=_reject_constant)
    except json.JSONDecodeError as error:
        raise ValueError(f'行不是合法 JSON（{error.msg}）') from error
    if not isinstance(value, dict):
        raise ValueError('行不是 JSON 对象')
    missing = [field for field in ('device', 'epoch', 'sequence', 'value') if field not in value]
    if missing:
        raise ValueError(f'记录缺少字段: {",".join(missing)}')
    device = value['device']
    epoch = value['epoch']
    try:
        sequence = int(value['sequence'])
    except (TypeError, ValueError):
        raise ValueError('序号不是整数') from None
    if not device or not epoch or sequence < 0:
        raise ValueError('设备记录标识不合法')
    record = dict(key=f'{device}:{epoch}:{sequence}', device=device, epoch=epoch,
                  sequence=sequence, value=value['value'], source=source,
                  byte_start=start, byte_end=start + len(raw))
    if identity is not None:
        record['file'] = identity
    return record


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
