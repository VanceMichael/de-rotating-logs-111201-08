"""按事件脚本模拟设备端写入：本地写入、轮转、截断和重启。

fixtures/log-events.json 描述事件顺序；create 强制产生新文件身份（设备重启后
同名文件不再是同一份），epoch 表示设备新的启动批次，仅作说明。
"""
import os
from pathlib import Path


def apply_event(directory, event):
    directory = Path(directory)
    op = event['op']
    if op == 'write':
        (directory / event['path']).write_bytes(event['text'].encode('utf-8'))
    elif op == 'append_bytes':
        with open(directory / event['path'], 'ab') as stream:
            stream.write(bytes.fromhex(event['hex']))
    elif op == 'rotate':
        os.replace(directory / event['from'], directory / event['to'])
    elif op == 'create':
        target = directory / event['path']
        if target.exists():
            target.unlink()
        open(target, 'xb').close()
    elif op == 'truncate':
        with open(directory / event['path'], 'r+b') as stream:
            stream.truncate(0)
    else:
        raise ValueError(f'未知事件: {op}')
