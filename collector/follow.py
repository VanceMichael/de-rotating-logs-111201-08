"""持续读取器：按真实文件身份跟踪目录内的轮转日志。

设备端文件协议不变，监测日志仍是目录里的普通文件。文件身份取 (st_dev, st_ino)
加文件头指纹，与文件名无关：重命名轮转继续跟读，同名新文件从头读起，截断或
整体改写先登记数据缺口再重读。读取位置、未完成字节与确认进度随记录在同一
事务提交，提交前后退出都能续跑，有效数据不重不漏。按块流式读取并限制半行
缓存，目录增长再快也不会把全部文件内容搬进内存。
"""
import os
import re
from pathlib import Path

from .domain import parse_line

CHUNK_SIZE = 64 * 1024
MAX_PENDING = 1024 * 1024
HEAD_SIZE = 64

_DEVICE_PATTERN = re.compile(r'"device"\s*:\s*"((?:[^"\\]|\\.)*)')


def identify(stat):
    """文件身份：设备号与 inode 组合，同名文件未必是同一份。"""
    return f'{stat.st_dev:x}:{stat.st_ino:x}'


def _preview(raw):
    return raw[:80].decode('utf-8', errors='replace')


class Follower:
    def __init__(self, store, directory, chunk_size=CHUNK_SIZE, max_pending=MAX_PENDING):
        self.store = store
        self.directory = Path(directory)
        self.chunk_size = chunk_size
        self.max_pending = max_pending

    def scan_once(self):
        """扫描一轮目录：先对账文件身份，再逐文件续读新字节。"""
        if not self.directory.is_dir():
            return dict(error=f'目录不存在: {self.directory}')
        observed = {}
        with os.scandir(self.directory) as entries:
            for entry in entries:
                try:
                    if not entry.is_file():
                        continue
                    stat = entry.stat()
                except OSError:
                    continue
                observed[identify(stat)] = (entry.name, stat.st_size)
        gaps = self.store.reconcile(observed)
        imported = defects = 0
        for identity, (name, size) in sorted(observed.items(), key=lambda item: item[1][0]):
            count, broken = self._read_file(identity, name, size)
            imported += count
            defects += broken
        return dict(files=len(observed), imported=imported, defects=defects, gaps=gaps)

    def _read_file(self, identity, name, observed_size):
        row = self.store.file_state(identity)
        if row is None:
            return 0, 0
        position = row['confirmed'] + row['skipped'] + len(row['pending'])
        head = row['head']
        if observed_size <= position and head is None:
            return 0, 0
        try:
            stream = open(self.directory / name, 'rb')
        except OSError:
            return 0, 0  # 扫描间隙被移走，下一轮对账
        imported = defects = 0
        with stream:
            try:
                stat = os.fstat(stream.fileno())
            except OSError:
                return 0, 0
            if identify(stat) != identity:
                return 0, 0  # 打开的瞬间已换成另一份文件
            size = stat.st_size
            if head:
                # 尺寸不变也可能已被同身份重写（inode 复用、原地改写），核对文件头
                stream.seek(0)
                try:
                    current = stream.read(len(head))
                except OSError:
                    return 0, 0
                if current != head:
                    self.store.reset_file(identity, name, size, '文件内容被整体改写，未确认数据按缺口登记')
                    row = self.store.file_state(identity)
                    head = None
            elif size <= position:
                return 0, 0
            confirmed = row['confirmed']
            skipped = row['skipped']
            pending = bytes(row['pending'])
            position = confirmed + skipped + len(pending)
            capture = position == 0 and head is None
            new_head = head
            discarding = skipped > 0
            try:
                stream.seek(position)
            except OSError:
                return 0, 0
            while True:
                try:
                    chunk = stream.read(self.chunk_size)
                except OSError:
                    break
                if not chunk:
                    break
                if capture:
                    new_head = chunk[:HEAD_SIZE]
                    capture = False
                items = []
                broken = []
                if discarding:
                    index = chunk.find(b'\n')
                    if index < 0:
                        skipped += len(chunk)
                        self.store.commit_progress(identity, name, confirmed=confirmed, skipped=skipped,
                                                   pending=pending, size_seen=size, head=new_head,
                                                   items=(), defects=())
                        continue
                    start = confirmed
                    confirmed += skipped + index + 1
                    skipped = 0
                    discarding = False
                    broken.append(dict(start=start, end=confirmed, finalize=True))
                    chunk = chunk[index + 1:]
                buffer = pending + chunk
                parts = buffer.split(b'\n')
                for piece in parts[:-1]:
                    raw = piece + b'\n'
                    try:
                        record = parse_line(raw, name, confirmed, identity=identity)
                        items.append((raw, record))
                    except ValueError as error:
                        broken.append(dict(start=confirmed, end=confirmed + len(raw),
                                           reason=str(error), preview=_preview(raw)))
                    confirmed += len(raw)
                pending = parts[-1]
                if len(pending) > self.max_pending:
                    broken.append(dict(start=confirmed, end=confirmed + len(pending),
                                       reason='行超出长度上限', preview=_preview(pending)))
                    skipped = len(pending)
                    pending = b''
                    discarding = True
                self.store.commit_progress(identity, name, confirmed=confirmed, skipped=skipped,
                                           pending=pending, size_seen=size, head=new_head,
                                           items=items, defects=broken)
                imported += len(items)
                defects += len(broken)
        return imported, defects


def status_view(store):
    """现场视角：每个文件身份的进度、半行内容与设备、坏行与数据缺口。"""
    files = []
    for row in store.file_states():
        pending = row['pending']
        preview = _preview(pending)
        match = _DEVICE_PATTERN.search(preview)
        position = row['confirmed'] + row['skipped'] + len(pending)
        files.append(dict(identity=row['identity'], path=row['path'], status=row['status'],
                          confirmed=row['confirmed'], skipped=row['skipped'],
                          read_position=position, size_seen=row['size_seen'],
                          unread=max(row['size_seen'] - position, 0),
                          pending_bytes=len(pending), pending_preview=preview,
                          pending_device=match.group(1) if match else None,
                          last_device=row['last_device'], last_key=row['last_key'],
                          updated=row['updated']))
    return dict(files=files, defects=store.defects(), gaps=store.gaps())
