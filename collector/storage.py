import hashlib
import json
import sqlite3
from contextlib import contextmanager


class Store:
    def __init__(self, path):
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS raw_objects(
                digest TEXT PRIMARY KEY, payload BLOB NOT NULL);
            CREATE TABLE IF NOT EXISTS imports(
                id INTEGER PRIMARY KEY, source TEXT NOT NULL,
                digest TEXT NOT NULL REFERENCES raw_objects(digest),
                UNIQUE(source,digest));
            CREATE TABLE IF NOT EXISTS records(
                record_key TEXT PRIMARY KEY, payload TEXT NOT NULL,
                import_id INTEGER NOT NULL REFERENCES imports(id));
            CREATE TABLE IF NOT EXISTS file_states(
                identity TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                status TEXT NOT NULL,
                confirmed INTEGER NOT NULL,
                skipped INTEGER NOT NULL,
                pending BLOB NOT NULL,
                head BLOB,
                size_seen INTEGER NOT NULL,
                last_device TEXT,
                last_key TEXT,
                updated TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS defects(
                identity TEXT NOT NULL,
                byte_start INTEGER NOT NULL,
                byte_end INTEGER NOT NULL,
                reason TEXT NOT NULL,
                preview TEXT NOT NULL,
                PRIMARY KEY(identity,byte_start));
            CREATE TABLE IF NOT EXISTS gaps(
                id INTEGER PRIMARY KEY,
                identity TEXT NOT NULL,
                path TEXT NOT NULL,
                byte_start INTEGER NOT NULL,
                byte_end INTEGER NOT NULL,
                reason TEXT NOT NULL,
                created TEXT NOT NULL);
        """)

    @contextmanager
    def transaction(self):
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def ingest(self, source, raw, records):
        digest = hashlib.sha256(raw).hexdigest()
        with self.transaction():
            self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
            self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (source, digest))
            import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (source, digest)).fetchone()[0]
            for record in records:
                key = record.get('key')
                if not isinstance(key, str) or not key:
                    raise ValueError('记录缺少有效标识')
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute("INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id", (key, encoded, import_id))
        return digest

    def records(self):
        return [json.loads(row[0]) for row in self.connection.execute("SELECT payload FROM records ORDER BY record_key")]

    def provenance(self, key):
        row = self.connection.execute("SELECT imports.source, imports.digest FROM records JOIN imports ON imports.id=records.import_id WHERE record_key=?", (key,)).fetchone()
        return dict(row) if row else None

    def file_state(self, identity):
        row = self.connection.execute("SELECT * FROM file_states WHERE identity=?", (identity,)).fetchone()
        return dict(row) if row else None

    def file_states(self):
        return [dict(row) for row in self.connection.execute("SELECT * FROM file_states ORDER BY path, identity")]

    def defects(self):
        return [dict(row) for row in self.connection.execute("SELECT * FROM defects ORDER BY identity, byte_start")]

    def gaps(self):
        return [dict(row) for row in self.connection.execute("SELECT * FROM gaps ORDER BY id")]

    def _gap_txn(self, identity, path, start, end, reason):
        self.connection.execute(
            "INSERT INTO gaps(identity,path,byte_start,byte_end,reason,created) VALUES (?,?,?,?,?,datetime('now'))",
            (identity, path, start, end, reason))

    def reconcile(self, observed):
        """按目录观测对账文件身份，返回新登记的数据缺口。

        observed: {identity: (name, size)}。重命名只更新路径；尺寸小于读取位置
        视为截断，未确认数据登记缺口后从头重读；读完前消失的身份登记缺口。
        """
        gaps = []
        with self.transaction():
            rows = list(self.connection.execute("SELECT * FROM file_states").fetchall())
            known = {row['identity'] for row in rows}
            for row in rows:
                identity = row['identity']
                current = observed.get(identity)
                if current is None:
                    if row['status'] != 'active':
                        continue
                    start = row['confirmed'] + row['skipped']
                    if start < row['size_seen'] or len(row['pending']):
                        reason = '文件在未读完时消失'
                        if len(row['pending']):
                            reason += f"（含未完成行 {len(row['pending'])} 字节）"
                        self._gap_txn(identity, row['path'], start, row['size_seen'], reason)
                        gaps.append(dict(identity=identity, path=row['path'], byte_start=start,
                                         byte_end=row['size_seen'], reason=reason))
                        status = 'lost'
                    else:
                        status = 'done'
                    self.connection.execute("UPDATE file_states SET status=?, updated=datetime('now') WHERE identity=?", (status, identity))
                    continue
                name, size = current
                position = row['confirmed'] + row['skipped'] + len(row['pending'])
                if size < position:
                    start = row['confirmed'] + row['skipped']
                    end = max(row['size_seen'], size)
                    if start < end or len(row['pending']):
                        reason = '文件被截断，未确认数据按缺口登记'
                        if len(row['pending']):
                            reason += f"（含未完成行 {len(row['pending'])} 字节）"
                        self._gap_txn(identity, name, start, end, reason)
                        gaps.append(dict(identity=identity, path=name, byte_start=start,
                                         byte_end=end, reason=reason))
                    self.connection.execute(
                        "UPDATE file_states SET path=?, status='active', confirmed=0, skipped=0, pending=?,"
                        " head=NULL, size_seen=?, updated=datetime('now') WHERE identity=?",
                        (name, b'', size, identity))
                else:
                    self.connection.execute(
                        "UPDATE file_states SET path=?, status='active', updated=datetime('now') WHERE identity=?",
                        (name, identity))
            for identity, (name, size) in observed.items():
                if identity not in known:
                    self.connection.execute(
                        "INSERT INTO file_states(identity,path,status,confirmed,skipped,pending,head,size_seen,updated)"
                        " VALUES (?,?,'active',0,0,?,NULL,?,datetime('now'))",
                        (identity, name, b'', size))
        return gaps

    def reset_file(self, identity, path, size, reason):
        """同一身份内容被整体改写：未确认数据登记缺口，从头重读。"""
        with self.transaction():
            row = self.connection.execute("SELECT * FROM file_states WHERE identity=?", (identity,)).fetchone()
            if row is None:
                return
            start = row['confirmed'] + row['skipped']
            if start < row['size_seen'] or len(row['pending']):
                self._gap_txn(identity, path, start, row['size_seen'], reason)
            self.connection.execute(
                "UPDATE file_states SET path=?, status='active', confirmed=0, skipped=0, pending=?,"
                " head=NULL, size_seen=?, updated=datetime('now') WHERE identity=?",
                (path, b'', size, identity))

    def commit_progress(self, identity, path, *, confirmed, skipped, pending, size_seen, head, items, defects):
        """记录、坏行与读取进度在同一事务提交，崩溃后可从确认进度续跑。"""
        with self.transaction():
            last_device = None
            last_key = None
            for raw, record in items:
                digest = hashlib.sha256(raw).hexdigest()
                self.connection.execute("INSERT OR IGNORE INTO raw_objects VALUES (?,?)", (digest, raw))
                self.connection.execute("INSERT OR IGNORE INTO imports(source,digest) VALUES (?,?)", (path, digest))
                import_id = self.connection.execute("SELECT id FROM imports WHERE source=? AND digest=?", (path, digest)).fetchone()[0]
                encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False)
                self.connection.execute(
                    "INSERT INTO records VALUES (?,?,?) ON CONFLICT(record_key) DO UPDATE SET payload=excluded.payload, import_id=excluded.import_id",
                    (record['key'], encoded, import_id))
                last_device = record.get('device')
                last_key = record['key']
            for defect in defects:
                if defect.get('finalize'):
                    self.connection.execute(
                        "UPDATE defects SET byte_end=? WHERE identity=? AND byte_start=?",
                        (defect['end'], identity, defect['start']))
                else:
                    self.connection.execute(
                        "INSERT INTO defects(identity,byte_start,byte_end,reason,preview) VALUES (?,?,?,?,?)"
                        " ON CONFLICT(identity,byte_start) DO UPDATE SET byte_end=excluded.byte_end,"
                        " reason=excluded.reason, preview=excluded.preview",
                        (identity, defect['start'], defect['end'], defect['reason'], defect['preview']))
            if last_key is None:
                self.connection.execute(
                    "UPDATE file_states SET path=?, status='active', confirmed=?, skipped=?, pending=?,"
                    " head=COALESCE(?,head), size_seen=?, updated=datetime('now') WHERE identity=?",
                    (path, confirmed, skipped, pending, head, size_seen, identity))
            else:
                self.connection.execute(
                    "UPDATE file_states SET path=?, status='active', confirmed=?, skipped=?, pending=?,"
                    " head=COALESCE(?,head), size_seen=?, last_device=?, last_key=?, updated=datetime('now') WHERE identity=?",
                    (path, confirmed, skipped, pending, head, size_seen, last_device, last_key, identity))

    def close(self):
        self.connection.close()
