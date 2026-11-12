import argparse
import json
from pathlib import Path
import tempfile
import time

from .domain import collect
from .follow import Follower, status_view
from .simulator import apply_event
from .storage import Store
from .transport import Client, replay_server


def main():
    parser = argparse.ArgumentParser(description='本地采集与凭据查看')
    parser.add_argument('command', choices=['demo', 'import', 'show', 'serve', 'follow', 'replay', 'status'])
    parser.add_argument('--input', default='fixtures/sample.json')
    parser.add_argument('--events', default='fixtures/log-events.json')
    parser.add_argument('--db', default='var/records.sqlite3')
    parser.add_argument('--dir', default=None)
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--interval', type=float, default=1.0)
    parser.add_argument('--once', action='store_true')
    arguments = parser.parse_args()
    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db, arguments.port)
        return
    if arguments.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(arguments.input).read_bytes()
            with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
                status, headers, raw = Client().request(base + '/source')
                if status != 200:
                    raise RuntimeError('本地来源未返回成功响应')
                store = Store(Path(directory) / 'demo.sqlite3')
                rows = collect(json.loads(raw))
                digest = store.ingest('本地演示来源', raw, rows)
                print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)), ensure_ascii=False, indent=2))
                store.close()
        return
    if arguments.command == 'follow':
        if not arguments.dir:
            parser.error('follow 需要 --dir 指定日志目录')
        Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
        store = Store(arguments.db)
        follower = Follower(store, arguments.dir)
        try:
            while True:
                summary = follower.scan_once()
                if arguments.once or summary.get('error') or summary.get('imported') or summary.get('defects') or summary.get('gaps'):
                    print(json.dumps(summary, ensure_ascii=False), flush=True)
                if arguments.once:
                    break
                time.sleep(arguments.interval)
        except KeyboardInterrupt:
            pass
        finally:
            store.close()
        return
    if arguments.command == 'replay':
        events = json.loads(Path(arguments.events).read_text(encoding='utf-8'))['events']
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'logs'
            target.mkdir()
            store = Store(Path(directory) / 'replay.sqlite3')
            follower = Follower(store, target)
            try:
                steps = []
                for event in events:
                    apply_event(target, event)
                    steps.append(dict(event=event, scan=follower.scan_once()))
                print(json.dumps(dict(steps=steps, records=store.records(), status=status_view(store)),
                                 ensure_ascii=False, indent=2))
            finally:
                store.close()
        return
    if arguments.command == 'status':
        store = Store(arguments.db)
        try:
            print(json.dumps(status_view(store), ensure_ascii=False, indent=2))
        finally:
            store.close()
        return
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        if arguments.command == 'import':
            raw = Path(arguments.input).read_bytes()
            store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
