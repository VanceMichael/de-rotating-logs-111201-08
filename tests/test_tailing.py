import json
import shutil
import unittest
from pathlib import Path
import tempfile

from collector.follow import Follower, status_view
from collector.simulator import apply_event
from collector.storage import Store


def line(device, epoch, sequence, value='正常'):
    return (json.dumps(dict(device=device, epoch=epoch, sequence=sequence, value=value),
                       ensure_ascii=False) + '\n').encode('utf-8')


class FollowerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.directory = self.root / 'logs'
        self.directory.mkdir()
        self.store = Store(self.root / 'follow.sqlite3')
        self.follower = Follower(self.store, self.directory)

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def write(self, name, content):
        (self.directory / name).write_bytes(content)

    def append(self, name, content):
        with open(self.directory / name, 'ab') as stream:
            stream.write(content)

    def scan(self):
        return self.follower.scan_once()

    def reopen(self):
        self.store.close()
        self.store = Store(self.root / 'follow.sqlite3')
        self.follower = Follower(self.store, self.directory)

    def test_fixture_events_replay(self):
        events = json.loads(Path('fixtures/log-events.json').read_text(encoding='utf-8'))['events']
        for event in events:
            apply_event(self.directory, event)
            self.scan()
        first = events[0]['text'].encode('utf-8')
        records = self.store.records()
        self.assertEqual([row['key'] for row in records], ['r:b1:1'])
        self.assertEqual(records[0]['source'], 'rig.log')
        self.assertEqual(records[0]['byte_start'], 0)
        self.assertEqual(records[0]['byte_end'], len(first))
        view = status_view(self.store)
        rotated = next(item for item in view['files'] if item['path'] == 'rig.log.1')
        fresh = next(item for item in view['files'] if item['path'] == 'rig.log')
        self.assertNotEqual(rotated['identity'], fresh['identity'])
        self.assertEqual(records[0]['file'], rotated['identity'])
        self.assertEqual(rotated['confirmed'], len(first))
        self.assertEqual(rotated['pending_bytes'], 3)
        self.assertEqual(rotated['pending_preview'], '高')
        self.assertEqual(rotated['last_device'], 'r')
        self.assertEqual(fresh['confirmed'], 0)
        self.assertEqual(view['gaps'], [])
        self.assertEqual(view['defects'], [])
        self.reopen()
        self.scan()
        self.assertEqual(len(self.store.records()), 1)
        self.assertEqual(self.store.gaps(), [])

    def test_split_utf8_waits_for_remaining_bytes(self):
        content = line('r', 'b1', 1, '高')
        cut = content.index(b'\xab')
        self.write('rig.log', content[:cut])
        self.scan()
        self.assertEqual(self.store.records(), [])
        self.assertEqual(self.store.file_states()[0]['pending'], content[:cut])
        self.append('rig.log', content[cut:])
        self.scan()
        records = self.store.records()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['value'], '高')
        self.assertEqual(self.store.file_states()[0]['pending'], b'')
        self.assertEqual(self.store.defects(), [])

    def test_bad_line_keeps_range_and_good_lines_survive(self):
        good1 = line('r', 'b1', 1)
        good2 = line('r', 'b1', 2)
        bad = b'{"device":"r","epoch":"b1",garbage\n'
        self.write('rig.log', good1 + bad + good2)
        self.scan()
        records = self.store.records()
        self.assertEqual([row['key'] for row in records], ['r:b1:1', 'r:b1:2'])
        self.assertEqual(records[1]['byte_start'], len(good1) + len(bad))
        defects = self.store.defects()
        self.assertEqual(len(defects), 1)
        self.assertEqual(defects[0]['byte_start'], len(good1))
        self.assertEqual(defects[0]['byte_end'], len(good1) + len(bad))
        self.assertIn('JSON', defects[0]['reason'])

    def test_invalid_utf8_line_isolated(self):
        good1 = line('r', 'b1', 1)
        good2 = line('r', 'b1', 2)
        self.write('rig.log', good1 + b'\xff\xfe\n' + good2)
        self.scan()
        self.assertEqual([row['key'] for row in self.store.records()], ['r:b1:1', 'r:b1:2'])
        defects = self.store.defects()
        self.assertEqual(len(defects), 1)
        self.assertIn('UTF-8', defects[0]['reason'])
        self.assertEqual(defects[0]['byte_start'], len(good1))

    def test_crash_after_commit_resumes_without_duplicates(self):
        first = line('r', 'b1', 1)
        second = line('r', 'b1', 2)
        self.write('rig.log', first + second)
        follower = Follower(self.store, self.directory, chunk_size=len(first))
        original = self.store.commit_progress

        def explode(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError('模拟提交后崩溃')

        self.store.commit_progress = explode
        with self.assertRaises(RuntimeError):
            follower.scan_once()
        self.store.commit_progress = original
        self.reopen()
        self.scan()
        keys = [row['key'] for row in self.store.records()]
        self.assertEqual(sorted(keys), ['r:b1:1', 'r:b1:2'])
        self.assertEqual(len(keys), len(set(keys)))
        self.scan()
        self.assertEqual(len(self.store.records()), 2)

    def test_crash_before_commit_recovers(self):
        first = line('r', 'b1', 1)
        second = line('r', 'b1', 2)
        self.write('rig.log', first + second)
        follower = Follower(self.store, self.directory, chunk_size=len(first))

        def explode(*args, **kwargs):
            raise RuntimeError('模拟提交前崩溃')

        self.store.commit_progress = explode
        with self.assertRaises(RuntimeError):
            follower.scan_once()
        del self.store.commit_progress
        self.reopen()
        self.scan()
        keys = [row['key'] for row in self.store.records()]
        self.assertEqual(sorted(keys), ['r:b1:1', 'r:b1:2'])

    def test_copy_truncate_rotation_overlap_dedupes(self):
        first = line('r', 'b1', 1)
        second = line('r', 'b1', 2)
        third = line('r', 'b1', 3)
        self.write('rig.log', first + second)
        self.scan()
        shutil.copyfile(self.directory / 'rig.log', self.directory / 'rig.log.1')
        with open(self.directory / 'rig.log', 'wb') as stream:
            stream.write(second + third)
        self.scan()
        keys = sorted(row['key'] for row in self.store.records())
        self.assertEqual(keys, ['r:b1:1', 'r:b1:2', 'r:b1:3'])
        self.assertEqual(self.store.gaps(), [])

    def test_vanished_file_marks_gap(self):
        first = line('r', 'b1', 1)
        fragment = b'{"device":"r","epoch":"b1","sequence":2,"value":"'
        self.write('rig.log', first + fragment)
        self.scan()
        (self.directory / 'rig.log').unlink()
        summary = self.scan()
        self.assertEqual([row['key'] for row in self.store.records()], ['r:b1:1'])
        gaps = self.store.gaps()
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]['byte_start'], len(first))
        self.assertEqual(gaps[0]['byte_end'], len(first) + len(fragment))
        self.assertIn('消失', gaps[0]['reason'])
        self.assertEqual(len(summary['gaps']), 1)
        files = status_view(self.store)['files']
        self.assertEqual(files[0]['status'], 'lost')

    def test_truncate_marks_gap_for_unconfirmed_tail(self):
        first = line('r', 'b1', 1)
        fragment = b'{"device":"r","epoch":"b1","sequence":2'
        third = line('r', 'b1', 3)
        self.write('rig.log', first)
        self.scan()
        self.append('rig.log', fragment)
        self.scan()
        with open(self.directory / 'rig.log', 'wb') as stream:
            stream.write(third)
        self.scan()
        self.assertEqual([row['key'] for row in self.store.records()], ['r:b1:1', 'r:b1:3'])
        gaps = self.store.gaps()
        self.assertEqual(len(gaps), 1)
        self.assertEqual(gaps[0]['byte_start'], len(first))
        self.assertEqual(gaps[0]['byte_end'], len(first) + len(fragment))
        self.assertIn('截断', gaps[0]['reason'])

    def test_same_inode_rewrite_detected_by_head(self):
        first = line('r', 'b1', 1)
        rewritten = line('r', 'b9', 1, '完全不同的内容，而且更长')
        self.write('rig.log', first)
        self.scan()
        with open(self.directory / 'rig.log', 'wb') as stream:
            stream.write(rewritten)
        self.scan()
        keys = sorted(row['key'] for row in self.store.records())
        self.assertEqual(keys, ['r:b1:1', 'r:b9:1'])
        self.assertEqual(self.store.gaps(), [])

    def test_restart_same_sequence_is_new_record(self):
        self.write('rig.log', line('r', 'b1', 1))
        self.scan()
        (self.directory / 'rig.log').unlink()
        self.write('rig.log', line('r', 'b2', 1))
        self.scan()
        keys = sorted(row['key'] for row in self.store.records())
        self.assertEqual(keys, ['r:b1:1', 'r:b2:1'])
        self.assertEqual(self.store.gaps(), [])

    def test_oversized_line_recorded_and_reader_continues(self):
        follower = Follower(self.store, self.directory, chunk_size=8, max_pending=128)
        junk = b'x' * 300
        good = line('r', 'b1', 1)
        self.write('rig.log', junk + b'\n' + good)
        follower.scan_once()
        defects = self.store.defects()
        self.assertEqual(len(defects), 1)
        self.assertEqual(defects[0]['reason'], '行超出长度上限')
        self.assertEqual(defects[0]['byte_start'], 0)
        self.assertEqual(defects[0]['byte_end'], len(junk) + 1)
        self.assertEqual([row['key'] for row in self.store.records()], ['r:b1:1'])
        state = self.store.file_states()[0]
        self.assertEqual(state['confirmed'], len(junk) + 1 + len(good))

    def test_large_file_streams_in_chunks(self):
        content = b''.join(line('r', 'b1', index) for index in range(200))
        self.write('rig.log', content)
        follower = Follower(self.store, self.directory, chunk_size=64)
        follower.scan_once()
        self.assertEqual(len(self.store.records()), 200)
        self.assertEqual(self.store.file_states()[0]['confirmed'], len(content))

    def test_status_shows_device_stuck_mid_line(self):
        fragment = b'{"device":"r9","epoch":"b7","sequ'
        self.write('rig.log', fragment)
        self.scan()
        files = status_view(self.store)['files']
        self.assertEqual(files[0]['pending_device'], 'r9')
        self.assertEqual(files[0]['pending_bytes'], len(fragment))
        self.assertEqual(files[0]['unread'], 0)

    def test_missing_directory_does_not_mark_gaps(self):
        self.write('rig.log', line('r', 'b1', 1))
        self.scan()
        follower = Follower(self.store, self.root / 'missing')
        summary = follower.scan_once()
        self.assertIn('error', summary)
        self.assertEqual(self.store.gaps(), [])


if __name__ == '__main__':
    unittest.main()
