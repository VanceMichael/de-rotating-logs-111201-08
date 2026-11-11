import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        rows = collect(self.payload)
        self.assertEqual(rows[0]['byte_start'], 0)
        self.assertEqual(rows[0]['byte_end'], len(self.payload['text'].encode()))
        with self.assertRaises(ValueError):
            import_file(b'{}', 'x')

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
