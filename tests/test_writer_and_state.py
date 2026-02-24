import os
import tempfile
import unittest
from pathlib import Path

from config import Config
from state import claim_pending, enqueue_urls, init_db, mark_done, mark_error, stats
from writer import DataWriter, validate_jsonl, write_md5


class StateAndWriterTest(unittest.TestCase):
    def test_state_queue_flow(self):
        with tempfile.TemporaryDirectory() as tmp:
            original_db = Config.DB_PATH
            Config.DB_PATH = str(Path(tmp) / "state.db")
            try:
                init_db()
                inserted = enqueue_urls([
                    ("https://news.detik.com/a", "politik", "2024-01-01"),
                    ("https://news.detik.com/b", "politik", "2024-01-02"),
                ])
                self.assertEqual(inserted, 2)

                rows = claim_pending(1)
                self.assertEqual(len(rows), 1)
                url = rows[0]["url"]
                mark_error(url, "failed")
                self.assertEqual(stats().get("PENDING"), 2)

                rows = claim_pending(2)
                for row in rows:
                    mark_done(row["url"])

                self.assertGreaterEqual(stats().get("DONE", 0), 1)
            finally:
                Config.DB_PATH = original_db

    def test_writer_rotation_and_integrity(self):
        with tempfile.TemporaryDirectory() as tmp:
            original_dir = Config.DATA_DIR
            original_max = Config.MAX_LINES_PER_FILE
            Config.DATA_DIR = tmp
            Config.MAX_LINES_PER_FILE = 2
            try:
                writer = DataWriter(prefix="politik")
                p1 = writer.write({"id": 1})
                writer.write({"id": 2})
                p2 = writer.write({"id": 3})

                self.assertEqual(Path(p1).name, "politik_1.jsonl")
                self.assertEqual(Path(p2).name, "politik_2.jsonl")

                ok, count = validate_jsonl(str(Path(tmp) / "politik_1.jsonl"))
                self.assertTrue(ok)
                self.assertEqual(count, 2)

                md5_path = write_md5(str(Path(tmp) / "politik_2.jsonl"))
                self.assertTrue(os.path.exists(md5_path))
            finally:
                Config.DATA_DIR = original_dir
                Config.MAX_LINES_PER_FILE = original_max


if __name__ == "__main__":
    unittest.main()
