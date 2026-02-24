import hashlib
import json
import os
from pathlib import Path
from typing import Any

from config import Config


class DataWriter:
    def __init__(self, prefix: str = "politik"):
        self.prefix = prefix
        self.line_count = 0
        self.file_index = 1
        self.current_file = Path(Config.DATA_DIR) / self._get_filename()
        self._sync_state_from_disk()

    def _get_filename(self) -> str:
        return f"{self.prefix}_{self.file_index}.jsonl"

    def _sync_state_from_disk(self) -> None:
        while self.current_file.exists():
            with self.current_file.open("r", encoding="utf-8") as f:
                count = sum(1 for _ in f)
            if count < Config.MAX_LINES_PER_FILE:
                self.line_count = count
                return
            self.file_index += 1
            self.current_file = Path(Config.DATA_DIR) / self._get_filename()
        self.line_count = 0

    def _rotate_if_needed(self) -> None:
        if self.line_count < Config.MAX_LINES_PER_FILE:
            return
        self.file_index += 1
        self.line_count = 0
        self.current_file = Path(Config.DATA_DIR) / self._get_filename()

    def write(self, data: dict[str, Any]) -> Path:
        self._rotate_if_needed()
        os.makedirs(Config.DATA_DIR, exist_ok=True)

        with self.current_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
            self.line_count += 1

        return self.current_file


def validate_jsonl(path: str) -> tuple[bool, int]:
    valid_count = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            json.loads(stripped)
            valid_count += 1
    return True, valid_count


def write_md5(path: str) -> str:
    md5 = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)

    checksum_path = f"{path}.md5"
    digest = md5.hexdigest()
    with open(checksum_path, "w", encoding="utf-8") as out:
        out.write(f"{digest}  {os.path.basename(path)}\n")
    return checksum_path
