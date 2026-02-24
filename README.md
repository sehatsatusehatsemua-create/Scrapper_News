# Detik News Corpus Scraper

Pipeline scraping corpus berita + komentar dengan arsitektur 2 tahap:

1. **Indexer (`indexer.py`)**: crawling halaman indeks Detik News lalu menyimpan URL artikel ke SQLite queue (`PENDING`).
2. **Worker (`worker.py`)**: mengambil URL `PENDING`, scraping detail artikel + komentar, menulis JSONL streaming dengan rotasi file, lalu update status `DONE/ERROR`.

## Arsitektur

- `config.py`: semua konfigurasi path, retry, timeout, rclone, dan scope crawler.
- `state.py`: state management queue berbasis SQLite (resume-safe).
- `scraper_utils.py`: HTTP fetch + retry exponential backoff + parser index/artikel.
- `writer.py`: JSONL streaming writer + rotation + JSON validity + MD5 checksum.
- `upload.sh`: upload otomatis dengan `rclone move --min-age 15m`.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Menjalankan

### 1) Index URL artikel

```bash
python indexer.py --max-pages 20 --category politik
```

### 2) Worker scrape corpus

```bash
python worker.py --batch-size 25
```

### 3) Upload data lama ke GDrive (opsional)

```bash
./upload.sh
```

## Resume behavior

Kalau proses mati di tengah jalan, cukup jalankan ulang indexer/worker. Queue status disimpan di SQLite (`db/crawler_state.db`) sehingga worker akan lanjut dari item `PENDING`.

## Struktur output

- `data/politik_1.jsonl`, `data/politik_2.jsonl`, ... (rotasi per `MAX_LINES_PER_FILE`)
- `data/*.md5` checksum file
- `logs/indexer.log`, `logs/worker.log`, `logs/rclone.log`
