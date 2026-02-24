import os


class Config:
    """Application configuration.

    Keep technical settings and data paths centralized here to avoid hardcoding
    in runtime modules.
    """

    # Target & Path
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    DB_DIR = os.path.join(BASE_DIR, "db")
    LOG_DIR = os.path.join(BASE_DIR, "logs")
    DB_PATH = os.path.join(DB_DIR, "crawler_state.db")

    # Scraper Settings
    BASE_URL = "https://news.detik.com"
    INDEX_PATH = "/indeks"
    MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "5"))
    RETRIES = int(os.getenv("RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
    TIMEOUT = int(os.getenv("TIMEOUT", "15"))

    # Rotation
    MAX_LINES_PER_FILE = int(os.getenv("MAX_LINES_PER_FILE", "10000"))

    # Crawl scope
    CATEGORY = os.getenv("CATEGORY", "politik")
    INDEX_MAX_PAGES = int(os.getenv("INDEX_MAX_PAGES", "5"))

    # Rclone
    RCLONE_REMOTE = os.getenv("RCLONE_REMOTE", "gdrive_corpus")
    RCLONE_DEST = os.getenv("RCLONE_DEST", "research/corpus_indonesia/politik")

    # Runtime
    USER_AGENT = os.getenv(
        "USER_AGENT",
        (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
    )


for _path in (Config.DATA_DIR, Config.DB_DIR, Config.LOG_DIR):
    os.makedirs(_path, exist_ok=True)
