import argparse
import asyncio
from urllib.parse import urlencode

import httpx

from config import Config
from scraper_utils import fetch_with_retry, parse_index_page, setup_logger
from state import enqueue_urls, init_db

logger = setup_logger("indexer", f"{Config.LOG_DIR}/indexer.log")


async def crawl_index(max_pages: int, category: str) -> None:
    init_db()

    headers = {"User-Agent": Config.USER_AGENT}
    async with httpx.AsyncClient(timeout=Config.TIMEOUT, headers=headers, follow_redirects=True) as client:
        total_enqueued = 0

        for page in range(1, max_pages + 1):
            params = {"page": page, "kategori": category}
            url = f"{Config.BASE_URL}{Config.INDEX_PATH}?{urlencode(params)}"
            logger.info("Fetching index page=%s url=%s", page, url)

            try:
                response = await fetch_with_retry(url, client)
            except Exception as exc:
                logger.error("Failed fetching page=%s error=%s", page, exc)
                continue

            rows = [
                (item["url"], category, item["publish_date"])
                for item in parse_index_page(response.text)
            ]
            inserted = enqueue_urls(rows)
            total_enqueued += inserted
            logger.info(
                "Index page=%s discovered=%s inserted=%s",
                page,
                len(rows),
                inserted,
            )

    logger.info("Done indexing. total_enqueued=%s", total_enqueued)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detik index crawler")
    parser.add_argument("--max-pages", type=int, default=Config.INDEX_MAX_PAGES)
    parser.add_argument("--category", default=Config.CATEGORY)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(crawl_index(max_pages=args.max_pages, category=args.category))
