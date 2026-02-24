import argparse
import asyncio
from typing import Any

import httpx

from config import Config
from scraper_utils import extract_article, fetch_with_retry, setup_logger
from state import claim_pending, init_db, mark_done, mark_error, stats
from writer import DataWriter, validate_jsonl, write_md5

logger = setup_logger("worker", f"{Config.LOG_DIR}/worker.log")


async def process_single(url: str, client: httpx.AsyncClient, writer: DataWriter) -> None:
    try:
        response = await fetch_with_retry(url, client)
        article: dict[str, Any] = extract_article(response.text, url)
        output_file = writer.write(article)
        mark_done(url)
        logger.info("DONE url=%s file=%s", url, output_file)
    except Exception as exc:
        mark_error(url, str(exc))
        logger.error("ERROR url=%s message=%s", url, exc)


async def run_worker(batch_size: int) -> None:
    init_db()
    writer = DataWriter(prefix=Config.CATEGORY)
    headers = {"User-Agent": Config.USER_AGENT}

    async with httpx.AsyncClient(timeout=Config.TIMEOUT, headers=headers, follow_redirects=True) as client:
        while True:
            batch = claim_pending(batch_size)
            if not batch:
                logger.info("No pending queue. worker finished.")
                break

            sem = asyncio.Semaphore(Config.MAX_CONCURRENT)

            async def _guarded(url: str) -> None:
                async with sem:
                    await process_single(url, client, writer)

            await asyncio.gather(*[_guarded(row["url"]) for row in batch])
            logger.info("Progress stats=%s", stats())

    finalize_integrity()


def finalize_integrity() -> None:
    from pathlib import Path

    for path in sorted(Path(Config.DATA_DIR).glob("*.jsonl")):
        try:
            validate_jsonl(str(path))
            checksum = write_md5(str(path))
            logger.info("integrity_ok file=%s checksum=%s", path.name, checksum)
        except Exception as exc:
            logger.error("integrity_error file=%s message=%s", path.name, exc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detik worker crawler")
    parser.add_argument("--batch-size", type=int, default=25)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run_worker(batch_size=args.batch_size))
