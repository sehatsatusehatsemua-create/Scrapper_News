import asyncio
import json
import logging
import random
import re
from datetime import datetime
from typing import Any, Optional

import httpx
from bs4 import BeautifulSoup

from config import Config


def setup_logger(name: str, file_name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    fh = logging.FileHandler(file_name)
    fh.setFormatter(formatter)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


async def fetch_with_retry(url: str, client: httpx.AsyncClient) -> httpx.Response:
    for i in range(Config.RETRIES):
        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
            resp = await client.get(url)
            resp.raise_for_status()
            return resp
        except Exception:
            if i == Config.RETRIES - 1:
                raise
            await asyncio.sleep(Config.RETRY_DELAY * (i + 1))


def normalize_article_url(url: str) -> str:
    url = url.split("?")[0]
    return url.rstrip("/")


def parse_index_page(html: str) -> list[dict[str, Optional[str]]]:
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict[str, Optional[str]]] = []

    for article in soup.select("article"):
        link_el = article.select_one("a")
        date_el = article.select_one(".media__date")
        if not link_el or not link_el.get("href"):
            continue

        url = normalize_article_url(link_el["href"])
        if "news.detik.com" not in url:
            continue

        publish_date = date_el.get_text(strip=True) if date_el else None
        results.append({"url": url, "publish_date": publish_date})

    return results


def _extract_json_from_script(text: str) -> Optional[Any]:
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return None


def extract_article(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.select_one("h1") or soup.select_one("title"))
    title_text = title.get_text(" ", strip=True) if title else ""

    content_parts = [p.get_text(" ", strip=True) for p in soup.select("div.detail__body-text p")]
    if not content_parts:
        content_parts = [p.get_text(" ", strip=True) for p in soup.select("article p")]

    publish_date = ""
    date_el = soup.select_one(".detail__date") or soup.select_one(".date")
    if date_el:
        publish_date = date_el.get_text(" ", strip=True)

    tags = [tag.get_text(strip=True) for tag in soup.select(".detail__body-tag a")]

    comments = extract_comments(soup)

    return {
        "url": url,
        "title": title_text,
        "publish_date": publish_date,
        "content": "\n".join([part for part in content_parts if part]),
        "tags": tags,
        "comments": comments,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }


def extract_comments(soup: BeautifulSoup) -> list[dict[str, str]]:
    comments: list[dict[str, str]] = []

    for comment_el in soup.select(".list-content__item, .comment-content, .komentar"):  # fallback selectors
        author = comment_el.select_one(".name, .username")
        body = comment_el.select_one("p, .content")
        if not body:
            continue
        comments.append(
            {
                "author": author.get_text(" ", strip=True) if author else "anonymous",
                "text": body.get_text(" ", strip=True),
            }
        )

    if comments:
        return comments

    for script in soup.find_all("script"):
        script_text = script.string or ""
        if "comment" not in script_text.lower():
            continue
        payload = _extract_json_from_script(script_text)
        if not isinstance(payload, dict):
            continue
        found = payload.get("comments") or payload.get("data", {}).get("comments")
        if isinstance(found, list):
            for item in found:
                if not isinstance(item, dict):
                    continue
                comments.append(
                    {
                        "author": str(item.get("author") or item.get("name") or "anonymous"),
                        "text": str(item.get("text") or item.get("comment") or ""),
                    }
                )

    return [c for c in comments if c["text"]]
