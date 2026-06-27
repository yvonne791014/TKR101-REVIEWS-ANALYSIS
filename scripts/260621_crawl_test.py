import asyncio
import argparse
import csv
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import requests
from playwright.async_api import Page, Response, async_playwright
from playwright_stealth import Stealth


DEFAULT_DEBUG_DIR = Path("/tmp/crawler_debug")
# Google Maps currently hides the existing-reviews tab from true headless
# Chromium. On GCP, keep this False and run the script through Xvfb:
# xvfb-run -a python 260618crawl_pw_batchexecute_to_csv_gcp.py
# Xvfb has no visible desktop window but receives the full Maps interface.
DEFAULT_HEADLESS = False
DEFAULT_NO_SANDBOX = True
SERVICE_NAME = "MapsUgcPostService.ListUgcPosts"

# Direct-run settings:
# Fill these values, then click "Run Python File" in VS Code.
RESTAURANT_ID = "ChIJ4WS_CUarQjQR5p36XIydHlY"
GOOGLE_MAPS_URL = "https://www.google.com/maps/search/?api=1&query=%E9%BA%B5%E6%87%B8%E4%B8%80%E7%94%9F%20-%20%E8%81%B7%E4%BA%BA%E9%BA%B5%E9%A4%A8&query_place_id=ChIJ4WS_CUarQjQR5p36XIydHlY"
OUTPUT_PATH = ""  # Empty: /tmp/reviews_<RESTAURANT_ID>.csv

SORT_LABELS = {
    "newest": "\u6700\u65b0",
    "highest": "\u8a55\u5206\u6700\u9ad8",
    "lowest": "\u8a55\u5206\u6700\u4f4e",
}
SORT_TYPES = {
    "newest": 2,
    "highest": 3,
    "lowest": 4,
}


def walk_json(node: Any):
    yield node
    if isinstance(node, list):
        for item in node:
            yield from walk_json(item)
    elif isinstance(node, dict):
        for item in node.values():
            yield from walk_json(item)


def google_maps_url_with_language(
    url: str,
    language: str = "zh-TW",
) -> str:
    parsed = urlparse(url)
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key != "hl"
    ]
    query_pairs.append(("hl", language))
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))


def google_maps_place_id_url(restaurant_id: str) -> str:
    query = urlencode(
        {
            "api": "1",
            "query": restaurant_id,
            "query_place_id": restaurant_id,
            "hl": "zh-TW",
        }
    )
    return f"https://www.google.com/maps/search/?{query}"


def is_review_text_candidate(value: str) -> bool:
    text = value.strip()
    if not text:
        return False
    if text.startswith(("http://", "https://", "//")):
        return False
    if text.startswith(("CIABI", "GUIDED_", "E:", "M:/")):
        return False
    if re.fullmatch(r"[a-z]{2}(?:-[A-Za-z]+)?", text):
        return False
    if text in {
        "\u9910\u9ede",
        "\u670d\u52d9",
        "\u6c23\u6c1b",
        "\u9910\u9ede\u985e\u578b",
        "\u8a02\u55ae\u985e\u578b",
        "\u5e73\u5747\u6bcf\u4eba\u6d88\u8cbb\u91d1\u984d",
        "\u5efa\u8b70\u7684\u9910\u9ede",
        "\u7528\u9910\u4eba\u6578",
        "\u7531 Google \u7ffb\u8b6f",
        "\u67e5\u770b\u539f\u6587",
        "\u986f\u793a\u539f\u6587",
    }:
        return False
    return True


def looks_like_chinese_text(text: str) -> bool:
    if not re.search(r"[\u4e00-\u9fff]", text):
        return False
    return not re.search(r"[\u3040-\u30ff\uac00-\ud7af]", text)


def looks_like_non_language_text(text: str) -> bool:
    return not re.search(r"[A-Za-z\u3040-\u30ff\uac00-\ud7af]", text)


def extract_text_tuple(node: Any) -> str:
    if not (
        isinstance(node, list)
        and len(node) >= 3
        and isinstance(node[0], str)
        and node[1] is None
        and isinstance(node[2], list)
        and len(node[2]) >= 2
        and all(isinstance(item, int) for item in node[2][:2])
    ):
        return ""

    text = node[0].strip()
    return text if is_review_text_candidate(text) else ""


def collect_review_text_candidates(node: Any) -> list[str]:
    candidates: list[str] = []
    for item in walk_json(node):
        text = extract_text_tuple(item)
        if text and text not in candidates:
            candidates.append(text)
    return candidates


def first_text(node: Any) -> str:
    if not isinstance(node, list):
        return ""

    language_candidates: list[str] = []
    for index, item in enumerate(node):
        if not (
            isinstance(item, list)
            and len(item) == 1
            and isinstance(item[0], str)
            and re.fullmatch(r"[a-z]{2}(?:-[A-Za-z]+)?", item[0])
        ):
            continue

        possible_texts = node[index + 1] if index + 1 < len(node) else None
        if not isinstance(possible_texts, list):
            continue
        for possible_text in possible_texts:
            text = extract_text_tuple(possible_text)
            if text:
                language_candidates.append(text)

    all_candidates = language_candidates + [
        text
        for text in collect_review_text_candidates(node)
        if text not in language_candidates
    ]
    for text in all_candidates:
        if looks_like_chinese_text(text):
            return text
    for text in all_candidates:
        if looks_like_non_language_text(text):
            return text
    return ""


def aspect_rating(content: Any, aspect_key: str):
    for item in walk_json(content):
        if not isinstance(item, list) or len(item) <= 11:
            continue
        marker = item[0]
        if (
            isinstance(marker, list)
            and marker
            and marker[0] == aspect_key
            and isinstance(item[11], list)
            and item[11]
        ):
            return item[11][0]
    return None


def parse_review_wrapper(
    wrapper: Any,
    restaurant_id: str | None = None,
) -> dict[str, Any] | None:
    if not (
        isinstance(wrapper, list)
        and wrapper
        and isinstance(wrapper[0], list)
    ):
        return None

    review = wrapper[0]
    if len(review) < 3:
        return None

    metadata = review[1] if isinstance(review[1], list) else []
    content = review[2] if isinstance(review[2], list) else []
    timestamp = (
        metadata[3]
        if len(metadata) > 3 and isinstance(metadata[3], (int, float))
        else metadata[2]
        if len(metadata) > 2 and isinstance(metadata[2], (int, float))
        else None
    )

    return {
        "review_id": review[0],
        "restaurant_id": restaurant_id,
        "review_timestamp": timestamp,
        "review_score": (
            content[0][0]
            if content
            and isinstance(content[0], list)
            and content[0]
            else None
        ),
        "review_content": first_text(content),
        "food_score": aspect_rating(
            content,
            "GUIDED_DINING_FOOD_ASPECT",
        ),
        "service_score": aspect_rating(
            content,
            "GUIDED_DINING_SERVICE_ASPECT",
        ),
        "atmosphere_score": aspect_rating(
            content,
            "GUIDED_DINING_ATMOSPHERE_ASPECT",
        ),
    }


def iter_listugc_payloads(raw_text: str):
    text = raw_text.strip()
    if text.startswith(")]}'"):
        text = text[4:].strip()

    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("[["):
            continue
        try:
            outer = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not isinstance(outer, list):
            continue
        for call in outer:
            if not (
                isinstance(call, list)
                and len(call) >= 3
                and isinstance(call[1], str)
                and SERVICE_NAME in call[1]
                and isinstance(call[2], str)
            ):
                continue
            try:
                yield json.loads(call[2])
            except json.JSONDecodeError:
                continue


def parse_batchexecute_text(
    raw_text: str,
    restaurant_id: str | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for payload in iter_listugc_payloads(raw_text):
        wrappers = (
            payload[2]
            if isinstance(payload, list) and len(payload) > 2
            else []
        )
        if not isinstance(wrappers, list):
            continue
        for wrapper in wrappers:
            row = parse_review_wrapper(
                wrapper,
                restaurant_id=restaurant_id,
            )
            if row:
                rows.append(row)
    return rows


def next_page_token_from_response(raw_text: str) -> str:
    """Return the next ListUgcPosts token from a batchexecute response."""
    for payload in iter_listugc_payloads(raw_text):
        if (
            isinstance(payload, list)
            and len(payload) > 1
            and isinstance(payload[1], str)
        ):
            return payload[1]
    return ""


class ListUgcRequestTemplate:
    """A captured browser request that requests can reuse for pagination."""

    def __init__(
        self,
        request_url: str,
        request_headers: dict[str, str],
        outer_request: list,
        sort_type: int,
    ):
        self.request_url = request_url
        self.request_headers = request_headers
        self.outer_request = outer_request
        self.sort_type = sort_type

    @classmethod
    def from_browser_request(
        cls,
        request_url: str,
        request_headers: dict[str, str],
        post_data: str,
        sort_type: int,
    ):
        form = parse_qs(post_data, keep_blank_values=True)
        values = form.get("f.req")
        if not values:
            raise ValueError("Captured request has no f.req field.")

        outer_request = json.loads(values[0])
        inner_request = json.loads(outer_request[0][0][1])
        if not (
            isinstance(inner_request, list)
            and len(inner_request) >= 2
            and isinstance(inner_request[1], list)
        ):
            raise ValueError("Unexpected ListUgcPosts f.req structure.")

        return cls(
            request_url=request_url,
            request_headers=request_headers,
            outer_request=outer_request,
            sort_type=sort_type,
        )

    def build_f_req(self, page_token: str = "") -> str:
        # Copy through JSON so the captured template remains unchanged.
        outer_request = json.loads(json.dumps(self.outer_request))
        inner_request = json.loads(outer_request[0][0][1])
        inner_request[1] = [10, page_token]
        inner_request[-1] = [self.sort_type]
        outer_request[0][0][1] = json.dumps(
            inner_request,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return json.dumps(
            outer_request,
            ensure_ascii=False,
            separators=(",", ":"),
        )


class ListUgcRequestCapture:
    """Capture the first sorted ListUgcPosts request made by the browser."""

    def __init__(self, sort_type: int):
        self.sort_type = sort_type
        self.template: ListUgcRequestTemplate | None = None
        self.captured = asyncio.Event()

    def handle_request(self, request):
        if self.template is not None or "batchexecute" not in request.url:
            return

        try:
            post_data = request.post_data or ""
        except UnicodeDecodeError:
            return
        if SERVICE_NAME not in post_data:
            return

        try:
            self.template = ListUgcRequestTemplate.from_browser_request(
                request_url=request.url,
                request_headers=dict(request.headers),
                post_data=post_data,
                sort_type=self.sort_type,
            )
        except (IndexError, TypeError, ValueError, json.JSONDecodeError):
            return
        self.captured.set()


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_optional_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None


def safe_label(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "debug"


async def save_debug_files(page: Page, debug_dir: Path, label: str):
    debug_dir.mkdir(parents=True, exist_ok=True)
    stem = safe_label(label)
    screenshot_path = debug_dir / f"{stem}.png"
    html_path = debug_dir / f"{stem}.html"

    try:
        await page.screenshot(path=str(screenshot_path), full_page=True)
        print(f"Saved debug screenshot: {screenshot_path}")
    except Exception as exc:
        print(f"Could not save debug screenshot: {exc}")

    try:
        html_path.write_text(await page.content(), encoding="utf-8")
        print(f"Saved debug HTML: {html_path}")
    except Exception as exc:
        print(f"Could not save debug HTML: {exc}")


async def click_side_and_reload(page: Page):
    viewport = page.viewport_size or {"width": 1280, "height": 1080}
    x = max(
        100,
        min(viewport["width"] - 80, int(viewport["width"] * 0.85)),
    )
    y = max(
        100,
        min(viewport["height"] - 80, int(viewport["height"] * 0.45)),
    )

    print("Clicking side area, then reloading to reveal review tab.")
    try:
        await page.mouse.click(x, y)
        await page.wait_for_timeout(700)
        await page.reload(
            wait_until="domcontentloaded",
            timeout=15000,
        )
    except Exception as exc:
        print(f"Side click/reload failed, continuing: {exc}")
    await page.wait_for_timeout(3000)


async def open_reviews_in_headless(page: Page):
    sort_button = page.locator(
        'button[aria-label*="\u6392\u5e8f"], '
        'button:has-text("\u6392\u5e8f")'
    ).first
    if await sort_button.count() > 0:
        return

    detail_pane = page.locator('div[role="main"]').first
    await detail_pane.wait_for(state="visible", timeout=10000)

    review_entry_selectors = [
        'button[jsaction*="reviewChart.moreReviews"]',
        'button[aria-label*="則評論"]',
        'button[aria-label*="篇評論"]',
        'button:has-text("則評論")',
        'div[role="button"]:has-text("則評論")',
    ]

    for _ in range(20):
        for selector in review_entry_selectors:
            entry = page.locator(selector).first
            if await entry.count() > 0 and await entry.is_visible():
                await entry.click()
                await page.wait_for_timeout(3000)
                return

        # Google Maps changes the sidebar's generated class names frequently.
        # Keeping the pointer inside the left pane and using the wheel is more
        # stable than targeting a specific internal scroll container.
        await page.mouse.move(300, 850)
        await page.mouse.wheel(0, 900)
        await page.wait_for_timeout(500)

    raise RuntimeError("Could not open the reviews list in headless mode.")


async def open_reviews_robust(page: Page):
    detail_pane = page.locator('div[role="main"]').first
    await detail_pane.wait_for(state="visible", timeout=10000)

    sort_selector = (
        'button[aria-label*="\u6392\u5e8f"], '
        'button:has-text("\u6392\u5e8f")'
    )
    entry_selectors = [
        'button[role="tab"]:has-text("\u8a55\u8ad6")',
        'button[jsaction*="moreReviews"]',
        'div[role="button"][jsaction*="moreReviews"]',
        '[jsaction*="reviewChart.moreReviews"]',
        '[jsaction*="rating.moreReviews"]',
        'button[aria-label*="\u5247\u8a55\u8ad6"]',
        'button[aria-label*="\u7bc7\u8a55\u8ad6"]',
        'button:has-text("\u5247\u8a55\u8ad6")',
        'button:has-text("\u7bc7\u8a55\u8ad6")',
        'div[role="button"]:has-text("\u5247\u8a55\u8ad6")',
        'div[role="button"]:has-text("\u7bc7\u8a55\u8ad6")',
    ]

    for attempt in range(24):
        sort_button = page.locator(sort_selector).first
        if await sort_button.count() > 0 and await sort_button.is_visible():
            return

        for selector in entry_selectors:
            entry = page.locator(selector).first
            if await entry.count() == 0 or not await entry.is_visible():
                continue

            label = (
                await entry.get_attribute("aria-label")
                or (await entry.inner_text()).strip()
                or selector
            )
            print(f"Opening reviews with: {label[:100]}")
            await entry.click(force=True)
            await page.wait_for_timeout(3000)

            sort_button = page.locator(sort_selector).first
            if (
                await sort_button.count() > 0
                and await sort_button.is_visible()
            ):
                return

        if attempt % 6 == 5:
            await detail_pane.evaluate("el => el.scrollTo(0, 0)")
        else:
            await detail_pane.evaluate(
                "(el, y) => el.scrollBy(0, y)",
                650,
            )
        await page.wait_for_timeout(700)

    visible_buttons = await page.locator("button:visible").all_inner_texts()
    summary = " | ".join(
        text.strip()
        for text in visible_buttons
        if text.strip()
    )[:500]
    summary = re.sub(r"[\ue000-\uf8ff]", "", summary)
    raise RuntimeError(
        "Could not open the reviews list. "
        f"Current URL: {page.url}. Visible buttons: {summary}"
    )


async def open_reviews_with_fallback(
    page: Page,
    restaurant_id: str,
    original_url: str,
):
    urls = list(
        dict.fromkeys(
            [
                google_maps_url_with_language(original_url),
                google_maps_place_id_url(restaurant_id),
            ]
        )
    )
    errors: list[str] = []

    for index, navigation_url in enumerate(urls, start=1):
        try:
            print(f"Opening restaurant page, route {index}/{len(urls)}.")
            await page.goto(
                navigation_url,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await page.wait_for_timeout(5000)
            await open_reviews_robust(page)
            return
        except Exception as exc:
            errors.append(str(exc))
            print(f"Review entry route {index} failed: {exc}")

    raise RuntimeError(
        "All review entry routes failed. " + " || ".join(errors)
    )


def looks_like_sorted_batch(rows: list[dict], sort_mode: str) -> bool:
    if not rows:
        return False

    if sort_mode in {"highest", "lowest"}:
        scores = [
            float(row["review_score"])
            for row in rows
            if isinstance(row.get("review_score"), (int, float))
        ]
        if len(scores) < 3:
            return bool(scores)

        if sort_mode == "highest":
            wrong_order = sum(
                left < right for left, right in zip(scores, scores[1:])
            )
            return (
                scores[0] == max(scores)
                and wrong_order <= max(1, len(scores) // 5)
            )

        wrong_order = sum(
            left > right for left, right in zip(scores, scores[1:])
        )
        return (
            scores[0] == min(scores)
            and wrong_order <= max(1, len(scores) // 5)
        )

    timestamps = [
        float(row["review_timestamp"])
        for row in rows
        if isinstance(row.get("review_timestamp"), (int, float))
    ]
    if len(timestamps) < 3:
        return bool(timestamps)

    wrong_order = sum(
        left < right for left, right in zip(timestamps, timestamps[1:])
    )
    return (
        timestamps[0] == max(timestamps)
        and wrong_order <= max(1, len(timestamps) // 5)
    )


def sort_review_rows(rows: list[dict], sort_mode: str):
    if sort_mode == "newest":
        rows.sort(
            key=lambda row: (
                float(row["review_timestamp"])
                if isinstance(row.get("review_timestamp"), (int, float))
                else float("-inf")
            ),
            reverse=True,
        )
    elif sort_mode == "highest":
        rows.sort(
            key=lambda row: (
                float(row["review_score"])
                if isinstance(row.get("review_score"), (int, float))
                else float("-inf")
            ),
            reverse=True,
        )
    else:
        rows.sort(key=lambda row: base_review_score(row))


def base_review_score(row: dict) -> float:
    score = row.get("review_score")
    return float(score) if isinstance(score, (int, float)) else float("inf")


class BatchReviewCollector:
    def __init__(self, restaurant_id: str, sort_mode: str):
        if sort_mode not in SORT_LABELS:
            raise ValueError(
                f"SORT_MODE must be one of: {', '.join(SORT_LABELS)}"
            )
        self.restaurant_id = restaurant_id
        self.sort_mode = sort_mode
        self.by_id: dict[str, dict[str, Any]] = {}
        self.collecting = False
        self.sort_confirmed = asyncio.Event()
        self.start_on_confirmation = False
        self.last_sorted_raw = ""
        self.raw_response_count = 0
        self.parsed_response_count = 0
        self.failed_response_count = 0
        self.http_error_count = 0
        self.next_progress_report = 100

    def prepare_sort_confirmation(self, start_collecting: bool = False):
        self.sort_confirmed.clear()
        self.start_on_confirmation = start_collecting

    def start_from_last_sorted_batch(self):
        if not self.last_sorted_raw:
            raise RuntimeError("No confirmed sorted batch is available.")
        self.collecting = True
        self.add_raw(self.last_sorted_raw)
        print(
            "Collection started from the previously confirmed "
            f"{self.sort_mode} batch."
        )

    def add_raw(self, raw: str) -> int:
        if SERVICE_NAME not in raw:
            return 0

        self.raw_response_count += 1
        rows = parse_batchexecute_text(
            raw,
            restaurant_id=self.restaurant_id,
        )
        if rows:
            self.parsed_response_count += 1

        added = 0
        for row in rows:
            review_id = row.get("review_id")
            if review_id and review_id not in self.by_id:
                self.by_id[review_id] = row
                added += 1

        while len(self.by_id) >= self.next_progress_report:
            print(
                f"progress: {self.next_progress_report} reviews, "
                f"service responses={self.raw_response_count}, "
                f"HTTP errors={self.http_error_count}, "
                f"read failures={self.failed_response_count}"
            )
            self.next_progress_report += 100
        return added

    async def handle_response(self, response: Response):
        if "batchexecute" not in response.url:
            return
        if response.status >= 400:
            self.http_error_count += 1

        raw = ""
        for attempt in range(1, 4):
            try:
                raw = await response.text()
                break
            except Exception as exc:
                if attempt == 3:
                    self.failed_response_count += 1
                    print(
                        "Failed to read batchexecute response "
                        f"after 3 attempts: {exc}"
                    )
                    return
                await asyncio.sleep(attempt)

        if SERVICE_NAME not in raw:
            return

        if not self.collecting:
            rows = parse_batchexecute_text(raw)
            if not looks_like_sorted_batch(rows, self.sort_mode):
                return

            self.last_sorted_raw = raw
            self.sort_confirmed.set()
            if not self.start_on_confirmation:
                return

            self.collecting = True
            print(
                f"Confirmed {self.sort_mode} results after clicking All. "
                "Collection started."
            )

        self.add_raw(raw)


def collect_reviews_with_requests_sync(
    template: ListUgcRequestTemplate,
    browser_cookies: list[dict[str, Any]],
    collector: BatchReviewCollector,
    max_reviews: int | None,
):
    """Use the captured browser request to fetch all remaining pages."""
    session = requests.Session()

    # Reuse browser headers, but let requests calculate transport headers.
    ignored_headers = {
        "content-length",
        "cookie",
        "host",
        "accept-encoding",
    }
    session.headers.update(
        {
            key: value
            for key, value in template.request_headers.items()
            if key.lower() not in ignored_headers
        }
    )
    session.headers["Content-Type"] = (
        "application/x-www-form-urlencoded;charset=UTF-8"
    )

    for cookie in browser_cookies:
        session.cookies.set(
            cookie["name"],
            cookie["value"],
            domain=cookie.get("domain"),
            path=cookie.get("path", "/"),
        )

    page_token = ""
    seen_tokens: set[str] = set()
    page_number = 1

    while True:
        if max_reviews is not None and len(collector.by_id) >= max_reviews:
            print(f"Reached max_reviews={max_reviews}.")
            break

        response = None
        for attempt in range(1, 4):
            try:
                response = session.post(
                    template.request_url,
                    data={"f.req": template.build_f_req(page_token)},
                    timeout=30,
                )
                if response.status_code < 500 and response.status_code != 429:
                    break
            except requests.RequestException as exc:
                if attempt == 3:
                    raise RuntimeError(
                        "ListUgcPosts request failed after 3 attempts."
                    ) from exc
            if attempt < 3:
                delay = attempt * 2
                print(
                    f"Page {page_number} request retry "
                    f"{attempt}/3 after {delay}s."
                )
                time.sleep(delay)

        if response is None:
            raise RuntimeError("ListUgcPosts returned no response.")
        if response.status_code >= 400:
            collector.http_error_count += 1
            raise RuntimeError(
                "ListUgcPosts HTTP error: "
                f"{response.status_code}, body={response.text[:300]}"
            )

        raw = response.text
        collector.add_raw(raw)
        next_token = next_page_token_from_response(raw)

        if not next_token:
            if not collector.by_id:
                raise RuntimeError(
                    "ListUgcPosts returned no review data. "
                    "The captured session may have expired."
                )
            break
        if next_token in seen_tokens:
            print("Repeated next token detected. Finished.")
            break

        seen_tokens.add(next_token)
        page_token = next_token
        page_number += 1

        time.sleep(random.uniform(0.8, 1.5))


async def click_sort_or_fail(
    page: Page,
    collector,
    sort_mode: str,
):
    label = SORT_LABELS.get(sort_mode)
    if label is None:
        raise ValueError(f"Unsupported sort mode: {sort_mode}")

    for attempt in range(1, 4):
        try:
            try:
                await page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:
                pass
            await page.wait_for_timeout(1000)

            sort_button = page.locator(
                'button[aria-label*="\u6392\u5e8f"], '
                'button:has-text("\u6392\u5e8f")'
            ).first
            await sort_button.wait_for(timeout=7000)
            await sort_button.click()
            await page.wait_for_timeout(800)

            option = page.locator(
                'div[role="menuitemradio"], '
                'div[role="menuitem"], '
                'div[role="option"], '
                'button'
            ).filter(has_text=label).first
            await option.wait_for(state="visible", timeout=7000)

            collector.prepare_sort_confirmation()
            await option.click()
            await asyncio.wait_for(
                collector.sort_confirmed.wait(),
                timeout=30,
            )
            print(f"Confirmed sorting mode: {sort_mode} ({label}).")
            return
        except Exception as exc:
            print(f"Sort attempt {attempt} for {sort_mode} failed: {exc}")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(1200)

    raise RuntimeError(
        f"Could not confirm sorting mode {sort_mode}. "
        "Stopped to avoid scraping the wrong order."
    )


async def click_all_reviews_or_fail(
    page: Page,
    collector: BatchReviewCollector,
):
    for attempt in range(1, 4):
        try:
            all_reviews = page.get_by_text(
                "\u5168\u90e8",
                exact=True,
            ).first
            await all_reviews.wait_for(state="visible", timeout=7000)

            collector.prepare_sort_confirmation(start_collecting=True)
            await all_reviews.click()
            try:
                await asyncio.wait_for(
                    collector.sort_confirmed.wait(),
                    timeout=8,
                )
            except asyncio.TimeoutError:
                collector.start_from_last_sorted_batch()
            return
        except Exception as exc:
            print(f"Click-All attempt {attempt} failed: {exc}")
            await page.wait_for_timeout(1200)

    raise RuntimeError(
        "Could not confirm sorted results after clicking All."
    )


async def find_scroll_pane(page: Page):
    selectors = [
        ".m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde",
        'div[role="main"] .m6QErb.DxyBCb.kA9KIf',
        'div[aria-label*="\u8a55\u8ad6"]',
    ]
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            await locator.wait_for(timeout=5000)
            return locator
        except Exception:
            pass
    raise RuntimeError("Could not find the review scroll pane.")


async def scroll_to_collect_all(
    page: Page,
    scroll_pane,
    collector: BatchReviewCollector,
    max_reviews: int | None,
):
    no_growth_rounds = 0
    last_count = len(collector.by_id)
    last_height = await scroll_pane.evaluate("el => el.scrollHeight")
    final_check_started = False

    while True:
        if max_reviews is not None and len(collector.by_id) >= max_reviews:
            print(f"Reached max_reviews={max_reviews}.")
            break

        await scroll_pane.evaluate(
            "el => el.scrollTo(0, el.scrollHeight)"
        )
        await page.wait_for_timeout(random.randint(2500, 4000))

        for _ in range(random.randint(7, 10)):
            await scroll_pane.evaluate(
                "(el, y) => el.scrollBy(0, y)",
                random.randint(450, 950),
            )
            await page.wait_for_timeout(random.randint(800, 1500))

        await page.wait_for_timeout(3000)
        current_count = len(collector.by_id)
        current_height = await scroll_pane.evaluate(
            "el => el.scrollHeight"
        )

        if current_count == last_count and current_height == last_height:
            no_growth_rounds += 1
            if no_growth_rounds == 12 and not final_check_started:
                final_check_started = True
                print(
                    "No growth for 12 rounds. Waiting 30 seconds "
                    "for delayed responses."
                )
                await page.wait_for_timeout(30000)
                await scroll_pane.evaluate(
                    "el => el.scrollTo(0, el.scrollHeight)"
                )
                await page.wait_for_timeout(5000)

                delayed_count = len(collector.by_id)
                delayed_height = await scroll_pane.evaluate(
                    "el => el.scrollHeight"
                )
                if (
                    delayed_count != current_count
                    or delayed_height != current_height
                ):
                    no_growth_rounds = 0
                    final_check_started = False
                    current_count = delayed_count
                    current_height = delayed_height

            if no_growth_rounds >= 20:
                print("No new reviews for 20 rounds. Finished.")
                break
        else:
            no_growth_rounds = 0
            final_check_started = False

        last_count = current_count
        last_height = current_height


async def scrape_reviews_on_gcp(
    restaurant_id: str,
    url: str,
    output_path: Path | None = None,
):

    output_path = output_path or Path(
        os.getenv("OUTPUT_PATH", f"/tmp/reviews_{restaurant_id}.csv")
    )
    session_dir = Path(
        os.getenv("SESSION_DIR", "/tmp/playwright_google_session")
    )
    debug_dir = Path(os.getenv("DEBUG_DIR", str(DEFAULT_DEBUG_DIR)))
    max_reviews = env_optional_int("MAX_REVIEWS")
    headless = env_bool("HEADLESS", DEFAULT_HEADLESS)
    sort_mode = os.getenv("SORT_MODE", "lowest").strip().lower()
    if sort_mode not in SORT_LABELS:
        raise ValueError(
            f"SORT_MODE must be one of: {', '.join(SORT_LABELS)}"
        )

    # Google's score-sorted cursors stop after roughly 360 reviews per star
    # bucket. Fetch chronologically to reach the full token chain, then sort
    # the completed rows locally into the requested score order.
    request_sort_mode = (
        "newest"
        if sort_mode in {"lowest", "highest"}
        else sort_mode
    )
    request_max_reviews = (
        max_reviews if request_sort_mode == sort_mode else None
    )

    if not headless and os.name != "nt" and not os.getenv("DISPLAY"):
        raise RuntimeError(
            "No DISPLAY was found. Run this crawler with: "
            "xvfb-run -a python 260618crawl_pw_batchexecute_to_csv_gcp.py"
        )

    chromium_args = [
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--window-size=1920,1080",
    ]
    no_sandbox = env_bool("CHROMIUM_NO_SANDBOX", DEFAULT_NO_SANDBOX)
    if no_sandbox:
        chromium_args.extend(["--no-sandbox", "--disable-setuid-sandbox"])

    launch_options = {
        "user_data_dir": str(session_dir),
        # True headless currently receives a restricted Google Maps page with
        # no existing-reviews tab. Use headed Chromium inside Xvfb on GCP.
        "headless": headless,
        "args": chromium_args,
        "locale": "zh-TW",
        "timezone_id": "Asia/Taipei",
        "viewport": {"width": 1920, "height": 1080},
    }
    executable_path = os.getenv("CHROMIUM_EXECUTABLE_PATH", "").strip()
    if executable_path:
        launch_options["executable_path"] = executable_path

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch_persistent_context(
            **launch_options
        )
        page = browser.pages[0] if browser.pages else await browser.new_page()
        await Stealth().apply_stealth_async(browser)

        collector = BatchReviewCollector(
            restaurant_id=restaurant_id,
            sort_mode=request_sort_mode,
        )
        request_capture = ListUgcRequestCapture(
            SORT_TYPES[request_sort_mode]
        )
        pending_tasks: set[asyncio.Task] = set()

        def schedule_response_parse(response):
            task = asyncio.create_task(collector.handle_response(response))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        try:
            mode = "headless" if headless else "headed/Xvfb"
            sandbox_mode = "no-sandbox" if no_sandbox else "sandboxed"
            print(
                f"Opening Google Maps in GCP {mode}, "
                f"{sandbox_mode} mode."
            )
            if request_sort_mode != sort_mode:
                print(
                    f"Fetching API pages with {request_sort_mode} order "
                    f"to avoid Google's score-sort cursor limit; "
                    f"CSV will be sorted as {sort_mode} after collection."
                )
            await open_reviews_with_fallback(
                page,
                restaurant_id=restaurant_id,
                original_url=url,
            )

            page.on("response", schedule_response_parse)
            page.on("request", request_capture.handle_request)
            await click_sort_or_fail(
                page,
                collector,
                request_sort_mode,
            )
            await asyncio.wait_for(
                request_capture.captured.wait(),
                timeout=10,
            )

            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)

            if request_capture.template is None:
                raise RuntimeError(
                    "Could not capture the sorted ListUgcPosts request."
                )

            browser_cookies = await browser.cookies()
            print(
                "Captured a valid browser request. "
                "Switching to requests pagination."
            )
            await asyncio.to_thread(
                collect_reviews_with_requests_sync,
                request_capture.template,
                browser_cookies,
                collector,
                request_max_reviews,
            )

            rows = list(collector.by_id.values())
            sort_review_rows(rows, sort_mode)
            if max_reviews is not None:
                rows = rows[:max_reviews]

            output_path.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = [
                "review_id",
                "restaurant_id",
                "review_score",
                "review_content",
                "food_score",
                "service_score",
                "atmosphere_score",
            ]
            with output_path.open(
                "w",
                encoding="utf-8-sig",
                newline="",
            ) as csv_file:
                writer = csv.DictWriter(
                    csv_file,
                    fieldnames=fieldnames,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(rows)

            print(
                "Collection statistics: "
                f"service responses={collector.raw_response_count}, "
                f"parsed responses={collector.parsed_response_count}, "
                f"HTTP errors={collector.http_error_count}, "
                f"read failures={collector.failed_response_count}, "
                f"unique reviews={len(collector.by_id)}"
            )
            print(f"Wrote {len(rows)} reviews to {output_path}")
        except Exception as exc:
            print(f"GCP crawler failed: {exc}")
            await save_debug_files(page, debug_dir, "crawler_failed")
            raise
        finally:
            await browser.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Standalone Google Maps review crawler for GCP."
    )
    parser.add_argument(
        "--restaurant-id",
        default=os.getenv("RESTAURANT_ID", RESTAURANT_ID).strip(),
        help="Google Maps place ID. Can also use RESTAURANT_ID.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("GOOGLE_MAPS_URL", GOOGLE_MAPS_URL).strip(),
        help="Google Maps restaurant URL. Can also use GOOGLE_MAPS_URL.",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_PATH", OUTPUT_PATH).strip(),
        help="CSV output path. Defaults to /tmp/reviews_<ID>.csv.",
    )
    args = parser.parse_args()

    if not args.restaurant_id:
        parser.error(
            "--restaurant-id is required "
            "(or set RESTAURANT_ID)."
        )
    if not args.url:
        parser.error("--url is required (or set GOOGLE_MAPS_URL).")
    if not args.url.startswith(("http://", "https://")):
        parser.error("--url must start with http:// or https://.")
    return args


async def main():
    args = parse_args()
    output_path = Path(args.output) if args.output else None
    await scrape_reviews_on_gcp(
        restaurant_id=args.restaurant_id,
        url=args.url,
        output_path=output_path,
    )


if __name__ == "__main__":
    asyncio.run(main())
