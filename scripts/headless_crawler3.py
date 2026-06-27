import asyncio
import csv
import json
import random
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from playwright.async_api import Page, Response, async_playwright
from playwright_stealth import Stealth


SERVICE_NAME = "MapsUgcPostService.ListUgcPosts"

DEFAULT_RESTAURANT_ID = "ChIJ4WS_CUarQjQR5p36XIydHlY"
DEFAULT_URL = "https://www.google.com/maps/search/?api=1&query=%E9%BA%B5%E6%87%B8%E4%B8%80%E7%94%9F%20-%20%E8%81%B7%E4%BA%BA%E9%BA%B5%E9%A4%A8&query_place_id=ChIJ4WS_CUarQjQR5p36XIydHlY"
DEFAULT_OUTPUT = f"reviews_{DEFAULT_RESTAURANT_ID}.csv"

DEFAULT_MAX_REVIEWS = 2200
DEFAULT_HEADLESS = False

async def main():
    print("[START] 啟動 Google Maps 評論爬蟲工作 (JSON 檔案驅動)")
    
    # 讀取 JSON 檔案
    restaurant_list = load_restaurant_json()
    print(f"[INFO] 成功載入 JSON 檔案，共計 {len(restaurant_list)} 筆餐廳資料")

    # 巡迴 JSON 內的所有餐廳資料
    for idx, item in enumerate(restaurant_list, start=1):
        # 取得對應欄位
        restaurant_id = item.get("restaurant_id")
        google_map_url = item.get("google_map_url")
        
        if not restaurant_id or not google_map_url:
            print(f"[WARNING] 第 {idx} 筆資料欄位缺失 (id: {restaurant_id})，跳過。")
            continue
        
        # 變更輸出路徑：多存放在一層 csv 資料夾底下 (csv/round2/reviews_{restaurant_id}.csv)
        output_path = Path(__file__).parent / "csv" / "round2" / f"reviews_{restaurant_id}.csv"
        
        print("\n" + "="*50)
        print(f"[{idx}/{len(restaurant_list)}] 開始處理餐廳 ID: {restaurant_id}")
        print(f"[PROCESS] URL: {google_map_url}")
        print(f"[PROCESS] 輸出路徑: {output_path}")
        print("="*50)

        try:
            await scrape_reviews_to_csv(
                restaurant_id=restaurant_id,
                url=google_map_url,
                output_path=output_path
            )
            # 餐廳與餐廳之間加入短暫隨機冷卻
            await asyncio.sleep(random.randint(5, 10))
        except Exception as e:
            print(f"[ERROR] 餐廳 {restaurant_id} 爬取失敗，跳過並繼續下一間。錯誤訊息: {e}", file=sys.stderr)

    print("\n[SUCCESS] JSON 名單所有餐廳處理完畢！")

def load_restaurant_json() -> list[dict[str, str]]:
    """讀取同層目錄下的 JSON 檔案並傳回餐廳清單"""
    json_path = Path(__file__).parent /"url_info_600_round2.json"
    
    if not json_path.exists():
        print(f"[CRITICAL ERROR] 找不到 JSON 檔案：{json_path}", file=sys.stderr)
        sys.exit(1)
        
    try:
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            
        # 兼容 JSON 最外層是物件還是陣列的狀況
        if isinstance(data, dict):
            # 如果最外層是物件且含有某個 list 鍵，可以微調。這裡假設是標準的陣列。
            return [data]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError("JSON 格式不符合預期的 List 或 Dict 格式")
            
    except Exception as e:
        print(f"[CRITICAL ERROR] 解析 JSON 檔案失敗: {e}", file=sys.stderr)
        sys.exit(1)

def walk_json(node: Any):
    yield node
    if isinstance(node, list):
        for item in node:
            yield from walk_json(item)
    elif isinstance(node, dict):
        for item in node.values():
            yield from walk_json(item)


def google_maps_url_with_language(url: str, language: str = "zh-TW") -> str:
    parsed = urlparse(url)
    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key != "hl"
    ]
    query_pairs.append(("hl", language))
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))


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
        "餐點",
        "服務",
        "氣氛",
        "餐點類型",
        "訂單類型",
        "平均每人消費金額",
        "建議的餐點",
        "用餐人數",
        "由 Google 翻譯",
        "查看原文",
        "顯示原文",
    }:
        return False
    return True


def looks_like_chinese_text(text: str) -> bool:
    if not re.search(r"[\u4e00-\u9fff]", text):
        return False
    if re.search(r"[\u3040-\u30ff\uac00-\ud7af]", text):
        return False
    return True


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
        and all(isinstance(x, int) for x in node[2][:2])
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

    language_block_candidates: list[str] = []

    for idx, item in enumerate(node):
        if not (
            isinstance(item, list)
            and len(item) == 1
            and isinstance(item[0], str)
            and re.fullmatch(r"[a-z]{2}(?:-[A-Za-z]+)?", item[0])
        ):
            continue

        maybe_texts = node[idx + 1] if idx + 1 < len(node) else None
        if not isinstance(maybe_texts, list):
            continue

        for maybe_text in maybe_texts:
            text = extract_text_tuple(maybe_text)
            if text:
                language_block_candidates.append(text)

    all_candidates = language_block_candidates + [
        text
        for text in collect_review_text_candidates(node)
        if text not in language_block_candidates
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
    if not isinstance(wrapper, list) or not wrapper or not isinstance(wrapper[0], list):
        return None

    review = wrapper[0]

    if len(review) < 3:
        return None

    review_id = review[0]
    meta = review[1] if isinstance(review[1], list) else []
    content = review[2] if isinstance(review[2], list) else []

    created_timestamp_raw = meta[2] if len(meta) > 2 else None
    updated_timestamp_raw = meta[3] if len(meta) > 3 else None

    timestamps = [
        timestamp
        for timestamp in [created_timestamp_raw, updated_timestamp_raw]
        if isinstance(timestamp, int)
    ]

    return {
        "review_id": review_id,
        "restaurant_id": restaurant_id,
        "_sort_timestamp": max(timestamps) if timestamps else 0,
        "review_score": content[0][0]
        if content and isinstance(content[0], list) and content[0]
        else None,
        "review_content": first_text(content),
        "food_score": aspect_rating(content, "GUIDED_DINING_FOOD_ASPECT"),
        "service_score": aspect_rating(content, "GUIDED_DINING_SERVICE_ASPECT"),
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
        review_wrappers = payload[2] if isinstance(payload, list) and len(payload) > 2 else []

        if not isinstance(review_wrappers, list):
            continue

        for wrapper in review_wrappers:
            row = parse_review_wrapper(wrapper, restaurant_id=restaurant_id)
            if row:
                rows.append(row)

    return rows


def review_sort_timestamp(row: dict[str, Any]) -> int:
    sort_timestamp = row.get("_sort_timestamp")
    return sort_timestamp if isinstance(sort_timestamp, int) else 0


def review_sort_score(row: dict[str, Any]) -> float:
    score = row.get("review_score")
    return float(score) if isinstance(score, (int, float)) else float("inf")


class BatchReviewCollector:
    def __init__(self, restaurant_id: str):
        self.restaurant_id = restaurant_id
        self.by_id: dict[str, dict[str, Any]] = {}
        self.raw_response_count = 0
        self.parsed_response_count = 0
        self.batchexecute_count = 0
        self.next_progress_report = 100

    def add_raw(self, raw: str):
        if SERVICE_NAME not in raw:
            return

        self.raw_response_count += 1
        rows = parse_batchexecute_text(raw, restaurant_id=self.restaurant_id)

        if rows:
            self.parsed_response_count += 1

        for row in rows:
            review_id = row.get("review_id")
            if review_id and review_id not in self.by_id:
                self.by_id[review_id] = row

        if len(self.by_id) >= self.next_progress_report:
            print(f"[INFO] 已解析並累積 {len(self.by_id)} 筆不重複評論")
            self.next_progress_report = (len(self.by_id) // 100 + 1) * 100

    async def handle_response(self, response: Response):
        if "batchexecute" not in response.url:
            return

        self.batchexecute_count += 1

        try:
            raw = await response.text()
        except Exception:
            return

        self.add_raw(raw)


async def click_reviews_tab(page: Page):
    print("[INFO] 正在尋找並點擊評論標籤...")

    selectors = [
        'button:has-text("評論")',
        'div[role="tab"]:has-text("評論")',
        'button[aria-label*="評論"]',
        'button:has-text("Reviews")',
        'div[role="tab"]:has-text("Reviews")',
    ]

    for sel in selectors:
        locator = page.locator(sel).first

        try:
            await locator.wait_for(state="visible", timeout=5000)
            await locator.click()
            print(f"[INFO] 成功點擊評論標籤: {sel}")
            await page.wait_for_timeout(3000)
            return
        except Exception:
            pass

    print("[WARNING] 未找到評論標籤，嘗試繼續")


async def click_side_and_reload(page: Page):
    print("[INFO] 執行側邊點擊與重整網頁以觸發 UI...")

    try:
        viewport = page.viewport_size or {"width": 1280, "height": 1080}
        x = int(viewport["width"] * 0.85)
        y = int(viewport["height"] * 0.45)

        await page.mouse.click(x, y)
        await page.wait_for_timeout(1000)

        await page.reload(wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)

    except Exception as e:
        print(f"[WARNING] 側邊點擊或重新整理失敗，繼續執行: {e}")


async def click_sort_lowest_rating_or_continue(page: Page):
    print("[INFO] 嘗試切換評論排序為最低評分...")

    sort_selectors = [
        'button[aria-label*="排序"]',
        'button[aria-label*="Sort"]',
        'button:has-text("排序")',
        'button:has-text("Sort")',
    ]

    lowest_selectors = [
        'div[role="menuitemradio"]:has-text("最低")',
        'div[role="menuitem"]:has-text("最低")',
        'div[role="option"]:has-text("最低")',
        'span:has-text("最低")',
        'div[role="menuitemradio"]:has-text("Lowest")',
        'div[role="menuitem"]:has-text("Lowest")',
        'div[role="option"]:has-text("Lowest")',
        'span:has-text("Lowest")',
    ]

    for attempt in range(1, 4):
        try:
            sort_button = None

            for sel in sort_selectors:
                locator = page.locator(sel).first
                try:
                    await locator.wait_for(state="visible", timeout=3000)
                    sort_button = locator
                    print(f"[INFO] 找到排序按鈕: {sel}")
                    break
                except Exception:
                    pass

            if sort_button is None:
                raise RuntimeError("找不到排序按鈕")

            await sort_button.click()
            await page.wait_for_timeout(1200)

            lowest_option = None

            for sel in lowest_selectors:
                locator = page.locator(sel).first
                try:
                    await locator.wait_for(state="visible", timeout=3000)
                    lowest_option = locator
                    print(f"[INFO] 找到最低評分選項: {sel}")
                    break
                except Exception:
                    pass

            if lowest_option is None:
                raise RuntimeError("找不到最低評分選項")

            await lowest_option.click()
            await page.wait_for_timeout(4000)

            print("[INFO] 已嘗試切換為最低評分排序")
            return

        except Exception as exc:
            print(f"[WARNING] 第 {attempt} 次切換最低排序失敗: {exc}")
            try:
                await page.keyboard.press("Escape")
            except Exception:
                pass
            await page.wait_for_timeout(1500)

    print("[WARNING] 無法切換最低排序，將以目前排序繼續抓取")


async def find_scroll_pane(page: Page):
    print("[INFO] 正在尋找真正的評論滾動容器...")

    selectors = [
        'div[role="feed"]',
        'div[aria-label*="評論"]',
        'div[aria-label*="Reviews"]',
        'div[role="main"] div',
        ".m6QErb.DxyBCb.kA9KIf.dS8AEf.XiKgde",
        "div.m6QErb.DxyBCb.kA9KIf.dS8AEf",
    ]

    best_locator = None
    best_score = -1
    best_info = ""

    for selector in selectors:
        locators = page.locator(selector)
        count = await locators.count()

        for i in range(min(count, 80)):
            locator = locators.nth(i)

            try:
                scroll_height = await locator.evaluate("el => el.scrollHeight")
                client_height = await locator.evaluate("el => el.clientHeight")
                scroll_top = await locator.evaluate("el => el.scrollTop")
                box = await locator.bounding_box()

                if not box:
                    continue

                if scroll_height <= client_height:
                    continue

                if client_height < 250:
                    continue

                score = scroll_height - client_height + client_height

                if score > best_score:
                    best_score = score
                    best_locator = locator
                    best_info = (
                        f"{selector} #{i}, "
                        f"scrollHeight={scroll_height}, "
                        f"clientHeight={client_height}, "
                        f"scrollTop={scroll_top}"
                    )

            except Exception:
                pass

    if best_locator is None:
        raise RuntimeError("找不到真正的評論滾動容器")

    print(f"[INFO] 使用滾動容器: {best_info}")
    return best_locator


async def scroll_to_collect_all(
    page: Page,
    scroll_pane,
    collector: BatchReviewCollector,
    max_reviews: int | None,
):
    no_growth_rounds = 0
    last_count = len(collector.by_id)
    last_height = await scroll_pane.evaluate("el => el.scrollHeight")

    print(f"[INFO] 開始滾動自動載入，目標上限: {max_reviews} 筆")

    while True:
        if max_reviews is not None and len(collector.by_id) >= max_reviews:
            print(f"[INFO] 已達目標數量: {len(collector.by_id)}")
            break

        await scroll_pane.evaluate("el => el.scrollTo(0, el.scrollHeight)")
        await page.wait_for_timeout(random.randint(1800, 2800))

        for _ in range(random.randint(2, 4)):
            distance = random.randint(600, 1200)
            await scroll_pane.evaluate("(el, y) => el.scrollBy(0, y)", distance)
            await page.wait_for_timeout(random.randint(700, 1300))

        current_count = len(collector.by_id)
        current_height = await scroll_pane.evaluate("el => el.scrollHeight")
        current_top = await scroll_pane.evaluate("el => el.scrollTop")

        print(
            f"[LOG] scrollTop={current_top} | "
            f"scrollHeight={current_height} | "
            f"batchexecute={collector.batchexecute_count} | "
            f"ListUgcPosts={collector.raw_response_count} | "
            f"reviews={current_count}"
        )

        if current_count == last_count and current_height == last_height:
            no_growth_rounds += 1

            if no_growth_rounds >= 10:
                print("[INFO] 多輪無增長，停止滾動")
                break
        else:
            no_growth_rounds = 0

        last_count = current_count
        last_height = current_height


async def scrape_reviews_to_csv(
    restaurant_id: str = DEFAULT_RESTAURANT_ID,
    url: str = DEFAULT_URL,
    output_path: Path | None = None,
    max_reviews: int | None = DEFAULT_MAX_REVIEWS,
    headless: bool = DEFAULT_HEADLESS,
):
    output_path = output_path or Path(DEFAULT_OUTPUT)
    user_data_dir = Path.cwd() / "playwright_google_session"

    async with async_playwright() as p:
        print(f"[INFO] 啟動 Chromium 瀏覽器 Headless={headless}")
        
        # 偽裝成真實 Windows 桌面版 Chrome 的 User-Agent
        CHROME_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        browser = await p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            locale="zh-TW",
            timezone_id="Asia/Taipei",
            # 1. 強制指定網頁呈現解析度為寬螢幕桌面版 (1920x1080)
            viewport={"width": 1920, "height": 1080}, 
            user_agent=CHROME_USER_AGENT,
            args=[
                "--disable-gpu", 
                "--no-sandbox", 
                "--disable-dev-shm-usage",
                # 2. 強制實體瀏覽器視窗大小也是 1920x1080
                "--window-size=1920,1080" 
            ]
        )

        page = browser.pages[0] if browser.pages else await browser.new_page()

        stealth_obj = Stealth()
        await stealth_obj.apply_stealth_async(browser)

        collector = BatchReviewCollector(restaurant_id=restaurant_id)
        pending_tasks: set[asyncio.Task] = set()

        def schedule_response_parse(response: Response):
            task = asyncio.create_task(collector.handle_response(response))
            pending_tasks.add(task)
            task.add_done_callback(pending_tasks.discard)

        page.on("response", schedule_response_parse)

        navigation_url = google_maps_url_with_language(url)

        print(f"[INFO] 正在導向目標 URL: {navigation_url}")

        try:
            await page.goto(
                navigation_url,
                wait_until="domcontentloaded",
                timeout=30000,
            )
        except Exception as exc:
            print(f"[WARNING] 首頁載入超時，繼續執行: {exc}")

        await page.wait_for_timeout(5000)

        await click_side_and_reload(page)
        await click_reviews_tab(page)
        await click_sort_lowest_rating_or_continue(page)

        try:
            scroll_pane = await find_scroll_pane(page)
            await scroll_to_collect_all(
                page,
                scroll_pane,
                collector,
                max_reviews=max_reviews,
            )
        except Exception as scroll_err:
            print(f"[ERROR] 滾動流程發生異常: {scroll_err}")

        if pending_tasks:
            print("[INFO] 等待最後剩餘的網路封包解析...")
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        rows = list(collector.by_id.values())

        print(f"[INFO] batchexecute 封包數: {collector.batchexecute_count}")
        print(f"[INFO] ListUgcPosts 封包數: {collector.raw_response_count}")
        print(f"[INFO] 成功解析封包數: {collector.parsed_response_count}")
        print(f"[INFO] 不重複評論數: {len(rows)}")

        if len(rows) == 0:
            await browser.close()
            raise RuntimeError(
                "抓到 0 筆評論，判定任務失敗。"
                "可能原因：Google Maps UI 改版、被擋、未進入評論列表、或滾動容器不正確。"
            )

        rows.sort(key=lambda row: (review_sort_score(row), -review_sort_timestamp(row)))

        if max_reviews is not None:
            rows = rows[:max_reviews]

        for row in rows:
            row.pop("_sort_timestamp", None)

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

        with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

        print(f"[SUCCESS] 已將 {len(rows)} 筆評論寫入: {output_path}")

        await browser.close()


# async def main():
#     print("[START] 啟動 Google Maps 評論爬蟲工作")
#     print(f"[PARAM] Restaurant ID: {DEFAULT_RESTAURANT_ID}")
#     print(f"[PARAM] Output Destination: {DEFAULT_OUTPUT}")

#     try:
#         await scrape_reviews_to_csv()
#     except Exception as e:
#         print(f"[CRITICAL ERROR] 腳本執行中斷: {e}", file=sys.stderr)
#         sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())