import os
import sys
import time
import typing as t

import requests
from dotenv import load_dotenv


NOTION_VERSION = "2022-06-28"
TITLE_PROPERTY = "Game Title"
METACRITIC_PROPERTY = "Metacritic Score"
STATUS_PROPERTY = "Status"
SKIP_STATUS = "Upcoming"
RAWG_BASE_URL = "https://api.rawg.io/api"


class ConfigError(Exception):
    pass


def _env(name: str, required: bool = True, default: t.Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value or ""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def build_notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def notion_query_database(notion_headers: dict, database_id: str, start_cursor: t.Optional[str] = None) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload: dict = {"page_size": 100}
    if start_cursor:
        payload["start_cursor"] = start_cursor
    resp = requests.post(url, headers=notion_headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def iter_database_pages(notion_headers: dict, database_id: str) -> t.Iterator[dict]:
    cursor: t.Optional[str] = None
    while True:
        payload = notion_query_database(notion_headers, database_id, start_cursor=cursor)
        for page in payload.get("results", []):
            yield page
        if not payload.get("has_more"):
            break
        cursor = payload.get("next_cursor")


def get_all_database_pages(notion_headers: dict, database_id: str) -> t.List[dict]:
    return list(iter_database_pages(notion_headers, database_id))


def format_duration(seconds: float) -> str:
    total = max(int(seconds), 0)
    mins, sec = divmod(total, 60)
    hrs, mins = divmod(mins, 60)
    if hrs:
        return f"{hrs}h {mins}m {sec}s"
    if mins:
        return f"{mins}m {sec}s"
    return f"{sec}s"


def extract_text_property(prop_value: dict) -> t.Optional[str]:
    ptype = prop_value.get("type")
    if ptype == "title":
        parts = prop_value.get("title", [])
        text = "".join((p.get("plain_text") or "") for p in parts).strip()
        return text or None
    if ptype == "rich_text":
        parts = prop_value.get("rich_text", [])
        text = "".join((p.get("plain_text") or "") for p in parts).strip()
        return text or None
    return None


def extract_game_title(page: dict) -> t.Optional[str]:
    properties = page.get("properties", {})
    title_prop = properties.get(TITLE_PROPERTY)
    if not title_prop:
        return None
    return extract_text_property(title_prop)


def extract_status(page: dict) -> t.Optional[str]:
    prop = page.get("properties", {}).get(STATUS_PROPERTY)
    if not prop:
        return None
    ptype = prop.get("type")
    if ptype == "status":
        status_obj = prop.get("status") or {}
        return status_obj.get("name")
    if ptype == "select":
        select_obj = prop.get("select") or {}
        return select_obj.get("name")
    return None


def property_has_value(prop_value: dict) -> bool:
    ptype = prop_value.get("type")
    if ptype == "number":
        return prop_value.get("number") is not None
    if ptype == "rich_text":
        return len(prop_value.get("rich_text", [])) > 0
    if ptype == "title":
        return len(prop_value.get("title", [])) > 0
    return False


def find_steam_app_id(game_name: str, debug: bool = False) -> t.Optional[int]:
    safe_name = requests.utils.quote(game_name)
    resp = requests.get(
        f"https://store.steampowered.com/api/storesearch/?term={safe_name}&l=english&cc=US",
        timeout=30,
    )
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items:
        if debug:
            print(f"[DEBUG] No Steam search result for: {game_name}")
        return None
    return items[0].get("id")


def find_metacritic_score_steam(game_name: str, debug: bool = False) -> t.Optional[int]:
    """Look up Metacritic score via Steam appdetails API. Returns None if not found."""
    app_id = find_steam_app_id(game_name, debug=debug)
    if not app_id:
        return None

    resp = requests.get(
        f"https://store.steampowered.com/api/appdetails?appids={app_id}",
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    app_data = data.get(str(app_id), {})
    if not app_data.get("success"):
        if debug:
            print(f"[DEBUG] Steam appdetails not successful for app {app_id} ({game_name})")
        return None

    mc = app_data.get("data", {}).get("metacritic")
    if not mc:
        if debug:
            print(f"[DEBUG] No Metacritic data in Steam for: {game_name}")
        return None

    score = mc.get("score")
    if score is None:
        return None

    if debug:
        print(f"[DEBUG] Steam Metacritic for '{game_name}': {score}")
    return int(score)


def find_metacritic_score(rawg_api_key: str, game_name: str, debug: bool = False) -> t.Optional[int]:
    """Search RAWG for a game and return its Metacritic score, or None if not found."""
    params = {
        "key": rawg_api_key,
        "search": game_name,
        "search_exact": False,
        "page_size": 5,
    }
    resp = requests.get(f"{RAWG_BASE_URL}/games", params=params, timeout=30)

    if resp.status_code == 401:
        raise RuntimeError("RAWG unauthorized. Check RAWG_API_KEY.")

    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])

    if not results:
        if debug:
            print(f"[DEBUG] No RAWG results for: {game_name}")
        return None

    # Prefer an exact name match; fall back to first result
    match = next(
        (r for r in results if r.get("name", "").lower() == game_name.lower()),
        results[0],
    )

    score = match.get("metacritic")
    if not score:
        if debug:
            print(f"[DEBUG] RAWG found '{match.get('name')}' but Metacritic score is null.")
        return None

    if debug:
        print(f"[DEBUG] RAWG matched '{match.get('name')}' -> Metacritic: {score}")

    return int(score)


def notion_update_page_properties(notion_headers: dict, page_id: str, properties_update: dict) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": properties_update}
    resp = requests.patch(url, headers=notion_headers, json=payload, timeout=30)
    resp.raise_for_status()


def main() -> int:
    load_dotenv()

    try:
        notion_token = _env("NOTION_TOKEN")
        database_id = _env("DATABASE_ID")
        rawg_api_key = _env("RAWG_API_KEY", required=False)
    except ConfigError as exc:
        print(f"[ERROR] {exc}")
        print("Get a free RAWG API key at https://rawg.io/apidocs and add RAWG_API_KEY to your .env file.")
        return 1

    debug = os.getenv("DEBUG", "false").strip().lower() == "true"
    overwrite = _env_bool("OVERWRITE_METACRITIC", default=False)

    notion_headers = build_notion_headers(notion_token)

    updated = 0
    skipped_no_title = 0
    skipped_upcoming = 0
    skipped_no_score = 0
    skipped_already_set = 0
    skipped_missing_property = 0
    skipped_unsupported_type = 0
    failed = 0
    progress_interval = 10
    started_at = time.time()

    print(f"[INFO] OVERWRITE_METACRITIC={overwrite}")
    if rawg_api_key:
        print("[INFO] RAWG fallback enabled.")
    else:
        print("[INFO] RAWG_API_KEY not set — Steam only (no fallback).")

    try:
        pages = get_all_database_pages(notion_headers, database_id)
        total_pages = len(pages)
        print(f"[INFO] Loaded {total_pages} pages from Notion.")

        for index, page in enumerate(pages, start=1):
            page_id = page.get("id", "<unknown>")
            title = extract_game_title(page)

            elapsed = time.time() - started_at
            processed_before = index - 1
            avg = (elapsed / processed_before) if processed_before else 0.0
            remaining = (total_pages - processed_before) * avg if processed_before else 0.0
            prefix = f"[{index}/{total_pages}]"

            if not title:
                skipped_no_title += 1
                if debug:
                    print(f"{prefix} [DEBUG] Skip (no {TITLE_PROPERTY}): {page_id}")
                continue

            status = extract_status(page)
            if status == SKIP_STATUS:
                skipped_upcoming += 1
                if debug:
                    print(f"{prefix} [DEBUG] Skip ({STATUS_PROPERTY}={status!r}): {title}")
                continue

            properties = page.get("properties", {})
            mc_prop = properties.get(METACRITIC_PROPERTY)

            if mc_prop is None:
                skipped_missing_property += 1
                if debug:
                    print(f"{prefix} [DEBUG] Skip (Metacritic property missing): {title}")
                continue

            ptype = mc_prop.get("type")
            if ptype not in {"number", "rich_text", "title"}:
                skipped_unsupported_type += 1
                print(f"{prefix} [WARN] Skip (unsupported type '{ptype}'): {title}")
                continue

            if property_has_value(mc_prop) and not overwrite:
                skipped_already_set += 1
                if debug:
                    print(f"{prefix} [DEBUG] Skip (already set): {title}")
                continue

            try:
                score = find_metacritic_score_steam(title, debug=debug)
                if score is None and rawg_api_key:
                    if debug:
                        print(f"[DEBUG] Steam had no score for '{title}', trying RAWG...")
                    score = find_metacritic_score(rawg_api_key, title, debug=debug)

                if score is None:
                    skipped_no_score += 1
                    print(f"{prefix} [WARN] No Metacritic score found: {title}")
                    if index % progress_interval == 0:
                        total_skipped = skipped_no_title + skipped_no_score + skipped_already_set + skipped_missing_property + skipped_unsupported_type
                        print(f"{prefix} [PROGRESS] updated={updated} skipped={total_skipped} failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}")
                    continue

                if ptype == "number":
                    prop_update = {"number": score}
                elif ptype == "title":
                    prop_update = {"title": [{"text": {"content": str(score)}}]}
                else:
                    prop_update = {"rich_text": [{"text": {"content": str(score)}}]}

                notion_update_page_properties(notion_headers, page_id, {METACRITIC_PROPERTY: prop_update})
                updated += 1
                print(f"{prefix} [OK] Updated Metacritic: {title} -> {score}")

                if index % progress_interval == 0:
                    total_skipped = skipped_no_title + skipped_no_score + skipped_already_set + skipped_missing_property + skipped_unsupported_type
                    print(f"{prefix} [PROGRESS] updated={updated} skipped={total_skipped} failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}")

                time.sleep(0.25)

            except requests.HTTPError as exc:
                failed += 1
                status = exc.response.status_code if exc.response is not None else "?"
                print(f"{prefix} [ERROR] HTTP {status} for '{title}' ({page_id}): {exc}")
            except Exception as exc:
                failed += 1
                print(f"{prefix} [ERROR] Failed for '{title}' ({page_id}): {exc}")

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"[FATAL] Notion query failed with HTTP {status}: {exc}")
        return 2

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Skipped (no title): {skipped_no_title}")
    print(f"Skipped (upcoming): {skipped_upcoming}")
    print(f"Skipped (no score found): {skipped_no_score}")
    print(f"Skipped (already set): {skipped_already_set}")
    print(f"Skipped (property missing): {skipped_missing_property}")
    print(f"Skipped (unsupported type): {skipped_unsupported_type}")
    print(f"Failed: {failed}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
