import json
import os
import re
import sys
import time
import typing as t
from difflib import SequenceMatcher

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


NOTION_VERSION = "2022-06-28"
TITLE_PROPERTY = "Game Title"
HLTB_PROPERTY = "HLTB"
STATUS_PROPERTY = "Status"
SKIP_STATUS = "Upcoming"

# ---------------------------------------------------------------------------
# Direct HLTB API (replaces howlongtobeatpy which no longer sends the
# required x-hp-key / x-hp-val honeypot headers since HLTB updated its API.)
# ---------------------------------------------------------------------------

_HLTB_BASE = "https://howlongtobeat.com/"
_HLTB_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)
_hltb_session: t.Optional[requests.Session] = None
_hltb_search_path: t.Optional[str] = None  # e.g. "api/find"

_HLTB_FETCH_POST_RE = re.compile(
    r'fetch\s*\(\s*["\']\s*/api/([a-zA-Z0-9_/]+)[^"\']*(["\']),\s*\{[^}]*method:\s*["\']POST["\'][^}]*\}',
    re.DOTALL | re.IGNORECASE,
)


def _hltb_ensure_session() -> None:
    """Lazily create the requests session and discover the current search path."""
    global _hltb_session, _hltb_search_path
    if _hltb_session is not None and _hltb_search_path is not None:
        return
    session = requests.Session()
    session.headers.update({"User-Agent": _HLTB_UA, "referer": _HLTB_BASE})
    resp = session.get(_HLTB_BASE, timeout=30)
    if resp.status_code != 200:
        _hltb_session = session
        _hltb_search_path = "api/find"
        return
    soup = BeautifulSoup(resp.text, "html.parser")
    found_path: t.Optional[str] = None
    for script in soup.find_all("script", src=True):
        src: str = script["src"]
        sr = session.get(_HLTB_BASE + src.lstrip("/"), timeout=30)
        if sr.status_code != 200 or not sr.text:
            continue
        m = _HLTB_FETCH_POST_RE.search(sr.text)
        if m:
            suffix = m.group(1)
            base = suffix.split("/")[0]
            found_path = f"api/{base}"
            break
    _hltb_session = session
    _hltb_search_path = found_path or "api/find"


def _hltb_get_auth() -> t.Tuple[t.Optional[str], t.Optional[str], t.Optional[str]]:
    """Return (token, hpKey, hpVal) from the /init endpoint."""
    assert _hltb_session and _hltb_search_path
    t_ms = int(time.time() * 1000)
    resp = _hltb_session.get(
        _HLTB_BASE + _hltb_search_path + "/init",
        params={"t": t_ms},
        timeout=30,
    )
    if resp.status_code != 200:
        return None, None, None
    data = resp.json()
    return data.get("token"), data.get("hpKey"), data.get("hpVal")


def _hltb_score(query: str, game_name: t.Optional[str]) -> float:
    """SequenceMatcher similarity with number-mismatch penalty (mirrors the library)."""
    if not game_name:
        return 0.0
    sim = SequenceMatcher(None, query, game_name).ratio()
    numbers = [w for w in query.split() if w.isdigit()]
    if numbers:
        cleaned = re.sub(r"([^\s\w]|_)+", "", game_name)
        if not any(w.isdigit() and w in numbers for w in cleaned.split()):
            sim -= 0.1
    return sim


def _hltb_search(query: str) -> t.List[dict]:
    """
    Search HLTB for *query* and return a list of dicts with keys:
    game_name, similarity, all_styles, main_extra, main_story, completionist.
    Hours are already divided by 3600.
    """
    _hltb_ensure_session()
    assert _hltb_session and _hltb_search_path

    token, hp_key, hp_val = _hltb_get_auth()
    if not token:
        return []

    payload: dict = {
        "searchType": "games",
        "searchTerms": query.split(),
        "searchPage": 1,
        "size": 20,
        "searchOptions": {
            "games": {
                "userId": 0,
                "platform": "",
                "sortCategory": "popular",
                "rangeCategory": "main",
                "rangeTime": {"min": 0, "max": 0},
                "gameplay": {"perspective": "", "flow": "", "genre": "", "difficulty": ""},
                "rangeYear": {"max": "", "min": ""},
                "modifier": "",
            },
            "users": {"sortCategory": "postcount"},
            "lists": {"sortCategory": "follows"},
            "filter": "",
            "sort": 0,
            "randomizer": 0,
        },
        "useCache": True,
    }
    if hp_key:
        payload[hp_key] = hp_val  # honeypot field in body

    resp = _hltb_session.post(
        _HLTB_BASE + _hltb_search_path,
        headers={
            "Content-Type": "application/json",
            "x-auth-token": token,
            "x-hp-key": hp_key or "",
            "x-hp-val": hp_val or "",
        },
        data=json.dumps(payload),
        timeout=30,
    )
    if resp.status_code != 200:
        return []

    def _h(v: t.Optional[int]) -> t.Optional[float]:
        return round(v / 3600, 2) if v else None

    results = []
    for g in resp.json().get("data", []):
        name = g.get("game_name") or ""
        alias = g.get("game_alias") or ""
        sim = max(_hltb_score(query, name), _hltb_score(query, alias))
        results.append({
            "game_name": name,
            "similarity": sim,
            "all_styles": _h(g.get("comp_all")),
            "main_extra": _h(g.get("comp_plus")),
            "main_story": _h(g.get("comp_main")),
            "completionist": _h(g.get("comp_100")),
        })
    return results


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


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


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


def normalize_title(value: str) -> str:
    text = value.strip()
    text = text.replace("’", "'")
    text = text.replace("‘", "'")
    text = text.replace("“", '"')
    text = text.replace("”", '"')
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_hltb_queries(game_name: str) -> t.List[str]:
    normalized = normalize_title(game_name)
    queries: t.List[str] = [normalized]

    split_chars = [":", "-", "|"]
    for ch in split_chars:
        if ch in normalized:
            head = normalized.split(ch, 1)[0].strip()
            if head and head not in queries:
                queries.append(head)

    simple = re.sub(r"[^a-zA-Z0-9\s']", " ", normalized)
    simple = re.sub(r"\s+", " ", simple).strip()
    if simple and simple not in queries:
        queries.append(simple)

    return queries


def find_hltb_hours(game_name: str, min_similarity: float, debug: bool = False) -> t.Optional[float]:
    best_match: t.Optional[dict] = None

    for query in build_hltb_queries(game_name):
        results = _hltb_search(query)
        if not results:
            continue

        candidate = max(results, key=lambda x: x["similarity"])
        if best_match is None or candidate["similarity"] > best_match["similarity"]:
            best_match = candidate

    if best_match is None or best_match["similarity"] < min_similarity:
        if debug:
            print(f"[DEBUG] No HLTB match for: {game_name}")
        return None

    # Values are already in hours (_hltb_search divides by 3600).
    # Prefer overall HLTB aggregate, then main+sides, then main story, then completionist.
    duration_hours = None
    for label in ("all_styles", "main_extra", "main_story", "completionist"):
        candidate = best_match[label]
        if candidate and candidate > 0:
            duration_hours = candidate
            if debug:
                print(f"[DEBUG] Using {label}={candidate}h for '{game_name}'.")
            break

    if not duration_hours or duration_hours <= 0:
        if debug:
            print(f"[DEBUG] No valid duration for: {game_name}")
        return None

    if debug:
        print(
            f"[DEBUG] HLTB match for '{game_name}': '{best_match['game_name']}' "
            f"(similarity={best_match['similarity']:.2f}, hours={duration_hours})"
        )

    return round(duration_hours, 1)


def build_notion_properties_update(page: dict, hltb_hours: t.Optional[float]) -> dict:
    properties = page.get("properties", {})
    update: dict = {}

    hltb_prop = properties.get(HLTB_PROPERTY)
    if not hltb_prop or hltb_hours is None or property_has_value(hltb_prop):
        return update

    ptype = hltb_prop.get("type")
    if ptype == "number":
        update[HLTB_PROPERTY] = {"number": hltb_hours}
    elif ptype == "title":
        update[HLTB_PROPERTY] = {"title": [{"text": {"content": str(hltb_hours)}}]}
    elif ptype == "rich_text":
        update[HLTB_PROPERTY] = {"rich_text": [{"text": {"content": str(hltb_hours)}}]}

    return update


def get_hltb_property_state(page: dict) -> t.Tuple[str, bool, t.Optional[str]]:
    properties = page.get("properties", {})
    hltb_prop = properties.get(HLTB_PROPERTY)
    if not hltb_prop:
        return "missing", False, None

    ptype = hltb_prop.get("type")
    supported = ptype in {"number", "title", "rich_text"}
    has_value = property_has_value(hltb_prop) if supported else False
    return "ok" if supported else "unsupported", has_value, ptype


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
    except ConfigError as exc:
        print(f"[ERROR] {exc}")
        print("Copy .env.example to .env and fill in required values.")
        return 1

    debug = os.getenv("DEBUG", "false").strip().lower() == "true"
    overwrite_hltb = _env_bool("OVERWRITE_HLTB", default=False)
    min_similarity = _env_float("HLTB_MIN_SIMILARITY", default=0.45)

    notion_headers = build_notion_headers(notion_token)

    updated = 0
    skipped_no_title = 0
    skipped_upcoming = 0
    skipped_no_hltb = 0
    skipped_no_changes = 0
    skipped_hltb_missing_property = 0
    skipped_hltb_unsupported_type = 0
    skipped_hltb_already_set = 0
    failed = 0
    progress_interval = 10
    started_at = time.time()

    try:
        pages = get_all_database_pages(notion_headers, database_id)
        total_pages = len(pages)
        print(f"[INFO] Loaded {total_pages} pages from Notion.")
        print(f"[INFO] HLTB_MIN_SIMILARITY={min_similarity}, OVERWRITE_HLTB={overwrite_hltb}")

        for index, page in enumerate(pages, start=1):
            page_id = page.get("id", "<unknown>")
            title = extract_game_title(page)

            elapsed = time.time() - started_at
            processed_before = index - 1
            avg = (elapsed / processed_before) if processed_before else 0.0
            remaining = (total_pages - processed_before) * avg if processed_before else 0.0
            progress_prefix = f"[{index}/{total_pages}]"

            if not title:
                skipped_no_title += 1
                if debug:
                    print(f"{progress_prefix} [DEBUG] Skip (no {TITLE_PROPERTY}): {page_id}")
                elif index % progress_interval == 0:
                    print(
                        f"{progress_prefix} [PROGRESS] updated={updated} skipped={skipped_no_title + skipped_upcoming + skipped_no_hltb + skipped_no_changes} "
                        f"failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}"
                    )
                continue

            status = extract_status(page)
            if status == SKIP_STATUS:
                skipped_upcoming += 1
                if debug:
                    print(f"{progress_prefix} [DEBUG] Skip ({STATUS_PROPERTY}={status!r}): {title}")
                continue

            try:
                hltb_state, hltb_has_value, hltb_ptype = get_hltb_property_state(page)
                if hltb_state == "missing":
                    skipped_hltb_missing_property += 1
                    if debug:
                        print(f"{progress_prefix} [DEBUG] Skip (HLTB property missing): {title}")
                    continue
                if hltb_state == "unsupported":
                    skipped_hltb_unsupported_type += 1
                    print(f"{progress_prefix} [WARN] Skip (HLTB unsupported type '{hltb_ptype}'): {title}")
                    continue
                if hltb_has_value and not overwrite_hltb:
                    skipped_hltb_already_set += 1
                    if debug:
                        print(f"{progress_prefix} [DEBUG] Skip (HLTB already set): {title}")
                    continue

                hltb_hours = find_hltb_hours(title, min_similarity=min_similarity, debug=debug)
                if hltb_hours is None:
                    skipped_no_hltb += 1
                    print(f"{progress_prefix} [WARN] No HLTB match: {title}")
                    if index % progress_interval == 0:
                        print(
                            f"{progress_prefix} [PROGRESS] updated={updated} skipped={skipped_no_title + skipped_no_hltb + skipped_no_changes + skipped_hltb_missing_property + skipped_hltb_unsupported_type + skipped_hltb_already_set} "
                            f"failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}"
                        )
                    continue

                properties_update = build_notion_properties_update(page, hltb_hours)
                if not properties_update:
                    skipped_no_changes += 1
                    if debug:
                        print(f"{progress_prefix} [DEBUG] Skip (nothing to update): {title}")
                    elif index % progress_interval == 0:
                        print(
                            f"{progress_prefix} [PROGRESS] updated={updated} skipped={skipped_no_title + skipped_no_hltb + skipped_no_changes + skipped_hltb_missing_property + skipped_hltb_unsupported_type + skipped_hltb_already_set} "
                            f"failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}"
                        )
                    continue

                notion_update_page_properties(notion_headers, page_id, properties_update)
                updated += 1
                print(f"{progress_prefix} [OK] Updated HLTB: {title} -> {hltb_hours}h")

                if index % progress_interval == 0:
                    print(
                        f"{progress_prefix} [PROGRESS] updated={updated} skipped={skipped_no_title + skipped_no_hltb + skipped_no_changes + skipped_hltb_missing_property + skipped_hltb_unsupported_type + skipped_hltb_already_set} "
                        f"failed={failed} elapsed={format_duration(elapsed)} eta={format_duration(remaining)}"
                    )

                time.sleep(0.25)

            except requests.HTTPError as exc:
                failed += 1
                status = exc.response.status_code if exc.response is not None else "?"
                print(f"{progress_prefix} [ERROR] HTTP {status} for '{title}' ({page_id}): {exc}")
            except Exception as exc:
                failed += 1
                print(f"{progress_prefix} [ERROR] Failed for '{title}' ({page_id}): {exc}")

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"[FATAL] Notion query failed with HTTP {status}: {exc}")
        return 2

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Skipped (no title): {skipped_no_title}")
    print(f"Skipped (upcoming): {skipped_upcoming}")
    print(f"Skipped (no hltb): {skipped_no_hltb}")
    print(f"Skipped (hltb property missing): {skipped_hltb_missing_property}")
    print(f"Skipped (hltb unsupported type): {skipped_hltb_unsupported_type}")
    print(f"Skipped (hltb already set): {skipped_hltb_already_set}")
    print(f"Skipped (no changes): {skipped_no_changes}")
    print(f"Failed: {failed}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
