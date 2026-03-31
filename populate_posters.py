import argparse
import os
import sys
import time
import typing as t

import requests
from dotenv import load_dotenv


NOTION_VERSION = "2022-06-28"
IGDB_IMAGE_BASE = "https://images.igdb.com/igdb/image/upload"


class ConfigError(Exception):
    pass


def _env(name: str, required: bool = True, default: t.Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value or ""


def build_notion_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def build_igdb_headers(client_id: str, access_token: str) -> dict:
    return {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/plain",
    }


def notion_query_database(notion_headers: dict, database_id: str, start_cursor: t.Optional[str] = None) -> dict:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload: dict = {"page_size": 100}
    if start_cursor:
        payload["start_cursor"] = start_cursor
    resp = requests.post(url, headers=notion_headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def notion_update_cover_only(notion_headers: dict, page_id: str, cover_url: str) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "cover": {
            "type": "external",
            "external": {"url": cover_url},
        },
    }
    resp = requests.patch(url, headers=notion_headers, json=payload, timeout=30)
    resp.raise_for_status()


def notion_update_page(notion_headers: dict, page_id: str, cover_url: str, icon_url: str) -> None:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "cover": {
            "type": "external",
            "external": {"url": cover_url},
        },
        "icon": {
            "type": "external",
            "external": {"url": icon_url},
        },
    }
    resp = requests.patch(url, headers=notion_headers, json=payload, timeout=30)
    resp.raise_for_status()


def extract_title_from_property_value(prop_value: dict) -> t.Optional[str]:
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


def extract_page_title(page: dict, title_property_name: str = "Game Title") -> t.Optional[str]:
    properties = page.get("properties", {})
    prop_value = properties.get(title_property_name)
    if not prop_value:
        return None

    return extract_title_from_property_value(prop_value)


def page_already_has_cover(page: dict) -> bool:
    cover = page.get("cover")
    return bool(cover)


def find_steam_cover_url(game_name: str, debug: bool = False) -> t.Optional[str]:
    safe_name = requests.utils.quote(game_name)
    resp = requests.get(
        f"https://store.steampowered.com/api/storesearch/?term={safe_name}&l=english&cc=US",
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", [])
    if not items:
        if debug:
            print(f"[DEBUG] No Steam match for: {game_name}")
        return None

    app_id = items[0].get("id")
    if not app_id:
        return None

    # Use appdetails to get the authoritative header_image URL.
    # Newer games use a content-hash in their CDN path, so we can't construct it manually.
    detail_resp = requests.get(
        f"https://store.steampowered.com/api/appdetails?appids={app_id}",
        timeout=30,
    )
    detail_resp.raise_for_status()
    app_data = detail_resp.json().get(str(app_id), {})
    if not app_data.get("success"):
        if debug:
            print(f"[DEBUG] Steam appdetails not successful for app {app_id} ({game_name})")
        return None

    header_image = app_data.get("data", {}).get("header_image")
    if not header_image:
        if debug:
            print(f"[DEBUG] No header_image in Steam appdetails for: {game_name}")
        return None

    return header_image


def find_igdb_cover_url(igdb_headers: dict, game_name: str, debug: bool = False) -> t.Optional[str]:
    # Escape double quotes to keep IGDB query valid.
    safe_name = game_name.replace('"', '\\"')

    game_query = (
        f'search "{safe_name}"; '
        "fields name,cover.url,first_release_date; "
        "limit 5;"
    )

    resp = requests.post(
        "https://api.igdb.com/v4/games",
        headers=igdb_headers,
        data=game_query,
        timeout=30,
    )

    if resp.status_code == 401:
        raise RuntimeError("IGDB unauthorized. Check IGDB_ACCESS_TOKEN and IGDB_CLIENT_ID.")

    resp.raise_for_status()
    results = resp.json()

    if not isinstance(results, list) or not results:
        if debug:
            print(f"[DEBUG] No IGDB match for: {game_name}")
        return None

    for game in results:
        cover = game.get("cover")
        if not cover:
            continue

        cover_url = cover.get("url")
        if not cover_url:
            continue

        # Convert IGDB thumbnail URL to a bigger image.
        normalized = cover_url
        if normalized.startswith("//"):
            normalized = "https:" + normalized
        normalized = normalized.replace("t_thumb", "t_cover_big")

        if normalized.startswith("http://") or normalized.startswith("https://"):
            return normalized

        return f"{IGDB_IMAGE_BASE}/{normalized.lstrip('/')}"

    return None


def iter_database_pages(notion_headers: dict, database_id: str) -> t.Iterator[dict]:
    cursor: t.Optional[str] = None

    while True:
        payload = notion_query_database(notion_headers, database_id, start_cursor=cursor)

        for page in payload.get("results", []):
            yield page

        if not payload.get("has_more"):
            break

        cursor = payload.get("next_cursor")


def main() -> int:
    parser = argparse.ArgumentParser(description="Populate Notion game pages with poster art.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch and overwrite cover and icon even for pages that already have one.",
    )
    parser.add_argument(
        "--force-cover-only",
        action="store_true",
        help="Re-fetch and overwrite only the cover (Steam) for all pages, leaving the icon untouched.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=os.getenv("DEBUG", "false").strip().lower() == "true",
        help="Enable verbose debug output.",
    )
    args = parser.parse_args()

    load_dotenv()

    try:
        notion_token = _env("NOTION_TOKEN")
        database_id = _env("DATABASE_ID")
        igdb_client_id = _env("IGDB_CLIENT_ID")
        igdb_access_token = _env("IGDB_ACCESS_TOKEN")
    except ConfigError as exc:
        print(f"[ERROR] {exc}")
        print("Copy .env.example to .env and fill in required values.")
        return 1

    debug = args.debug

    notion_headers = build_notion_headers(notion_token)
    igdb_headers = build_igdb_headers(igdb_client_id, igdb_access_token)

    updated = 0
    skipped_has_cover = 0
    skipped_no_title = 0
    skipped_no_poster = 0
    failed = 0

    try:
        for page in iter_database_pages(notion_headers, database_id):
            page_id = page.get("id", "<unknown>")

            if page_already_has_cover(page) and not args.force and not args.force_cover_only:
                skipped_has_cover += 1
                if debug:
                    print(f"[DEBUG] Skip (already has cover): {page_id}")
                continue

            title = extract_page_title(page)
            if not title:
                skipped_no_title += 1
                if debug:
                    print(f"[DEBUG] Skip (no title): {page_id}")
                continue

            try:
                if args.force_cover_only:
                    steam_url = find_steam_cover_url(title, debug=debug)
                    if not steam_url:
                        skipped_no_poster += 1
                        print(f"[WARN] No Steam cover found: {title}")
                        continue
                    notion_update_cover_only(notion_headers, page_id, steam_url)
                else:
                    icon_url = find_igdb_cover_url(igdb_headers, title, debug=debug)
                    if not icon_url:
                        skipped_no_poster += 1
                        print(f"[WARN] No IGDB poster found: {title}")
                        continue

                    steam_url = find_steam_cover_url(title, debug=debug)
                    cover_url = steam_url if steam_url else icon_url
                    if not steam_url and debug:
                        print(f"[DEBUG] No Steam cover found for '{title}', falling back to IGDB")

                    notion_update_page(notion_headers, page_id, cover_url, icon_url)

                updated += 1
                print(f"[OK] Updated: {title}")

                # Be polite to APIs and avoid burst limits.
                time.sleep(0.2)

            except requests.HTTPError as exc:
                failed += 1
                status = exc.response.status_code if exc.response is not None else "?"
                print(f"[ERROR] HTTP {status} for page '{title}' ({page_id}): {exc}")
            except Exception as exc:
                failed += 1
                print(f"[ERROR] Failed for page '{title}' ({page_id}): {exc}")

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"[FATAL] Notion query failed with HTTP {status}: {exc}")
        return 2

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Skipped (already has cover): {skipped_has_cover}")
    print(f"Skipped (no title): {skipped_no_title}")
    print(f"Skipped (no poster found): {skipped_no_poster}")
    print(f"Failed: {failed}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
