import datetime as dt
import os
import sys
import time
import typing as t

import requests
from dotenv import load_dotenv


NOTION_VERSION = "2022-06-28"
TITLE_PROPERTY = "Game Title"
RELEASE_DATE_PROPERTY = "Release Date"
DEVELOPER_PROPERTY = "Developer"


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


def iter_database_pages(notion_headers: dict, database_id: str) -> t.Iterator[dict]:
    cursor: t.Optional[str] = None
    while True:
        payload = notion_query_database(notion_headers, database_id, start_cursor=cursor)

        for page in payload.get("results", []):
            yield page

        if not payload.get("has_more"):
            break
        cursor = payload.get("next_cursor")


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


def property_has_value(prop_value: dict) -> bool:
    ptype = prop_value.get("type")

    if ptype == "date":
        return bool(prop_value.get("date") and prop_value.get("date", {}).get("start"))

    if ptype == "rich_text":
        return len(prop_value.get("rich_text", [])) > 0

    if ptype == "title":
        return len(prop_value.get("title", [])) > 0

    return False


def maybe_release_date(value: t.Optional[int]) -> t.Optional[str]:
    if value is None:
        return None
    try:
        return dt.datetime.utcfromtimestamp(value).date().isoformat()
    except Exception:
        return None


def find_igdb_game_data(igdb_headers: dict, game_name: str, debug: bool = False) -> t.Tuple[t.Optional[str], t.Optional[str]]:
    safe_name = game_name.replace('"', '\\"')
    game_query = (
        f'search "{safe_name}"; '
        "fields name,first_release_date,involved_companies.company.name,involved_companies.developer; "
        "limit 5;"
    )

    resp = requests.post("https://api.igdb.com/v4/games", headers=igdb_headers, data=game_query, timeout=30)

    if resp.status_code == 401:
        raise RuntimeError("IGDB unauthorized. Check IGDB_ACCESS_TOKEN and IGDB_CLIENT_ID.")

    resp.raise_for_status()
    results = resp.json()

    if not isinstance(results, list) or not results:
        if debug:
            print(f"[DEBUG] No IGDB match for: {game_name}")
        return None, None

    best = results[0]
    release_date = maybe_release_date(best.get("first_release_date"))

    developer_name = None
    involved = best.get("involved_companies") or []
    for comp in involved:
        if comp.get("developer"):
            company = comp.get("company") or {}
            name = company.get("name")
            if name:
                developer_name = name
                break

    if developer_name is None and involved:
        company = (involved[0].get("company") or {})
        developer_name = company.get("name")

    return release_date, developer_name


def build_notion_properties_update(page: dict, release_date: t.Optional[str], developer: t.Optional[str]) -> dict:
    properties = page.get("properties", {})
    update: dict = {}

    release_prop = properties.get(RELEASE_DATE_PROPERTY)
    if release_prop and release_prop.get("type") == "date" and release_date and not property_has_value(release_prop):
        update[RELEASE_DATE_PROPERTY] = {"date": {"start": release_date}}

    developer_prop = properties.get(DEVELOPER_PROPERTY)
    if developer_prop and developer_prop.get("type") in {"rich_text", "title"} and developer and not property_has_value(developer_prop):
        if developer_prop.get("type") == "title":
            update[DEVELOPER_PROPERTY] = {"title": [{"text": {"content": developer}}]}
        else:
            update[DEVELOPER_PROPERTY] = {"rich_text": [{"text": {"content": developer}}]}

    return update


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
        igdb_client_id = _env("IGDB_CLIENT_ID")
        igdb_access_token = _env("IGDB_ACCESS_TOKEN")
    except ConfigError as exc:
        print(f"[ERROR] {exc}")
        print("Copy .env.example to .env and fill in required values.")
        return 1

    debug = os.getenv("DEBUG", "false").strip().lower() == "true"

    notion_headers = build_notion_headers(notion_token)
    igdb_headers = build_igdb_headers(igdb_client_id, igdb_access_token)

    updated = 0
    skipped_no_title = 0
    skipped_no_changes = 0
    failed = 0

    try:
        for page in iter_database_pages(notion_headers, database_id):
            page_id = page.get("id", "<unknown>")
            title = extract_game_title(page)

            if not title:
                skipped_no_title += 1
                if debug:
                    print(f"[DEBUG] Skip (no {TITLE_PROPERTY}): {page_id}")
                continue

            try:
                release_date, developer = find_igdb_game_data(igdb_headers, title, debug=debug)
                properties_update = build_notion_properties_update(
                    page=page,
                    release_date=release_date,
                    developer=developer,
                )

                if not properties_update:
                    skipped_no_changes += 1
                    if debug:
                        print(f"[DEBUG] Skip (nothing to update): {title}")
                    continue

                notion_update_page_properties(notion_headers, page_id, properties_update)
                updated += 1
                print(f"[OK] Updated metadata: {title}")

                # Keep request cadence gentle for APIs.
                time.sleep(0.25)

            except requests.HTTPError as exc:
                failed += 1
                status = exc.response.status_code if exc.response is not None else "?"
                print(f"[ERROR] HTTP {status} for '{title}' ({page_id}): {exc}")
            except Exception as exc:
                failed += 1
                print(f"[ERROR] Failed for '{title}' ({page_id}): {exc}")

    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "?"
        print(f"[FATAL] Notion query failed with HTTP {status}: {exc}")
        return 2

    print("\nDone.")
    print(f"Updated: {updated}")
    print(f"Skipped (no title): {skipped_no_title}")
    print(f"Skipped (no changes): {skipped_no_changes}")
    print(f"Failed: {failed}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
