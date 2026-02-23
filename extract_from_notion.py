import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")  # can be with or without dashes
NOTION_VERSION = os.getenv("NOTION_VERSION", "2025-09-03")

BASE_URL = "https://api.notion.com/v1"


def _require_env(name: str, value: Optional[str]) -> str:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def paginate_query(
        token: str,
        notion_version: str,
        path: str,
        base_body: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    body = dict(base_body or {})
    body["page_size"] = min(max(page_size, 1), 100)

    start_cursor: Optional[str] = None

    while True:
        if start_cursor:
            body["start_cursor"] = start_cursor
        else:
            body.pop("start_cursor", None)

        data = notion_request(
            token,
            notion_version,
            "POST",
            f"{BASE_URL}{path}",
            json_body=body,
        )

        results.extend(data.get("results", []))

        has_more = bool(data.get("has_more"))
        start_cursor = data.get("next_cursor")

        if not has_more:
            break
        if has_more and not start_cursor:
            raise RuntimeError("Notion returned has_more=true but next_cursor is missing.")

    return results


def get_database(token: str, notion_version: str, database_id: str) -> Dict[str, Any]:
    return notion_request(token, notion_version, "GET", f"{BASE_URL}/databases/{database_id}")


def extract_all_tasks(database_id: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    token = _require_env("NOTION_TOKEN", NOTION_TOKEN)
    notion_version = NOTION_VERSION

    try:
        results = paginate_query(token, notion_version, f"/databases/{database_id}/query", base_body={})
        meta = {"mode": "database_query", "count": len(results)}
        return results, meta
    except RuntimeError as e:
        first_error = str(e)

    db = get_database(token, notion_version, database_id)
    data_sources = db.get("data_sources") or []
    if not isinstance(data_sources, list) or not data_sources:
        raise RuntimeError(
            "Database query failed, and no data_sources were found on the database object.\n\n"
            f"Original database query error:\n{first_error}"
        )

    all_results: List[Dict[str, Any]] = []
    for ds in data_sources:
        ds_id = ds.get("id")
        if not ds_id:
            continue
        ds_results = paginate_query(token, notion_version, f"/data_sources/{ds_id}/query", base_body={})
        all_results.extend(ds_results)

    meta = {
        "mode": "data_source_query",
        "count": len(all_results),
        "data_sources": [ds.get("id") for ds in data_sources],
    }
    return all_results, meta


def main() -> None:
    token = _require_env("NOTION_TOKEN", NOTION_TOKEN)
    database_id = _require_env("NOTION_DATABASE_ID", NOTION_DATABASE_ID)
    notion_version = NOTION_VERSION

    tasks, meta = extract_all_tasks(database_id)

    normalized = normalize_tasks(tasks, token=token, notion_version=notion_version)

    with open("tasks_notion.json", "w", encoding="utf-8") as f:
        json.dump(normalized, f, ensure_ascii=False, indent=2)

    print(f"Extracted {meta['count']} tasks using {meta['mode']}.")
    print(f"Wrote {len(normalized)} normalized tasks to tasks_notion.json")


# -------------------------
# Notion HTTP helpers
# -------------------------

def notion_headers(token: str, notion_version: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": notion_version,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def notion_request(
        token: str,
        notion_version: str,
        method: str,
        url: str,
        *,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    resp = requests.request(
        method,
        url,
        headers=notion_headers(token, notion_version),
        json=json_body,
        params=params,
        timeout=60,
    )

    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text}
        raise RuntimeError(
            f"Notion API error {resp.status_code} for {method} {url}\n"
            f"{json.dumps(err, ensure_ascii=False, indent=2)}"
        )
    return resp.json()


def paginate_list_endpoint(
        token: str,
        notion_version: str,
        url: str,
        *,
        params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    base_params = dict(params or {})

    while True:
        call_params = dict(base_params)
        if cursor:
            call_params["start_cursor"] = cursor

        data = notion_request(token, notion_version, "GET", url, params=call_params)
        out.extend(data.get("results", []))

        if not data.get("has_more"):
            break

        cursor = data.get("next_cursor")
        if not cursor:
            raise RuntimeError("Pagination error: has_more=true but next_cursor missing.")

    return out


# -------------------------
# Rich text flattening
# -------------------------

def _plain_text(rich_text: Optional[List[Dict[str, Any]]]) -> str:
    if not rich_text:
        return ""
    return "".join((rt.get("plain_text") or "") for rt in rich_text).strip()


def _block_rich_text(block: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return the rich_text array for known block types, else [].
    """
    t = block.get("type")
    if not t:
        return []
    payload = block.get(t) or {}
    if isinstance(payload, dict) and "rich_text" in payload:
        return payload.get("rich_text") or []
    return []


def _block_text(block: Dict[str, Any]) -> str:
    """
    Extract plain text from common Notion blocks (including to_do).
    """
    t = block.get("type")
    if not t:
        return ""

    payload = block.get(t) or {}

    # Most text blocks store content in "rich_text"
    if "rich_text" in payload:
        text = _plain_text(payload.get("rich_text"))
    else:
        text = ""

    if t == "to_do":
        checked = bool(payload.get("checked"))
        prefix = "[x] " if checked else "[ ] "
        return (prefix + text).strip()

    # Bullets / numbers: keep simple markers
    if t == "bulleted_list_item":
        return (f"- {text}").strip()
    if t == "numbered_list_item":
        return (f"1. {text}").strip()  # Notion doesn’t give the number; keep a generic prefix.

    # Headings: keep as plain text (already extracted)
    return text.strip()


# -------------------------
# Property extraction
# -------------------------

def _get_prop(properties: Dict[str, Any], name: str) -> Dict[str, Any]:
    return properties.get(name) or {}


def _as_title(prop: Dict[str, Any]) -> str:
    t = prop.get("type")
    if t == "title":
        return _plain_text(prop.get("title"))
    if t == "rich_text":
        return _plain_text(prop.get("rich_text"))
    return ""


def _as_select_name(prop: Dict[str, Any]) -> str:
    t = prop.get("type")
    if t == "select":
        sel = prop.get("select") or {}
        return (sel.get("name") or "").strip()
    if t == "status":
        st = prop.get("status") or {}
        return (st.get("name") or "").strip()
    return ""


def _as_multi_select_names(prop: Dict[str, Any]) -> str:
    t = prop.get("type")
    if t == "multi_select":
        items = prop.get("multi_select") or []
        names = [(it.get("name") or "").strip() for it in items if it.get("name")]
        return ", ".join([n for n in names if n])

    one = _as_select_name(prop)
    return one if one else ""


def _as_people_names(prop: Dict[str, Any]) -> str:
    if prop.get("type") != "people":
        return ""
    people = prop.get("people") or []
    names = []
    for p in people:
        nm = (p.get("name") or "").strip()
        if nm:
            names.append(nm)
    return ", ".join(names)


def _as_date_text(prop: Dict[str, Any]) -> str:
    if prop.get("type") != "date":
        return ""
    d = prop.get("date") or {}
    start = (d.get("start") or "").strip()
    end = (d.get("end") or "").strip()
    if start and end and end != start:
        return f"{start}..{end}"
    return start


# -------------------------
# Fetch comments + description blocks
# -------------------------

def fetch_comments_text(token: str, notion_version: str, page_id: str) -> str:
    url = f"{BASE_URL}/comments"
    comments = paginate_list_endpoint(
        token,
        notion_version,
        url,
        params={"block_id": page_id},
    )

    lines: List[str] = []
    for c in comments:
        created = (c.get("created_time") or "").strip()
        rich = c.get("rich_text") or []
        text = _plain_text(rich)
        if text:
            lines.append(f"{created}: {text}" if created else text)

    return "\n".join(lines).strip()


def fetch_block_children(token: str, notion_version: str, block_id: str) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/blocks/{block_id}/children"
    return paginate_list_endpoint(token, notion_version, url, params={"page_size": 100})


def fetch_page_blocks(token: str, notion_version: str, page_id: str) -> List[Dict[str, Any]]:
    # In Notion API, a page is also a block; you can fetch its children via blocks/<page_id>/children
    return fetch_block_children(token, notion_version, page_id)


def _flatten_blocks_with_children(
        token: str,
        notion_version: str,
        blocks: List[Dict[str, Any]],
        indent: int = 0,
) -> List[str]:
    """
    Depth-first flatten of blocks into text lines.
    Indents nested children for readability (especially to_do under headings).
    """
    lines: List[str] = []
    prefix = "  " * indent

    for b in blocks:
        txt = _block_text(b)
        if txt:
            lines.append(prefix + txt)

        if b.get("has_children"):
            child_id = b.get("id")
            if child_id:
                children = fetch_block_children(token, notion_version, child_id)
                lines.extend(_flatten_blocks_with_children(token, notion_version, children, indent=indent + 1))

    return lines


def extract_description_from_page(
        token: str,
        notion_version: str,
        page_id: str,
) -> str:
    """
    Extract everything AFTER the 'Beskrivelse:' label block, including headings and checkboxes.

    This fixes the truncation you saw, because we no longer stop at the next heading.
    """
    top_blocks = fetch_page_blocks(token, notion_version, page_id)

    # Find the index of the block that starts with "Beskrivelse"
    idx = -1
    for i, b in enumerate(top_blocks):
        txt = _block_text(b).lower().strip()
        if txt.startswith("beskrivelse"):
            idx = i
            break

    if idx == -1:
        return ""

    # Take all blocks after the "Beskrivelse:" label and flatten recursively
    after = top_blocks[idx + 1 :]
    lines = _flatten_blocks_with_children(token, notion_version, after, indent=0)

    # Optional: stop if we hit a new top-level "Comments/Kommentarer" section in content
    # (Notion comments are not blocks, but some pages include a manual section)
    cleaned: List[str] = []
    for line in lines:
        l = line.strip().lower()
        if l in ("kommentarer:", "comments:", "comments"):
            break
        cleaned.append(line)

    return "\n".join(cleaned).strip()


# -------------------------
# Main normalizer
# -------------------------

def normalize_task(
        page: Dict[str, Any],
        *,
        token: str,
        notion_version: str,
) -> Dict[str, str]:
    properties = page.get("properties") or {}
    page_id = page.get("id")

    title_text = _as_title(_get_prop(properties, "Title"))
    if not title_text:
        for _, prop in properties.items():
            if isinstance(prop, dict) and prop.get("type") == "title":
                title_text = _as_title(prop)
                break

    comments_text = ""
    description_text = ""
    if page_id:
        comments_text = fetch_comments_text(token, notion_version, page_id)
        description_text = extract_description_from_page(token, notion_version, page_id)

    out: Dict[str, str] = {
        "Title": (title_text or "").strip(),
        "Status": _as_select_name(_get_prop(properties, "Status")).strip(),
        "Prioritet": _as_select_name(_get_prop(properties, "Prioritet")).strip(),
        "Estimater": _as_select_name(_get_prop(properties, "Estimater")).strip(),
        "Tilordnet": _as_people_names(_get_prop(properties, "Tilordnet")).strip(),
        "Tags": _as_multi_select_names(_get_prop(properties, "Tags")).strip(),
        "Ferdigstilt": _as_date_text(_get_prop(properties, "Ferdigstilt")).strip(),
        "Arkiveringsdato": _as_date_text(_get_prop(properties, "Arkiveringsdato")).strip(),
        "Kodebase / type": _as_multi_select_names(_get_prop(properties, "Kodebase / type")).strip(),
        "Description": (description_text or "").strip(),
        "Comments": (comments_text or "").strip(),
    }

    for k, v in list(out.items()):
        out[k] = (v or "").strip()

    return out


def normalize_tasks(
        pages: List[Dict[str, Any]],
        *,
        token: str,
        notion_version: str,
) -> List[Dict[str, str]]:
    return [normalize_task(p, token=token, notion_version=notion_version) for p in pages]


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(f"\nERROR:\n{ex}\n", file=sys.stderr)
        sys.exit(1)