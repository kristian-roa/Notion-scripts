import os
import re
import json
import time
import requests
from dataclasses import dataclass
from typing import Any, Optional
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
from datetime import date

load_dotenv()

# ----------------------------
# Config (set env vars)
# ----------------------------
JIRA_BASE_URL = os.environ["JIRA_BASE_URL"].rstrip("/")
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]
JIRA_PROJECT_KEY = os.environ["JIRA_PROJECT_KEY"]

BOARD_ID = 527
RANK_CUSTOM_FIELD_ID = 10201  # from your board config

# Board status IDs from your board config
STATUS_ID_BACKLOG = "13380"
STATUS_ID_READY = "13383"
STATUS_ID_INPROGRESS = "13381"
STATUS_ID_QA = "13384"
STATUS_ID_DONE = "13382"

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
headers = {"Accept": "application/json", "Content-Type": "application/json"}

# ----------------------------
# Requested constants
# ----------------------------
CONST_OPPGAVEKATEGORI = "Markedsinitiativ"
CONST_CATEGORY_OF_WORK = "Strategic intent"

# Priority mapping you provided (Notion -> Jira priority name)
PRIORITY_MAP = {
    "P: Lav": "Uviktig",
    "P: Medium": "Mindre alvorlig",
    "P: Høy": "Alvorlig",
    "P: Haster": "Kritisk",
}

CUTOFF = date.fromisoformat("2026-01-01")

def _parse_iso_date_start(value: str | None) -> Optional[date]:
    """
    Accepts:
      'YYYY-MM-DD'
      'YYYY-MM-DD..YYYY-MM-DD'
      'YYYY-MM-DDTHH:MM:SS...'  (we take the date part)
    Returns a date or None.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None

    # handle ranges "start..end"
    if ".." in s:
        s = s.split("..", 1)[0].strip()

    # handle timestamps
    s = s[:10]

    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def filter_tasks(tasks: list[dict]) -> list[dict]:
    out = []
    for t in tasks:
        status = (t.get("Status") or "").strip()
        if status.lower() != "archived":
            out.append(t)
            continue

        # Archived: only include if Arkiveringsdato > 2026-01-01
        ark = _parse_iso_date_start(t.get("Arkiveringsdato"))
        if ark and ark > CUTOFF:
            out.append(t)

    return out


# ----------------------------
# Jira helpers
# ----------------------------
def adf_doc(text: str) -> dict:
    """Minimal Atlassian Document Format doc with paragraphs split on newlines."""
    paragraphs = []
    for line in (text or "").splitlines():
        # Keep empty lines as empty paragraphs for readability
        if line.strip() == "":
            paragraphs.append({"type": "paragraph", "content": []})
        else:
            paragraphs.append({
                "type": "paragraph",
                "content": [{"type": "text", "text": line}],
            })
    return {"type": "doc", "version": 1, "content": paragraphs or [{"type": "paragraph", "content": []}]}


def jira_request(
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        params: dict | None = None
) -> requests.Response:
    url = f"{JIRA_BASE_URL}{path}"
    return requests.request(method, url, auth=auth, headers=headers, json=json_body, params=params, timeout=30)


def create_issue(payload: dict) -> dict:
    r = jira_request("POST", "/rest/api/3/issue", json_body=payload)
    if r.status_code >= 400:
        raise RuntimeError(f"Create issue failed {r.status_code}: {r.text}")
    return r.json()


def move_issue_to_board(issue_key: str, *, rank_before: str | None = None, rank_after: str | None = None) -> None:
    """
    Equivalent to UI: Flytt arbeidsoppgave -> Tavle
    """
    payload = {"issues": [issue_key], "rankCustomFieldId": RANK_CUSTOM_FIELD_ID}
    if rank_before:
        payload["rankBeforeIssue"] = rank_before
    if rank_after:
        payload["rankAfterIssue"] = rank_after

    r = jira_request("POST", f"/rest/agile/1.0/board/{BOARD_ID}/issue", json_body=payload)
    # 204 No Content is success; 207 can happen for partial success
    if r.status_code not in (204, 207):
        raise RuntimeError(f"Move-to-board failed {r.status_code}: {r.text}")


def get_board_issues(max_results: int = 50) -> list[dict]:
    r = jira_request("GET", f"/rest/agile/1.0/board/{BOARD_ID}/issue", params={"maxResults": max_results})
    if r.status_code >= 400:
        raise RuntimeError(f"Get board issues failed {r.status_code}: {r.text}")
    return r.json().get("issues", [])


def get_transitions(issue_key: str) -> list[dict]:
    r = jira_request("GET", f"/rest/api/3/issue/{issue_key}/transitions")
    if r.status_code >= 400:
        raise RuntimeError(f"Get transitions failed {r.status_code}: {r.text}")
    return r.json().get("transitions", [])


def transition_to_status_id(issue_key: str, target_status_id: str) -> None:
    transitions = get_transitions(issue_key)
    match = next((t for t in transitions if t.get("to", {}).get("id") == target_status_id), None)
    if not match:
        available = [
            (t.get("id"), t.get("name"), t.get("to", {}).get("name"), t.get("to", {}).get("id"))
            for t in transitions
        ]
        raise RuntimeError(f"No transition from {issue_key} to status id {target_status_id}. Available: {available}")

    r = jira_request("POST", f"/rest/api/3/issue/{issue_key}/transitions",
                     json_body={"transition": {"id": match["id"]}})
    if r.status_code >= 400:
        raise RuntimeError(f"Transition failed {r.status_code}: {r.text}")


def add_comment(issue_key: str, comment_text: str) -> None:
    payload = {"body": adf_doc(comment_text)}
    r = jira_request("POST", f"/rest/api/3/issue/{issue_key}/comment", json_body=payload)
    if r.status_code >= 400:
        raise RuntimeError(f"Add comment failed {r.status_code}: {r.text}")


def get_priorities() -> list[dict]:
    r = jira_request("GET", "/rest/api/3/priority")
    if r.status_code >= 400:
        raise RuntimeError(f"Get priorities failed {r.status_code}: {r.text}")
    return r.json()


def get_fields() -> list[dict]:
    r = jira_request("GET", "/rest/api/3/field")
    if r.status_code >= 400:
        raise RuntimeError(f"Get fields failed {r.status_code}: {r.text}")
    return r.json()


def resolve_field_ids(field_names: list[str]) -> dict[str, str]:
    """
    Jira Cloud requires custom fields to be referenced by id (customfield_XXXXX).
    We resolve the IDs from /rest/api/3/field.
    """
    all_fields = get_fields()
    by_name = {f.get("name"): f.get("id") for f in all_fields if f.get("name") and f.get("id")}
    missing = [n for n in field_names if n not in by_name]
    if missing:
        raise RuntimeError(f"Could not resolve Jira field IDs for: {missing}")
    return {n: by_name[n] for n in field_names}


def set_issue_fields(issue_key: str, fields: dict) -> None:
    r = jira_request("PUT", f"/rest/api/3/issue/{issue_key}", json_body={"fields": fields})
    if r.status_code >= 400:
        raise RuntimeError(f"Update issue failed {r.status_code}: {r.text}")


# ----------------------------
# Mapping / parsing
# ----------------------------
def normalize_label(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-_]+", "", s)
    return s[:255]


def parse_tags(tags: str | None) -> list[str]:
    if not tags:
        return []
    parts = [p.strip() for p in tags.split(",") if p.strip()]
    return [normalize_label(p) for p in parts]


def merge_labels(*label_lists: list[str]) -> list[str]:
    """Merge multiple label lists, de-dupe while preserving order."""
    out: list[str] = []
    seen = set()
    for lst in label_lists:
        for x in lst:
            if x and x not in seen:
                seen.add(x)
                out.append(x)
    return out


def parse_estimate_to_jira(original: str | None) -> Optional[str]:
    """
    Your format: 'S: Liten (4t)' -> '4h'
    """
    if not original:
        return None
    m = re.search(r"\((\d+)\s*t\)", original.lower())
    if not m:
        return None
    hours = int(m.group(1))
    return f"{hours}h"


# ----------------------------
# Task mapping
# ----------------------------
@dataclass
class TaskMappingResult:
    summary: str
    description: str
    labels: list[str]
    priority_id: Optional[str]
    original_estimate: Optional[str]
    target_status_id: Optional[str]
    comments: list[str]


class TaskMapper:
    """
    Keep all mapping rules here so it’s easy to adjust and test.
    """

    def __init__(self):
        # Map external status to target status id on the board
        self.status_map = {
            "archived": STATUS_ID_DONE,
            "done": STATUS_ID_DONE,
            "qa": STATUS_ID_QA,
            "in progress": STATUS_ID_INPROGRESS,
            "ready for development": STATUS_ID_READY,
            "backlog": STATUS_ID_BACKLOG,
        }

    def map_priority(self, src: str | None, jira_priorities: list[dict]) -> Optional[str]:
        if not src:
            return None

        mapped_name = PRIORITY_MAP.get(src.strip(), src.strip())

        for p in jira_priorities:
            if (p.get("name") or "").strip().lower() == mapped_name.lower():
                return p.get("id")

        print(f"WARN: No Jira priority match for '{mapped_name}'")
        return None

    def map_status_id(self, src: str | None) -> Optional[str]:
        if not src:
            return None
        return self.status_map.get(src.strip().lower())

    def map_comments(self, src: str | None) -> list[str]:
        if not src:
            return []
        return [l.strip() for l in src.splitlines() if l.strip()]

    def map_task(self, task: dict, jira_priorities: list[dict]) -> TaskMappingResult:
        summary = (task.get("Title") or "").strip()
        if not summary:
            raise ValueError("Task missing Title")

        description = (task.get("Description") or "").strip()

        # Tags + Codebase should both become labels
        tags_labels = parse_tags(task.get("Tags"))
        codebase_src = task.get("Labels") or task.get("Kodebase / type")
        codebase_labels = parse_tags(codebase_src)

        labels = merge_labels(tags_labels, codebase_labels)

        priority_id = self.map_priority(task.get("Prioritet"), jira_priorities)
        original_estimate = parse_estimate_to_jira(task.get("Estimater"))

        target_status_id = self.map_status_id(task.get("Status"))
        comments = self.map_comments(task.get("Comments"))

        return TaskMappingResult(
            summary=summary,
            description=description,
            labels=labels,
            priority_id=priority_id,
            original_estimate=original_estimate,
            target_status_id=target_status_id,
            comments=comments,
        )


# ----------------------------
# Payload build (Option B)
# Create first WITHOUT custom fields,
# then update fields afterwards.
# ----------------------------
def build_create_payload(mapped: TaskMappingResult) -> dict:
    fields: dict[str, Any] = {
        "project": {"key": JIRA_PROJECT_KEY},
        "summary": mapped.summary,
        "issuetype": {"name": "Oppgave"},
    }

    if mapped.description:
        fields["description"] = adf_doc(mapped.description)

    if mapped.labels:
        fields["labels"] = mapped.labels

    if mapped.priority_id:
        fields["priority"] = {"id": mapped.priority_id}

    if mapped.original_estimate:
        fields["timetracking"] = {"originalEstimate": mapped.original_estimate}

    return {"fields": fields}


def build_update_fields(*, field_ids: dict[str, str]) -> dict:
    """
    Set the two required fields after create.
    For select fields Jira expects {"value": "..."}.
    """
    return {
        field_ids["Oppgavekategori"]: {"value": CONST_OPPGAVEKATEGORI},
        field_ids["Category of work"]: {"value": CONST_CATEGORY_OF_WORK},
    }


# ----------------------------
# Migration
# ----------------------------
def migrate_tasks(tasks: list[dict], *, dry_run: bool = False) -> list[str]:
    jira_priorities = get_priorities()
    mapper = TaskMapper()

    # Resolve custom field IDs once
    field_ids = resolve_field_ids(["Oppgavekategori", "Category of work"])
    print("Resolved field ids:", field_ids)

    created_keys: list[str] = []

    # Optional: pick an anchor for ranking (top of board) if there are existing issues on board
    anchor_issues = get_board_issues(max_results=1)
    anchor_key = anchor_issues[0]["key"] if anchor_issues else None

    for i, task in enumerate(tasks, start=1):
        mapped = mapper.map_task(task, jira_priorities)

        print(f"\n[{i}/{len(tasks)}] {mapped.summary}")
        print(f"  labels={mapped.labels}")
        print(f"  priority={mapped.priority_id} estimate={mapped.original_estimate}")
        print(f"  target_status_id={mapped.target_status_id} comments={len(mapped.comments)}")

        if dry_run:
            continue

        # 1) Create (WITHOUT the custom fields)
        payload = build_create_payload(mapped)
        issue = create_issue(payload)
        key = issue["key"]
        created_keys.append(key)
        print(f"  created={key}")

        # 1b) Update custom fields after creation
        try:
            update_fields = build_update_fields(field_ids=field_ids)
            set_issue_fields(key, update_fields)
            print("  custom_fields_set=yes")
        except Exception as e:
            # Don't hard-fail migration if fields can't be set (screen/context issue)
            print(f"  WARN: could not set custom fields: {e}")

        # 2) Move to board (UI: Flytt arbeidsoppgave -> Tavle)
        move_issue_to_board(key, rank_before=anchor_key)
        print("  moved_to_board=yes")

        # 3) Transition status (if needed and not already)
        if mapped.target_status_id and mapped.target_status_id != STATUS_ID_BACKLOG:
            try:
                transition_to_status_id(key, mapped.target_status_id)
                print(f"  transitioned_to_status_id={mapped.target_status_id}")
            except Exception as e:
                print(f"  WARN: transition failed: {e}")

        # 4) Add comments
        for c in mapped.comments:
            add_comment(key, c)
            time.sleep(0.1)

        # 5) Optional migration metadata (NO codebase here anymore)
        meta_lines = []
        if task.get("Ferdigstilt"):
            meta_lines.append(f"Original Ferdigstilt: {task['Ferdigstilt']}")
        if task.get("Arkiveringsdato"):
            meta_lines.append(f"Original Arkiveringsdato: {task['Arkiveringsdato']}")
        if meta_lines:
            add_comment(key, "Migration metadata:\n" + "\n".join(meta_lines))

    return created_keys


if __name__ == "__main__":
    if __name__ == "__main__":
        with open("tasks_notion_small.json", "r", encoding="utf-8") as f:
            tasks = json.load(f)

        tasks = filter_tasks(tasks)
        print(f"After filter: {len(tasks)} tasks")

        created = migrate_tasks(tasks, dry_run=False)
        print("\nCreated:", created)
