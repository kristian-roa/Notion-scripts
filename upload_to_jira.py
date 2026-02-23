import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

JIRA_BASE_URL = os.environ["JIRA_BASE_URL"]
JIRA_EMAIL = os.environ["JIRA_EMAIL"]
JIRA_API_TOKEN = os.environ["JIRA_API_TOKEN"]
PROJECT_KEY = os.environ["JIRA_PROJECT_KEY"]
BOARD_ID = 527
RANK_CUSTOM_FIELD_ID = 10201


def adf_paragraph(text: str) -> dict:
    # Minimal ADF document with one paragraph of text
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": text}
                ],
            }
        ],
    }


def create_task(summary: str, description: str | None = None) -> dict:
    url = f"{JIRA_BASE_URL.rstrip('/')}/rest/api/3/issue"
    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    fields = {
        "project": {"key": PROJECT_KEY},
        "summary": summary,
        "issuetype": {"name": "Task"},
    }

    if description:
        fields["description"] = adf_paragraph(description)

    payload = {"fields": fields}

    r = requests.post(url, headers=headers, auth=auth, json=payload, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"Jira error {r.status_code}: {r.text}")

    json = r.json()

    key = json["key"]
    move_issue_to_board(key)

    return json


def move_issue_to_board(issue_key: str, rank_before: str | None = None, rank_after: str | None = None) -> None:
    """
    Equivalent to UI: Flytt arbeidsoppgave -> Tavle
    """

    auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    url = f"{JIRA_BASE_URL}/rest/agile/1.0/board/{BOARD_ID}/issue"
    payload = {
        "issues": [issue_key],
        "rankCustomFieldId": RANK_CUSTOM_FIELD_ID,
    }
    if rank_before:
        payload["rankBeforeIssue"] = rank_before
    if rank_after:
        payload["rankAfterIssue"] = rank_after

    r = requests.post(url, auth=auth, headers=headers, json=payload, timeout=30)
    if r.status_code not in (204, 207):
        raise RuntimeError(f"Move-to-board error {r.status_code}: {r.text}")


if __name__ == "__main__":
    issue = create_task(summary="Test task created by Python", description="Created via REST API from a script.")
    print("Created:", issue.get("key"))

