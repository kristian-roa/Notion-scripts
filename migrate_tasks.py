import requests
import os
from dotenv import load_dotenv

load_dotenv()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_URL = f'https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}'


def extract_tasks():
    headers = {
        "Notion-Version": "2025-09-03",
        "Authorization": f"Bearer {NOTION_TOKEN}"
    }

    query_url = f"{NOTION_URL}/query"
    response = requests.post(query_url, headers=headers)
    
    data = response.json()
    print(f"Extracted {len(data.get('results', []))} tasks.")
    print(data)


def main():
    extract_tasks()


if __name__ == '__main__':
    main()

