from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

app = FastAPI(title="EventBridge")

DATABASE_PATH = Path(__file__).with_name("events.db")


def initialize_database():
    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                received_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'received'
            )
            """
        )


initialize_database()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/{source}")
async def receive_webhook(source: str, request: Request):
    try:
        payload = await request.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Body must be valid JSON",
        ) from exc

    with sqlite3.connect(DATABASE_PATH) as connection:
        cursor = connection.execute(
            """
            INSERT INTO webhook_events (source, received_at, payload_json)
            VALUES (?, ?, ?)
            """,
            (
                source,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(payload),
            ),
        )
        event_id = cursor.lastrowid

    return {"accepted": True, "event_id": event_id}