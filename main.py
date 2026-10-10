from datetime import datetime, timezone
import httpx
import hashlib
import json
import hmac
import os
import sqlite3
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse

app = FastAPI(title="EventBridge")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(Path(__file__).with_name("events.db"))))
async def send_to_downstream(payload: dict) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            "https://httpbin.org/post",
            json=payload,
        )
        response.raise_for_status()

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

        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(webhook_events)"
            )
        }
        if "idempotency_key" not in columns:
            connection.execute(
                "ALTER TABLE webhook_events ADD COLUMN idempotency_key TEXT"
            )
        if "attempt_count" not in columns:
             connection.execute(
                "ALTER TABLE webhook_events ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0"
            )
        if "failure_reason" not in columns:
            connection.execute(
                "ALTER TABLE webhook_events ADD COLUMN failure_reason TEXT"
            )
        connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_webhook_events_idempotency_key
                ON webhook_events (idempotency_key)
                """
            )
initialize_database()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/{source}")
async def receive_webhook(
    source: str,
    request: Request,
    supplied_secret: str = Header(..., alias="X-Webhook-Secret"),
):
    expected_secret = os.getenv("WEBHOOK_SECRET")

    if not expected_secret or not hmac.compare_digest(
        supplied_secret, expected_secret
    ):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    try:
        raw_body = await request.body()
        payload = json.loads(raw_body)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail="Body must be valid JSON",
        ) from exc
    delivery_id = (
        request.headers.get("Idempotency-Key")
        or request.headers.get("X-GitHub-Delivery")
    )
    key_value = delivery_id or hashlib.sha256(raw_body).hexdigest()
    idempotency_key = f"{source}:{key_value}"
    with sqlite3.connect(DATABASE_PATH) as connection:
                connection.execute(
            """
            INSERT INTO webhook_events (
                source, received_at, payload_json, idempotency_key
            )
            VALUES (?, ?, ?, ?)
            ON CONFLICT(idempotency_key) DO UPDATE SET
                received_at = excluded.received_at,
                payload_json = excluded.payload_json
            """,
            (
                source,
                datetime.now(timezone.utc).isoformat(),
                json.dumps(payload),
                idempotency_key,
            ),
        )
                row = connection.execute(
           "SELECT id, status FROM webhook_events WHERE idempotency_key = ?",
            (idempotency_key,),
                ).fetchone()
                event_id = row[0]
                event_status = row[1]
                if event_status != "completed":
                   if isinstance(payload, dict):
                      try:
                          await send_to_downstream(payload)
                      except httpx.HTTPError as error:
                          connection.execute(
                        """
                        UPDATE webhook_events
                        SET status = 'failed',
                            attempt_count = attempt_count + 1,
                            failure_reason = ?
                        WHERE id = ?
                        """,
                        (str(error), event_id),
                           )
                      else:
                          connection.execute(
                        """
                        UPDATE webhook_events
                        SET status = 'completed',
                            attempt_count = attempt_count + 1,
                            failure_reason = NULL
                        WHERE id = ?
                        """,
                        (event_id,),
                          )
                else:
                    connection.execute(
                    """
                    UPDATE webhook_events
                    SET status = 'failed',
                        attempt_count = attempt_count + 1,
                        failure_reason = ?
                    WHERE id = ?
                    """,
                    ("Webhook payload must be a JSON object", event_id),
                   )
    return {"accepted": True, "event_id": event_id}

@app.get("/events")
def list_events():
    with sqlite3.connect(DATABASE_PATH) as connection:
        rows = connection.execute(
            """
            SELECT id, source, received_at, status, attempt_count
            FROM webhook_events
            ORDER BY id DESC
            """
        ).fetchall()

    return [
        {
            "event_id": row[0],
            "source": row[1],
            "received_at": row[2],
            "status": row[3],
            "attempt_count": row[4],
        }
        for row in rows
    ]

@app.get("/events/{event_id}/details")
def get_event_details(
    event_id: int,
    viewer_password: str = Header(..., alias="X-Viewer-Password"),
):
    expected_password = os.getenv("DASHBOARD_PASSWORD")
    if not expected_password or not hmac.compare_digest(
        viewer_password, expected_password
    ):
        raise HTTPException(status_code=401, detail="Invalid viewer password")

    with sqlite3.connect(DATABASE_PATH) as connection:
        row = connection.execute(
            """
            SELECT id, source, received_at, payload_json, status,
                   attempt_count, failure_reason
            FROM webhook_events
            WHERE id = ?
            """,
            (event_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Event not found")

    return {
        "event_id": row[0],
        "source": row[1],
        "received_at": row[2],
        "payload": json.loads(row[3]),
        "status": row[4],
        "attempt_count": row[5],
        "failure_reason": row[6],
    }
@app.post("/events/{event_id}/retry")
async def retry_event(
    event_id: int,
    viewer_password: str = Header(..., alias="X-Viewer-Password"),
):
    expected_password = os.getenv("DASHBOARD_PASSWORD")
    if not expected_password or not hmac.compare_digest(
        viewer_password, expected_password
    ):
        raise HTTPException(status_code=401, detail="Invalid viewer password")

    with sqlite3.connect(DATABASE_PATH) as connection:
        row = connection.execute(
            "SELECT payload_json, status FROM webhook_events WHERE id = ?",
            (event_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="Event not found")
        if row[1] != "failed":
            raise HTTPException(
                status_code=409,
                detail="Only failed events can be retried",
            )

        payload = json.loads(row[0])
        cursor = connection.execute(
            """
            UPDATE webhook_events
            SET status = 'processing',
                attempt_count = attempt_count + 1,
                failure_reason = NULL
            WHERE id = ? AND status = 'failed'
            """,
            (event_id,),
        )
        if cursor.rowcount != 1:
            raise HTTPException(status_code=409, detail="Event is already being retried")

    try:
        if not isinstance(payload, dict):
            raise ValueError("Webhook payload must be a JSON object")
        await send_to_downstream(payload)    
        status = "completed"
        failure_reason = None
    except (ValueError, httpx.HTTPError) as exc:
        status = "failed"
        failure_reason = str(exc)

    with sqlite3.connect(DATABASE_PATH) as connection:
        connection.execute(
            """
            UPDATE webhook_events
            SET status = ?, failure_reason = ?
            WHERE id = ?
            """,
            (status, failure_reason, event_id),
        )

    return {"event_id": event_id, "status": status}
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>EventBridge</title>
        <style>
            body { font: 16px Arial, sans-serif; max-width: 1000px; margin: 40px auto; padding: 0 16px; color: #243047; }
            table { width: 100%; border-collapse: collapse; margin-top: 24px; }
            th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
            th { background: #f2f5f9; }
            #updated { color: #667085; }
        </style>
    </head>
    <body>
        <h1>EventBridge</h1>
        <p>Webhook events — refreshes every 5 seconds.</p>
        <p id="updated">Loading events…</p>
        <table>
            <thead>
                <tr><th>ID</th><th>Source</th><th>Received</th><th>Status</th><th>Attempts</th><th>Actions</th></tr>
            </thead>
            <tbody id="events"></tbody>
        </table>
        <script>
            async function readResponse(response) {
    const data = await response.json();
    if (!response.ok) throw new Error(data.detail || "Request failed");
    return data;
}

async function showDetails(eventId) {
    const password = prompt("Enter the viewer password:");
    if (!password) return;

    try {
        const response = await fetch(`/events/${eventId}/details`, {
            headers: { "X-Viewer-Password": password }
        });
        const data = await readResponse(response);
        alert(JSON.stringify(data, null, 2));
    } catch (error) {
        alert(error.message);
    }
}

async function retryEvent(eventId) {
    if (!confirm(`Retry event ${eventId}?`)) return;
    const password = prompt("Enter the viewer password:");
    if (!password) return;

    try {
        const response = await fetch(`/events/${eventId}/retry`, {
            method: "POST",
            headers: { "X-Viewer-Password": password }
        });
        const data = await readResponse(response);
        alert(`Event ${eventId}: ${data.status}`);
        refreshEvents();
    } catch (error) {
        alert(error.message);
    }
}

async function refreshEvents() {
    const updated = document.getElementById("updated");
    const tableBody = document.getElementById("events");

    try {
        const response = await fetch("/events");
        const events = await readResponse(response);
        tableBody.replaceChildren();

        for (const event of events) {
            const row = document.createElement("tr");

            for (const value of [
                event.event_id,
                event.source,
                event.received_at,
                event.status,
                event.attempt_count
            ]) {
                const cell = document.createElement("td");
                cell.textContent = value ?? "";
                row.appendChild(cell);
            }

            const actions = document.createElement("td");
            const detailsButton = document.createElement("button");
            detailsButton.textContent = "Details";
            detailsButton.onclick = () => showDetails(event.event_id);
            actions.appendChild(detailsButton);

            if (event.status === "failed") {
                const retryButton = document.createElement("button");
                retryButton.textContent = "Retry";
                retryButton.onclick = () => retryEvent(event.event_id);
                actions.appendChild(retryButton);
            }

            row.appendChild(actions);
            tableBody.appendChild(row);
        }

        updated.textContent = "Updated: " + new Date().toLocaleTimeString();
    } catch (error) {
        updated.textContent = error.message;
    }
}

refreshEvents();
setInterval(refreshEvents, 5000);
        </script>
    </body>
    </html>
    """