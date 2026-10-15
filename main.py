from datetime import datetime, timezone
import asyncio
import httpx
import hashlib
import json
import hmac
import os
import sqlite3
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI(title="EventBridge")

DATABASE_PATH = Path(os.getenv("DATABASE_PATH", str(Path(__file__).with_name("events.db"))))
DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "https://httpbin.org/post")

event_subscribers: set[asyncio.Queue[None]] = set()


def notify_events_changed() -> None:
    """Notify connected dashboards that persisted event data changed."""
    for queue in tuple(event_subscribers):
        if queue.full():
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            queue.put_nowait(None)
        except asyncio.QueueFull:
            pass


async def send_to_downstream(payload: dict) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            DOWNSTREAM_URL,
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
    with sqlite3.connect(DATABASE_PATH, timeout=10) as connection:
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
        event_id, event_status = row
        should_process = False
        if event_status in ("received", "failed"):
            cursor = connection.execute(
                """
                UPDATE webhook_events
                SET status = 'processing',
                    attempt_count = attempt_count + 1,
                    failure_reason = NULL
                WHERE id = ? AND status IN ('received', 'failed')
                """,
                (event_id,),
            )
            should_process = cursor.rowcount == 1

    # Commit before notifying clients or awaiting the downstream HTTP request.
    notify_events_changed()
    if should_process:
        if not isinstance(payload, dict):
            status = "failed"
            failure_reason = "Webhook payload must be a JSON object"
        else:
            try:
                await send_to_downstream(payload)
            except httpx.HTTPError as error:
                status = "failed"
                failure_reason = str(error)
            else:
                status = "completed"
                failure_reason = None

        with sqlite3.connect(DATABASE_PATH, timeout=10) as connection:
            connection.execute(
                """
                UPDATE webhook_events
                SET status = ?, failure_reason = ?
                WHERE id = ?
                """,
                (status, failure_reason, event_id),
            )
        notify_events_changed()
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


@app.get("/events/stream")
async def stream_event_updates():
    queue: asyncio.Queue[None] = asyncio.Queue(maxsize=1)
    event_subscribers.add(queue)

    async def event_stream():
        try:
            # Send an initial event so each newly connected dashboard loads now.
            yield "event: events\ndata: update\n\n"
            while True:
                try:
                    await asyncio.wait_for(queue.get(), timeout=15)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                else:
                    yield "event: events\ndata: update\n\n"
        finally:
            event_subscribers.discard(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

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

    notify_events_changed()

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

    notify_events_changed()

    return {"event_id": event_id, "status": status}
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>EventBridge</title>
        <style>
            :root { color-scheme: dark; --bg: #0b1020; --surface: #111a2d; --surface-2: #17233a; --line: #263550; --text: #edf3ff; --muted: #8fa1bc; --blue: #76a9ff; --green: #45d6a0; --red: #ff7484; --amber: #ffc66d; }
            * { box-sizing: border-box; }
            body { margin: 0; min-height: 100vh; background: radial-gradient(ellipse at 50% -20%, #1a2c4b 0, transparent 58%), var(--bg); color: var(--text); font: 15px/1.5 Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif; }
            button, input { font: inherit; }
            .shell { max-width: 1180px; margin: 0 auto; padding: 28px 28px 48px; }
            .topbar { display: flex; align-items: center; justify-content: space-between; padding-bottom: 24px; border-bottom: 1px solid rgba(143,161,188,.16); }
            .brand { display: flex; align-items: center; gap: 12px; font-weight: 700; letter-spacing: -.02em; }
            .brand-mark { display: grid; width: 38px; height: 38px; place-items: center; border-radius: 12px; background: linear-gradient(145deg, #79b0ff, #7868fa); color: #071225; font-size: 13px; font-weight: 900; box-shadow: 0 8px 24px #586bff35; }
            .brand-name { font-size: 16px; }
            .system-pill, .stream-state { display: inline-flex; align-items: center; gap: 8px; color: var(--muted); font-size: 12px; }
            .system-pill { border: 1px solid #24443f; background: #10241f; color: #81e4bd; border-radius: 999px; padding: 7px 11px; font-weight: 700; letter-spacing: .04em; }
            .dot { width: 8px; height: 8px; flex: 0 0 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 0 4px #45d6a01a; }
            .page-heading { display: flex; align-items: flex-end; justify-content: space-between; gap: 20px; padding: 38px 0 27px; }
            .eyebrow { margin: 0 0 7px; color: var(--blue); font-size: 11px; font-weight: 800; letter-spacing: .12em; text-transform: uppercase; }
            h1 { margin: 0; font-size: clamp(30px, 5vw, 42px); line-height: 1.1; letter-spacing: -.045em; }
            .intro { margin: 11px 0 0; color: var(--muted); }
            .stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 15px; margin-bottom: 22px; }
            .stat-card { display: flex; align-items: center; gap: 15px; min-height: 105px; padding: 19px 21px; border: 1px solid var(--line); border-radius: 15px; background: linear-gradient(145deg, #14203a, #10182a); box-shadow: 0 14px 35px #00000012; }
            .stat-icon { display: grid; width: 42px; height: 42px; place-items: center; border-radius: 12px; font-size: 19px; font-weight: 700; }
            .total-icon { background: #20375b; color: #8fbbff; }
            .done-icon { background: #15392f; color: #61dfad; }
            .failed-icon { background: #40232e; color: #ff8796; }
            .stat-label { margin: 0 0 2px; color: var(--muted); font-size: 11px; font-weight: 750; letter-spacing: .09em; }
            .stat-value { font-size: 25px; font-weight: 750; letter-spacing: -.03em; }
            .panel { overflow: hidden; border: 1px solid var(--line); border-radius: 16px; background: rgba(15,23,40,.93); box-shadow: 0 22px 60px #00000025; }
            .panel-heading { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 21px 23px; border-bottom: 1px solid var(--line); }
            .panel-heading h2 { margin: 0; font-size: 18px; letter-spacing: -.02em; }
            .panel-heading .eyebrow { margin-bottom: 3px; }
            .stream-state { white-space: nowrap; }
            #updated { color: #70829e; }
            .table-wrap { overflow-x: auto; }
            table { width: 100%; border-collapse: collapse; min-width: 690px; }
            th { padding: 13px 20px; background: #121c30; color: #91a4c0; font-size: 10px; font-weight: 800; letter-spacing: .09em; text-align: left; text-transform: uppercase; }
            td { padding: 16px 20px; border-top: 1px solid #202d43; color: #c7d3e6; }
            tbody tr { transition: background .16s ease; }
            tbody tr:hover { background: #17243a; }
            .event-id { color: #8fbbff; font-weight: 750; font-variant-numeric: tabular-nums; }
            .source-tag { display: inline-block; border: 1px solid #31415c; border-radius: 6px; background: #18253b; padding: 3px 8px; color: #cedcf0; font-size: 12px; }
            .timestamp { color: #91a2ba; font-size: 13px; white-space: nowrap; }
            .status { display: inline-flex; align-items: center; gap: 7px; border-radius: 999px; padding: 5px 9px; font-size: 11px; font-weight: 750; text-transform: capitalize; }
            .status::before { width: 6px; height: 6px; border-radius: 50%; background: currentColor; content: ""; }
            .status-completed { background: #14352c; color: #65dcae; }
            .status-failed { background: #3b202b; color: #ff8796; }
            .status-processing { background: #342b1b; color: #ffd17b; }
            .status-received { background: #1d2e49; color: #9fc2ff; }
            .attempts { color: #d6e1f2; font-variant-numeric: tabular-nums; }
            .actions { display: flex; gap: 7px; }
            .action-button { cursor: pointer; border: 1px solid #344563; border-radius: 8px; background: #17243a; padding: 7px 10px; color: #c6d9f6; font-size: 12px; font-weight: 650; transition: border-color .15s, background .15s; }
            .action-button:hover { border-color: #6a9df2; background: #203553; }
            .retry-button { border-color: #70414b; background: #321f2a; color: #ffabb4; }
            .retry-button:hover { border-color: #d36e7e; background: #482631; }
            .empty-state { padding: 46px 20px; color: var(--muted); text-align: center; }
            .empty-icon { display: grid; width: 46px; height: 46px; margin: 0 auto 12px; place-items: center; border: 1px solid #2b3a55; border-radius: 14px; background: #17243a; color: #8fbbff; font-size: 20px; }
            .footer-note { margin: 18px 3px 0; color: #7486a0; font-size: 12px; }
            .modal-backdrop { position: fixed; z-index: 10; inset: 0; display: grid; place-items: center; overflow-y: auto; padding: 20px; background: #050914c9; backdrop-filter: blur(7px); }
            .modal-backdrop[hidden] { display: none; }
            .modal-card { width: min(100%, 560px); border: 1px solid #31425e; border-radius: 16px; background: #111a2d; padding: 22px; box-shadow: 0 30px 90px #0009; }
            .modal-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 18px; }
            .modal-header h2 { margin: 0; font-size: 20px; }
            .close-button { cursor: pointer; border: 1px solid #34445f; border-radius: 8px; background: #17243a; padding: 4px 10px; color: #c7d3e6; font-size: 20px; }
            .field-label { display: block; margin-bottom: 7px; color: #c6d3e7; font-size: 13px; font-weight: 650; }
            .password-input { width: 100%; border: 1px solid #364763; border-radius: 9px; outline: none; background: #0b1323; padding: 11px 12px; color: var(--text); }
            .password-input:focus { border-color: #78a9ff; box-shadow: 0 0 0 3px #78a9ff22; }
            .modal-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 15px; }
            .primary-button { cursor: pointer; border: 0; border-radius: 9px; background: linear-gradient(135deg, #78a9ff, #8c80ff); padding: 10px 14px; color: #071225; font-weight: 750; }
            .secondary-button { cursor: pointer; border: 1px solid #34445f; border-radius: 9px; background: #17243a; padding: 9px 13px; color: #c7d3e6; }
            .modal-error { margin: 11px 0 0; color: #ff8d9a; font-size: 13px; }
            .detail-content { margin-top: 18px; border-top: 1px solid var(--line); padding-top: 17px; }
            .detail-content h3 { margin: 0 0 10px; font-size: 13px; color: #9db0cc; }
            .detail-content pre { overflow: auto; max-height: 280px; margin: 0; border: 1px solid #2a3952; border-radius: 10px; background: #0a1221; padding: 14px; color: #cde0ff; font: 12px/1.55 ui-monospace, SFMono-Regular, Consolas, monospace; white-space: pre-wrap; overflow-wrap: anywhere; }
            @media (max-width: 700px) { .shell { padding: 18px 14px 32px; } .page-heading { align-items: flex-start; flex-direction: column; padding-top: 29px; } .stats { grid-template-columns: 1fr; gap: 10px; } .stat-card { min-height: 78px; padding: 14px 16px; } .stat-icon { width: 38px; height: 38px; } .panel-heading { align-items: flex-start; flex-direction: column; padding: 17px; } th, td { padding: 13px 14px; } }
        </style>
    </head>
    <body>
        <div class="shell">
            <header class="topbar">
                <div class="brand"><div class="brand-mark">EB</div><span class="brand-name">EventBridge</span></div>
                <div class="system-pill"><span class="dot"></span> SYSTEM ONLINE</div>
            </header>
            <main>
                <section class="page-heading">
                    <div>
                        <p class="eyebrow">EVENT OPERATIONS</p>
                        <h1>Webhook activity</h1>
                        <p class="intro">A clear view of every delivery, from arrival to completion.</p>
                    </div>
                </section>
                <section class="stats" aria-label="Event summary">
                    <article class="stat-card"><div class="stat-icon total-icon">↗</div><div><p class="stat-label">TOTAL EVENTS</p><div class="stat-value" id="totalEvents">0</div></div></article>
                    <article class="stat-card"><div class="stat-icon done-icon">✓</div><div><p class="stat-label">COMPLETED</p><div class="stat-value" id="completedEvents">0</div></div></article>
                    <article class="stat-card"><div class="stat-icon failed-icon">!</div><div><p class="stat-label">FAILED</p><div class="stat-value" id="failedEvents">0</div></div></article>
                </section>
                <section class="panel" aria-label="Webhook events">
                    <div class="panel-heading">
                        <div><p class="eyebrow">INCOMING TRAFFIC</p><h2>Recent events</h2></div>
                        <div class="stream-state"><span class="dot" id="streamDot"></span><span id="streamLabel">Connecting to live updates…</span><span>·</span><span id="updated">Loading</span></div>
                    </div>
                    <div class="table-wrap">
                        <table>
                            <thead><tr><th>ID</th><th>Source</th><th>Received</th><th>Status</th><th>Attempts</th><th>Actions</th></tr></thead>
                            <tbody id="events" aria-live="polite"></tbody>
                        </table>
                    </div>
                </section>
                <p class="footer-note">Payload details and retries require the dashboard password.</p>
            </main>
        </div>

        <div class="modal-backdrop" id="authModal" hidden>
            <section class="modal-card" role="dialog" aria-modal="true" aria-labelledby="authTitle">
                <div class="modal-header"><div><p class="eyebrow">SECURE ACCESS</p><h2 id="authTitle">Verify dashboard password</h2></div><button class="close-button" id="closeAuth" aria-label="Close">×</button></div>
                <form id="authForm">
                    <label class="field-label" for="viewerPassword">Dashboard password</label>
                    <input class="password-input" id="viewerPassword" type="password" autocomplete="current-password" required>
                    <p class="modal-error" id="authError" hidden></p>
                    <div class="modal-actions"><button class="secondary-button" id="cancelAuth" type="button">Cancel</button><button class="primary-button" type="submit">Continue</button></div>
                </form>
            </section>
        </div>

        <div class="modal-backdrop" id="detailsModal" hidden>
            <section class="modal-card" role="dialog" aria-modal="true" aria-labelledby="detailsTitle">
                <div class="modal-header"><div><p class="eyebrow">EVENT DETAILS</p><h2 id="detailsTitle">Event</h2></div><button class="close-button" id="closeDetails" aria-label="Close">×</button></div>
                <div id="detailContent" class="detail-content"></div>
            </section>
        </div>

        <script>
            async function readResponse(response) {
                const data = await response.json();
                if (!response.ok) throw new Error(data.detail || "Request failed");
                return data;
            }

            function askForPassword() {
                return new Promise((resolve) => {
                    const modal = document.getElementById("authModal");
                    const form = document.getElementById("authForm");
                    const input = document.getElementById("viewerPassword");
                    const error = document.getElementById("authError");
                    const finish = (value) => {
                        modal.hidden = true;
                        form.onsubmit = null;
                        document.getElementById("cancelAuth").onclick = null;
                        document.getElementById("closeAuth").onclick = null;
                        resolve(value);
                    };
                    input.value = "";
                    error.hidden = true;
                    modal.hidden = false;
                    input.focus();
                    form.onsubmit = (event) => { event.preventDefault(); finish(input.value); };
                    document.getElementById("cancelAuth").onclick = () => finish(null);
                    document.getElementById("closeAuth").onclick = () => finish(null);
                });
            }

            function showDetails(data) {
                const modal = document.getElementById("detailsModal");
                const content = document.getElementById("detailContent");
                document.getElementById("detailsTitle").textContent = `Event ${data.event_id}`;
                content.replaceChildren();
                const summary = document.createElement("p");
                summary.className = "timestamp";
                summary.textContent = `${data.source} · ${data.status} · ${data.attempt_count} attempt(s) · ${new Date(data.received_at).toLocaleString()}`;
                const title = document.createElement("h3");
                title.textContent = "Payload";
                const payload = document.createElement("pre");
                payload.textContent = JSON.stringify(data.payload, null, 2);
                content.append(summary, title, payload);
                if (data.failure_reason) {
                    const reasonTitle = document.createElement("h3");
                    reasonTitle.textContent = "Failure reason";
                    const reason = document.createElement("pre");
                    reason.textContent = data.failure_reason;
                    content.append(reasonTitle, reason);
                }
                modal.hidden = false;
            }

            async function loadDetails(eventId) {
                const password = await askForPassword();
                if (!password) return;
                try {
                    const response = await fetch(`/events/${eventId}/details`, { headers: { "X-Viewer-Password": password } });
                    showDetails(await readResponse(response));
                } catch (error) {
                    alert(error.message);
                }
            }

            async function retryEvent(eventId) {
                if (!confirm(`Retry event ${eventId}?`)) return;
                const password = await askForPassword();
                if (!password) return;
                try {
                    const response = await fetch(`/events/${eventId}/retry`, { method: "POST", headers: { "X-Viewer-Password": password } });
                    const data = await readResponse(response);
                    await refreshEvents();
                    alert(`Event ${eventId} is ${data.status}.`);
                } catch (error) {
                    alert(error.message);
                }
            }

            function cell(text, className) {
                const element = document.createElement("td");
                element.textContent = text ?? "";
                if (className) element.className = className;
                return element;
            }

            async function refreshEvents() {
                const tableBody = document.getElementById("events");
                try {
                    const response = await fetch("/events");
                    const events = await readResponse(response);
                    tableBody.replaceChildren();
                    document.getElementById("totalEvents").textContent = events.length;
                    document.getElementById("completedEvents").textContent = events.filter((event) => event.status === "completed").length;
                    document.getElementById("failedEvents").textContent = events.filter((event) => event.status === "failed").length;

                    if (!events.length) {
                        const row = document.createElement("tr");
                        const empty = cell("");
                        empty.colSpan = 6;
                        empty.innerHTML = '<div class="empty-state"><div class="empty-icon">↗</div><strong>No events yet</strong><div>Your webhook deliveries will appear here.</div></div>';
                        row.appendChild(empty);
                        tableBody.appendChild(row);
                    }

                    for (const event of events) {
                        const row = document.createElement("tr");
                        row.appendChild(cell(event.event_id, "event-id"));
                        const source = cell("");
                        const tag = document.createElement("span");
                        tag.className = "source-tag";
                        tag.textContent = event.source;
                        source.appendChild(tag);
                        row.appendChild(source);
                        row.appendChild(cell(new Date(event.received_at).toLocaleString(), "timestamp"));
                        const statusCell = cell("");
                        const badge = document.createElement("span");
                        const status = String(event.status || "received").toLowerCase();
                        badge.className = `status status-${status}`;
                        badge.textContent = status;
                        statusCell.appendChild(badge);
                        row.appendChild(statusCell);
                        row.appendChild(cell(event.attempt_count, "attempts"));

                        const actions = document.createElement("td");
                        const actionGroup = document.createElement("div");
                        actionGroup.className = "actions";
                        const detailsButton = document.createElement("button");
                        detailsButton.className = "action-button";
                        detailsButton.textContent = "View details";
                        detailsButton.onclick = () => loadDetails(event.event_id);
                        actionGroup.appendChild(detailsButton);
                        if (event.status === "failed") {
                            const retryButton = document.createElement("button");
                            retryButton.className = "action-button retry-button";
                            retryButton.textContent = "Retry";
                            retryButton.onclick = () => retryEvent(event.event_id);
                            actionGroup.appendChild(retryButton);
                        }
                        actions.appendChild(actionGroup);
                        row.appendChild(actions);
                        tableBody.appendChild(row);
                    }
                    document.getElementById("updated").textContent = "Updated " + new Date().toLocaleTimeString();
                } catch (error) {
                    document.getElementById("updated").textContent = "Unable to load events";
                }
            }

            document.getElementById("closeDetails").onclick = () => { document.getElementById("detailsModal").hidden = true; };
            document.getElementById("detailsModal").onclick = (event) => { if (event.target.id === "detailsModal") event.currentTarget.hidden = true; };
            document.getElementById("authModal").onclick = (event) => { if (event.target.id === "authModal") document.getElementById("cancelAuth").click(); };

            refreshEvents();
            const eventStream = new EventSource("/events/stream");
            eventStream.addEventListener("events", refreshEvents);
            eventStream.onopen = () => {
                document.getElementById("streamLabel").textContent = "Live updates connected";
                document.getElementById("streamDot").style.background = "var(--green)";
            };
            eventStream.onerror = () => {
                document.getElementById("streamLabel").textContent = "Reconnecting…";
                document.getElementById("streamDot").style.background = "var(--amber)";
            };
        </script>
    </body>
    </html>
    """
