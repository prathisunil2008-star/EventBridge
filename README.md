# EventBridge

EventBridge is a small webhook receiver and event-processing dashboard. It stores each delivery in SQLite, forwards JSON payloads to a downstream HTTP endpoint, and tracks the processing result.

## Live app

Dashboard: https://eventbridge-production.up.railway.app

Webhook endpoint: `POST https://eventbridge-production.up.railway.app/webhooks/{source}`

For example, use `/webhooks/demo` as the source.

## Send a webhook

Send a JSON object with the `X-Webhook-Secret` header set to the `WEBHOOK_SECRET` configured in Railway. An `Idempotency-Key` is recommended for each delivery; GitHub's `X-GitHub-Delivery` header is also recognized. If neither is provided, EventBridge uses a hash of the raw request body.

PowerShell example (replace the placeholder locally; never commit or share the secret):

```powershell
$headers = @{ "X-Webhook-Secret" = "<your WEBHOOK_SECRET>"; "Idempotency-Key" = "demo-001" }
Invoke-RestMethod -Uri "https://eventbridge-production.up.railway.app/webhooks/demo" -Method Post -Headers $headers -ContentType "application/json" -Body '{"message":"Hello from EventBridge"}'
```

The response includes `accepted` and the event ID. Open the dashboard to see the event and its status.

## Features

- Validates webhook requests with a server-side header secret.
- Persists event ID, source, received time, payload, status, attempt count, and failure reason in SQLite.
- Reuses the same event row for duplicate deliveries with the same idempotency key.
- Streams event-list and status changes to open dashboards using Server-Sent Events (SSE).
- Requires the shared `DASHBOARD_PASSWORD` to view payload details or retry a failed event.
- Retries failed events from the dashboard using the stored payload and prevents a second retry while one is in progress.
- Stores the database on Railway's mounted volume at `/data/events.db`.

## Downstream processing

For this project, EventBridge forwards each JSON object to `https://httpbin.org/post`, a test HTTP endpoint. Use sample data only; this is not connected to a production business service.

## Railway configuration

Set these variables in the Railway service's **Variables** tab. Keep their values out of source control:

- `WEBHOOK_SECRET` — required to accept webhook requests.
- `DASHBOARD_PASSWORD` — required to view event payloads and trigger retries.
- `DATABASE_PATH` — set to `/data/events.db` for the Railway volume.

The service starts with `uvicorn main:app --host 0.0.0.0 --port $PORT`.

## Local development

Install the packages with `pip install -r requirements.txt`, set `WEBHOOK_SECRET` and `DASHBOARD_PASSWORD` in your local environment, then start the app with `uvicorn main:app --reload`. The dashboard is available at `http://127.0.0.1:8000`.

Do not commit real secrets, and do not send sensitive payloads to the test downstream endpoint.
