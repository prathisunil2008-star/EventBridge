# EventBridge

A small webhook receiver that stores incoming events and displays their status on a dashboard.

## Live dashboard

https://eventbridge-production.up.railway.app

## Send a test event

Send a `POST` request to:

`https://eventbridge-production.up.railway.app/webhooks/demo`

Include these headers:

- `X-Webhook-Secret`: your `WEBHOOK_SECRET` value from Railway
- `Idempotency-Key`: a unique value for this request

Example JSON body:

```json
{"message": "Hello from EventBridge"}

