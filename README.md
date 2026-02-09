# DayZ Signal

Minimal HTTP service for sending global DayZ chat messages through BattlEye RCON.

The service sends this command to DayZ:

`say -1 <message>`

## 1. Configure

Create `.env` from `.env.example` and fill real values:

```env
HTTP_HOST=0.0.0.0
HTTP_PORT=8080
API_KEY=change_me

RCON_HOST=95.165.73.39
RCON_PORT=2305
RCON_PASSWORD=your_rcon_password
```

`API_KEY` is optional, but recommended.

## 2. Run with Docker Compose

```bash
docker compose up --build -d
```

Check logs:

```bash
docker compose logs -f
```

Stop:

```bash
docker compose down
```

## 3. API

### GET /health

Returns HTTP and RCON status.

### POST /broadcast

Sends a global message (`say -1`).

Request body:

```json
{
  "message": "Restart in 10 minutes"
}
```

If `API_KEY` is set, pass one header:

- `x-api-key: <API_KEY>`
- `Authorization: Bearer <API_KEY>`

Example:

```bash
curl -X POST "http://127.0.0.1:8080/broadcast" \
  -H "Content-Type: application/json" \
  -H "x-api-key: change_me" \
  -d "{\"message\":\"Restart in 10 minutes\"}"
```
