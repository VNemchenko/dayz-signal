# DayZ Signal

Minimal HTTP service for sending global DayZ chat messages through BattlEye RCON.

The service sends this command to DayZ:

`say -1 <message>`

## 1. Configure

Project now runs two containers (one per DayZ server):

```env
SERVER1_NAME=dayz-eu-main
SERVER1_HTTP_PORT=8081
SERVER1_API_KEY=change_me_server1
SERVER1_RCON_HOST=95.165.73.39
SERVER1_RCON_PORT=2305
SERVER1_RCON_PASSWORD=your_rcon_password_1

SERVER2_NAME=dayz-us-main
SERVER2_HTTP_PORT=8082
SERVER2_API_KEY=change_me_server2
SERVER2_RCON_HOST=203.0.113.42
SERVER2_RCON_PORT=2305
SERVER2_RCON_PASSWORD=your_rcon_password_2
```

All variables are listed in `.env.example`.

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

Returns HTTP and RCON status + instance name.

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

Examples:

```bash
curl -X POST "http://127.0.0.1:8081/broadcast" \
  -H "Content-Type: application/json" \
  -H "x-api-key: change_me_server1" \
  -d "{\"message\":\"Restart in 10 minutes\"}"
```

```bash
curl -X POST "http://127.0.0.1:8082/broadcast" \
  -H "Content-Type: application/json" \
  -H "x-api-key: change_me_server2" \
  -d "{\"message\":\"Restart in 10 minutes\"}"
```
