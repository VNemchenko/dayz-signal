const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { createHttpHandler } = require("../src/httpApp");

const API_KEY = "k".repeat(32);
const N8N_COMMAND = JSON.parse(fs.readFileSync(path.join(__dirname, "fixtures", "n8n-command.json"), "utf8"));

function v1Command(overrides = {}) {
  return {
    ...JSON.parse(JSON.stringify(N8N_COMMAND)),
    ...overrides,
  };
}

function makeRecord(requestId, state = "queued", apiVersion = "v1") {
  return {
    requestId,
    apiVersion,
    state,
    sentMessage: "Server budet perezapushchen",
    createdAt: Date.parse("2026-08-17T12:00:00Z"),
    acceptedAt: Date.parse("2026-08-17T12:00:01Z"),
    expiresAt: Date.parse("2026-08-17T12:05:00Z"),
    updatedAt: Date.parse("2026-08-17T12:00:01Z"),
    redactedAt: null,
    response: state === "acknowledged" ? "accepted" : null,
    errorCode: state === "delivery_unknown" ? "command_timeout" : null,
    errorMessage: state === "delivery_unknown" ? "timeout" : null,
  };
}

async function fixture(t, options = {}) {
  const records = new Map();
  const broadcasts = {
    saturated: false,
    queueDepth: () => options.queueDepth || 0,
    isSaturated() { return this.saturated; },
    health: () => ({ ok: options.journalOk !== false, writable: true, record_count: records.size }),
    get(requestId) { return records.get(requestId) || null; },
    async submit(input) {
      const requestId = input.requestId || "generated-id-0001";
      const record = makeRecord(requestId, options.submitState || "queued", input.apiVersion || "legacy");
      records.set(requestId, record);
      this.lastInput = input;
      return { record, created: options.created !== false };
    },
    async waitForTerminal(requestId) {
      const record = makeRecord(requestId, options.legacyFinalState || "acknowledged", "legacy");
      records.set(requestId, record);
      return record;
    },
  };
  const rcon = {
    status: () => ({ state: options.connected === false ? "backoff" : "connected", connected: options.connected !== false, lastErrorCode: null }),
  };
  const config = {
    serviceName: "test-signal",
    serverId: "livonia-1",
    apiKey: API_KEY,
    httpBodyLimitBytes: 1024,
    httpWaitMs: 100,
    broadcasts: { maxTtlSeconds: 300 },
  };
  const server = http.createServer(createHttpHandler({
    config,
    broadcasts,
    rcon,
    isShuttingDown: () => Boolean(options.shuttingDown),
  }));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = server.address().port;
  t.after(() => new Promise((resolve) => server.close(resolve)));
  return { port, broadcasts, records };
}

function request(port, method, path, options = {}) {
  return new Promise((resolve, reject) => {
    const body = options.rawBody !== undefined
      ? options.rawBody
      : options.body === undefined ? "" : JSON.stringify(options.body);
    const headers = { ...(options.headers || {}) };
    if (body && !headers["content-type"]) {
      headers["content-type"] = "application/json";
    }
    if (body) {
      headers["content-length"] = Buffer.byteLength(body);
    }
    const req = http.request({ host: "127.0.0.1", port, method, path, headers }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => {
        const text = Buffer.concat(chunks).toString("utf8");
        resolve({ status: res.statusCode, headers: res.headers, body: text ? JSON.parse(text) : null });
      });
    });
    req.on("error", reject);
    if (body) {
      req.write(body);
    }
    req.end();
  });
}

function authHeaders(extra = {}) {
  return { authorization: `Bearer ${API_KEY}`, ...extra };
}

function commandHeaders(commandId = N8N_COMMAND.command_id, extra = {}) {
  return authHeaders({ "idempotency-key": commandId, ...extra });
}

test("exposes safe health and RCON/journal-aware readiness without authentication", async (t) => {
  const ready = await fixture(t);
  assert.equal((await request(ready.port, "GET", "/livez")).status, 200);
  const health = await request(ready.port, "GET", "/health");
  assert.equal(health.status, 200);
  assert.equal(health.body.journal.ok, true);
  assert.equal((await request(ready.port, "GET", "/readyz")).status, 200);

  const offline = await fixture(t, { connected: false });
  assert.equal((await request(offline.port, "GET", "/readyz")).status, 503);
  const badJournal = await fixture(t, { journalOk: false });
  assert.equal((await request(badJournal.port, "GET", "/readyz")).status, 503);
});

test("fails closed for missing or conflicting credentials", async (t) => {
  const { port } = await fixture(t);
  const body = v1Command();
  assert.equal((await request(port, "POST", "/v1/broadcasts", { body })).status, 401);
  const conflict = await request(port, "POST", "/v1/broadcasts", {
    headers: { "x-api-key": API_KEY, authorization: `Bearer ${"z".repeat(32)}`, "idempotency-key": body.command_id },
    body,
  });
  assert.equal(conflict.status, 401);
});

test("accepts the exact n8n dayz.command.v1 envelope and queries it with GET 200", async (t) => {
  const { port, broadcasts } = await fixture(t);
  const body = v1Command();
  const created = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders(body.command_id),
    body,
  });
  assert.equal(created.status, 202);
  assert.equal(created.headers.location, `/v1/broadcasts/${body.command_id}`);
  assert.equal(created.body.command_id, body.command_id);
  assert.equal(created.body.request_id, body.command_id);
  assert.equal(broadcasts.lastInput.requestId, body.command_id);
  assert.equal(broadcasts.lastInput.message, body.message);
  assert.equal(broadcasts.lastInput.createdAt, Date.parse(body.created_at));
  assert.equal(broadcasts.lastInput.expiresAt, Date.parse(body.expires_at));
  assert.equal(broadcasts.lastInput.apiVersion, "v1");
  assert.deepEqual(broadcasts.lastInput.idempotencyPayload, body);

  const status = await request(port, "GET", `/v1/broadcasts/${body.command_id}`, { headers: authHeaders() });
  assert.equal(status.status, 200);
  assert.equal(status.body.status, "queued");
  assert.equal(status.body.command_id, body.command_id);
});

test("legacy route waits for the RCON result and preserves old response fields", async (t) => {
  const { port } = await fixture(t, { legacyFinalState: "acknowledged" });
  const response = await request(port, "POST", "/broadcast", {
    headers: { "x-api-key": API_KEY },
    body: { message: "Test" },
  });
  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.command, "say -1 <message>");
  assert.equal(response.body.response, "accepted");
});

test("v1 POST remains 202 for an idempotent terminal state while GET reports it with 200", async (t) => {
  const { port } = await fixture(t, { submitState: "delivery_unknown", created: false });
  const body = v1Command({ command_id: "cmd_delivery-http-0002" });
  const response = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders(body.command_id),
    body,
  });
  assert.equal(response.status, 202);
  assert.equal(response.headers.location, `/v1/broadcasts/${body.command_id}`);
  assert.equal(response.body.error_code, "command_timeout");
  const status = await request(port, "GET", `/v1/broadcasts/${body.command_id}`, { headers: authHeaders() });
  assert.equal(status.status, 200);
  assert.equal(status.body.status, "delivery_unknown");
});

test("rejects incomplete, misaddressed or unsupported v1 commands", async (t) => {
  const { port } = await fixture(t);
  const cases = [
    { body: v1Command({ schema: "dayz.command.v2" }), code: "invalid_schema" },
    { body: v1Command({ server_id: "other-1" }), code: "wrong_server" },
    { body: v1Command({ channel: "direct" }), code: "invalid_channel" },
    { body: v1Command({ created_at: "2026-08-17T12:00:00+00:00" }), code: "invalid_timestamp" },
    { body: v1Command({ created_at: "2026-02-30T12:00:00Z" }), code: "invalid_timestamp" },
    { body: v1Command({ expires_at: "2026-08-17T12:05:01Z" }), code: "invalid_ttl" },
    { body: v1Command({ metadata: { event_id: "evt_1", policy_version: "v1", extra: "no" } }), code: "unknown_fields" },
    { body: { ...v1Command(), metadata: undefined }, code: "missing_fields" },
    { body: { ...v1Command(), extra: true }, code: "unknown_fields" },
  ];
  for (const item of cases) {
    const response = await request(port, "POST", "/v1/broadcasts", {
      headers: commandHeaders(item.body.command_id),
      body: item.body,
    });
    assert.equal(response.status, item.code === "wrong_server" ? 409 : 400);
    assert.equal(response.body.error_code, item.code);
  }

  const missingHeader = await request(port, "POST", "/v1/broadcasts", {
    headers: authHeaders(),
    body: v1Command(),
  });
  assert.equal(missingHeader.body.error_code, "missing_idempotency_key");
  const mismatch = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders("cmd_delivery-http-wrong"),
    body: v1Command(),
  });
  assert.equal(mismatch.body.error_code, "command_id_mismatch");
});

test("validates JSON media type and body size", async (t) => {
  const { port } = await fixture(t);
  const invalidJson = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders(),
    rawBody: "{",
  });
  assert.equal(invalidJson.status, 400);

  const wrongType = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders(undefined, { "content-type": "text/plain" }),
    rawBody: "{}",
  });
  assert.equal(wrongType.status, 415);

  const tooLarge = await request(port, "POST", "/v1/broadcasts", {
    headers: commandHeaders(),
    body: v1Command({ message: "x".repeat(2000) }),
  });
  assert.equal(tooLarge.status, 413);
});

test("returns 404 for missing broadcast and unknown route", async (t) => {
  const { port } = await fixture(t);
  assert.equal((await request(port, "GET", "/v1/broadcasts/missing-id", { headers: authHeaders() })).status, 404);
  assert.equal((await request(port, "GET", "/unknown", { headers: authHeaders() })).status, 404);
});
