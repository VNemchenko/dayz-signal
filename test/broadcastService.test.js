const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const EventEmitter = require("node:events");
const { BroadcastService } = require("../src/broadcastService");
const { RconError } = require("../src/errors");
const { JsonlJournal } = require("../src/journal");

class FakeRcon extends EventEmitter {
  constructor(connected = false) {
    super();
    this.connected = connected;
    this.calls = [];
    this.behavior = async () => "";
  }

  status() {
    return { state: this.connected ? "connected" : "backoff", connected: this.connected, lastErrorCode: null };
  }

  setConnected(value) {
    this.connected = value;
    this.emit("state", this.status());
  }

  async sendGlobalMessage(message) {
    this.calls.push(message);
    return this.behavior(message);
  }

  close() {
    this.connected = false;
    this.emit("state", this.status());
  }
}

function waitFor(predicate, timeoutMs = 1000) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      if (predicate()) {
        resolve();
      } else if (Date.now() - started >= timeoutMs) {
        reject(new Error("Condition timed out"));
      } else {
        setTimeout(check, 5);
      }
    };
    check();
  });
}

async function serviceFixture(t, options = {}) {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dayz-signal-service-"));
  const fileName = path.join(directory, "broadcasts.jsonl");
  const rcon = options.rcon || new FakeRcon(Boolean(options.connected));
  const journal = new JsonlJournal(fileName, { retentionMs: 86400000, maxRecords: 10000 });
  const config = {
    queueCapacity: 50,
    defaultTtlSeconds: 30,
    maxTtlSeconds: 300,
    maxMessageLength: 160,
    ratePerMinute: 60000,
    rateBurst: 100,
    ...options.config,
  };
  const service = new BroadcastService({
    rcon,
    journal,
    config,
    now: options.now,
    setTimeout: options.setTimeout,
    clearTimeout: options.clearTimeout,
  });
  await service.init();
  t.after(async () => {
    if (!service.closed) {
      await service.close(0);
    }
    await fs.promises.rm(directory, { recursive: true, force: true });
  });
  return { service, rcon, journal, fileName };
}

test("queues while disconnected and enforces idempotency", async (t) => {
  const { service, rcon } = await serviceFixture(t);
  const first = await service.submit({ requestId: "event-0001", message: "Привет", ttlSeconds: 30 });
  assert.equal(first.record.state, "queued");
  assert.equal(first.record.sentMessage, "Privet");
  assert.equal(service.queueDepth(), 1);
  assert.deepEqual(rcon.calls, []);

  const replay = await service.submit({ requestId: "event-0001", message: "Привет", ttlSeconds: 30 });
  assert.equal(replay.created, false);
  assert.equal(service.queueDepth(), 1);
  await assert.rejects(
    service.submit({ requestId: "event-0001", message: "Другой текст", ttlSeconds: 30 }),
    (error) => error.statusCode === 409 && error.code === "idempotency_conflict",
  );
});

test("uses the full v1 envelope hash and preserves absolute expiry", async (t) => {
  let now = Date.parse("2026-08-17T12:00:00Z");
  const { service } = await serviceFixture(t, { now: () => now });
  const envelope = {
    schema: "dayz.command.v1",
    command_id: "cmd_envelope-0001",
    server_id: "livonia-1",
    created_at: new Date(now).toISOString(),
    expires_at: new Date(now + 300000).toISOString(),
    channel: "global",
    message: "Restart soon",
    metadata: { event_id: "evt_envelope-0001", policy_version: "v1" },
  };
  const input = {
    requestId: envelope.command_id,
    message: envelope.message,
    createdAt: now,
    expiresAt: now + 300000,
    apiVersion: "v1",
    idempotencyPayload: envelope,
  };
  const first = await service.submit(input);
  assert.equal(first.record.createdAt, now);
  assert.equal(first.record.expiresAt, now + 300000);

  const reordered = {
    ...input,
    idempotencyPayload: {
      metadata: { policy_version: "v1", event_id: "evt_envelope-0001" },
      message: "Restart soon",
      channel: "global",
      expires_at: envelope.expires_at,
      created_at: envelope.created_at,
      server_id: "livonia-1",
      command_id: envelope.command_id,
      schema: "dayz.command.v1",
    },
  };
  assert.equal((await service.submit(reordered)).created, false);

  await assert.rejects(
    service.submit({
      ...input,
      idempotencyPayload: {
        ...envelope,
        metadata: { ...envelope.metadata, policy_version: "v2" },
      },
    }),
    (error) => error.statusCode === 409 && error.code === "idempotency_conflict",
  );

  now += 301000;
  const expiredEnvelope = {
    ...envelope,
    command_id: "cmd_envelope-0002",
  };
  const expired = await service.submit({
    requestId: expiredEnvelope.command_id,
    message: expiredEnvelope.message,
    createdAt: Date.parse(expiredEnvelope.created_at),
    expiresAt: Date.parse(expiredEnvelope.expires_at),
    apiVersion: "v1",
    idempotencyPayload: expiredEnvelope,
  });
  assert.equal(expired.record.state, "expired");
  assert.equal(expired.record.errorCode, "command_expired");
});

test("rejects a command whose created_at is beyond the allowed future skew", async (t) => {
  const now = Date.parse("2026-08-17T12:00:00Z");
  const { service } = await serviceFixture(t, {
    now: () => now,
    config: { maxFutureSkewSeconds: 30 },
  });
  const createdAt = now + 30001;
  const envelope = {
    schema: "dayz.command.v1",
    command_id: "cmd_future-envelope-0001",
    server_id: "livonia-1",
    created_at: new Date(createdAt).toISOString(),
    expires_at: new Date(createdAt + 1000).toISOString(),
    channel: "global",
    message: "Test",
    metadata: { event_id: "evt_future-envelope-0001", policy_version: "v1" },
  };
  await assert.rejects(
    service.submit({
      requestId: envelope.command_id,
      message: envelope.message,
      createdAt,
      expiresAt: createdAt + 1000,
      apiVersion: "v1",
      idempotencyPayload: envelope,
    }),
    (error) => error.statusCode === 400 && error.code === "created_at_in_future",
  );
});

test("writes and fsyncs sending before invoking UDP transport", async (t) => {
  const { service, rcon, fileName } = await serviceFixture(t, { connected: true });
  rcon.behavior = async () => {
    const lines = (await fs.promises.readFile(fileName, "utf8")).trim().split("\n").map(JSON.parse);
    assert.equal(lines.at(-1).state, "sending");
    return "accepted";
  };
  const submitted = await service.submit({ requestId: "event-0002", message: "Сервер работает", ttlSeconds: 30 });
  const final = await service.waitForTerminal(submitted.record.requestId, 1000);
  assert.equal(final.state, "acknowledged");
  assert.equal(final.response, "accepted");
  assert.deepEqual(rcon.calls, ["Server rabotaet"]);

  const states = (await fs.promises.readFile(fileName, "utf8")).trim().split("\n").map(JSON.parse).map((line) => line.state);
  assert.deepEqual(states, ["queued", "sending", "acknowledged"]);
});

test("does not retry a delivery-unknown UDP command", async (t) => {
  const { service, rcon } = await serviceFixture(t, { connected: true });
  rcon.behavior = async () => {
    throw new RconError("command_timeout", "timeout", { deliveryUnknown: true });
  };
  const submitted = await service.submit({ requestId: "event-0003", message: "Test", ttlSeconds: 30 });
  const final = await service.waitForTerminal(submitted.record.requestId, 1000);
  assert.equal(final.state, "delivery_unknown");
  assert.equal(rcon.calls.length, 1);
  rcon.setConnected(false);
  rcon.setConnected(true);
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(rcon.calls.length, 1);
});

test("requeues a command known not to have reached UDP", async (t) => {
  const { service, rcon } = await serviceFixture(t, { connected: true });
  let attempts = 0;
  rcon.behavior = async () => {
    attempts += 1;
    if (attempts === 1) {
      rcon.connected = false;
      throw new RconError("not_connected", "not connected", { safeToRetry: true });
    }
    return "";
  };
  const submitted = await service.submit({ requestId: "event-0004", message: "Test", ttlSeconds: 30 });
  await waitFor(() => attempts === 1 && service.get(submitted.record.requestId).state === "queued");
  assert.equal(service.queueDepth(), 1);
  rcon.setConnected(true);
  const final = await service.waitForTerminal(submitted.record.requestId, 1000);
  assert.equal(final.state, "acknowledged");
  assert.equal(attempts, 2);
});

test("expires queued messages without an RCON connection", async (t) => {
  let now = 1000;
  const timer = { unref() {} };
  const { service } = await serviceFixture(t, {
    now: () => now,
    setTimeout: () => timer,
    clearTimeout: () => {},
  });
  await service.submit({ requestId: "event-0005", message: "Test", ttlSeconds: 1 });
  now = 2001;
  await service.pump();
  assert.equal(service.get("event-0005").state, "expired");
  assert.equal(service.queueDepth(), 0);
});

test("enforces bounded FIFO queue and rate limit", async (t) => {
  const queueFixture = await serviceFixture(t, { config: { queueCapacity: 1 } });
  await queueFixture.service.submit({ requestId: "event-0006", message: "One" });
  await assert.rejects(
    queueFixture.service.submit({ requestId: "event-0007", message: "Two" }),
    (error) => error.statusCode === 503 && error.code === "queue_full",
  );

  const rateFixture = await serviceFixture(t, { config: { ratePerMinute: 1, rateBurst: 1 } });
  await rateFixture.service.submit({ requestId: "event-0008", message: "One" });
  await assert.rejects(
    rateFixture.service.submit({ requestId: "event-0009", message: "Two" }),
    (error) => error.statusCode === 429 && error.retryAfter >= 1,
  );
});

test("rejects invalid IDs, empty transliteration and expanded overlength", async (t) => {
  const { service } = await serviceFixture(t);
  await assert.rejects(service.submit({ requestId: "bad", message: "Test" }), /request_id/);
  await assert.rejects(service.submit({ requestId: "event-0010", message: "🌧️" }), (error) => error.statusCode === 422);
  await assert.rejects(
    service.submit({ requestId: "event-0011", message: "щ".repeat(41) }),
    (error) => error.code === "message_too_long",
  );
});

test("restores a durable queued broadcast and sends it after restart", async (t) => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dayz-signal-restart-"));
  const fileName = path.join(directory, "broadcasts.jsonl");
  t.after(() => fs.promises.rm(directory, { recursive: true, force: true }));
  const config = {
    queueCapacity: 10,
    defaultTtlSeconds: 30,
    maxTtlSeconds: 300,
    maxMessageLength: 160,
    ratePerMinute: 60,
    rateBurst: 5,
  };

  const firstRcon = new FakeRcon(false);
  const first = new BroadcastService({
    rcon: firstRcon,
    journal: new JsonlJournal(fileName),
    config,
  });
  await first.init();
  await first.submit({ requestId: "event-restart-001", message: "После рестарта" });
  await first.close(0);

  const secondRcon = new FakeRcon(true);
  const second = new BroadcastService({
    rcon: secondRcon,
    journal: new JsonlJournal(fileName),
    config,
  });
  await second.init();
  await waitFor(() => second.get("event-restart-001").state === "acknowledged");
  assert.deepEqual(secondRcon.calls, ["Posle restarta"]);
  await second.close(0);
});
