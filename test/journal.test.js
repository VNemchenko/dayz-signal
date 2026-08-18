const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { JsonlJournal } = require("../src/journal");

async function temporaryJournal(t) {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dayz-signal-journal-"));
  t.after(() => fs.promises.rm(directory, { recursive: true, force: true }));
  return path.join(directory, "broadcasts.jsonl");
}

function record(requestId, state, now, overrides = {}) {
  return {
    v: 1,
    requestId,
    apiVersion: "legacy",
    payloadHash: "a".repeat(64),
    sentMessage: "Test",
    ttlSeconds: 30,
    createdAt: now,
    acceptedAt: now,
    expiresAt: now + 30000,
    updatedAt: now,
    state,
    response: null,
    errorCode: null,
    errorMessage: null,
    redactedAt: null,
    ...overrides,
  };
}

test("persists strict snapshots and replays only the latest state", async (t) => {
  const fileName = await temporaryJournal(t);
  const now = Date.now();
  const first = new JsonlJournal(fileName);
  await first.init(now);
  await first.append(record("request-001", "queued", now));
  await first.append(record("request-001", "acknowledged", now, { updatedAt: now + 100, response: "ok" }));
  await first.close();

  const lines = (await fs.promises.readFile(fileName, "utf8")).trim().split("\n").map(JSON.parse);
  assert.equal(lines.length, 1);
  assert.equal(lines[0].state, "acknowledged");

  const second = new JsonlJournal(fileName);
  await second.init(now + 200);
  assert.equal(second.get("request-001").state, "acknowledged");
  assert.equal(second.get("request-001").response, "ok");
  assert.equal(second.health().ok, true);
  await second.close();
});

test("recovers durable sending marker as delivery_unknown without resend", async (t) => {
  const fileName = await temporaryJournal(t);
  const first = new JsonlJournal(fileName);
  await first.init(1000);
  await first.append(record("request-002", "sending", 1000));
  await first.close();

  const second = new JsonlJournal(fileName);
  await second.init(2000);
  const recovered = second.get("request-002");
  assert.equal(recovered.state, "delivery_unknown");
  assert.equal(recovered.errorCode, "recovered_after_sending");
  await second.close();

  const lines = (await fs.promises.readFile(fileName, "utf8")).trim().split("\n").map(JSON.parse);
  assert.deepEqual(lines.map((line) => line.state), ["delivery_unknown"]);
});

test("expires queued entries during recovery", async (t) => {
  const fileName = await temporaryJournal(t);
  const first = new JsonlJournal(fileName);
  await first.init(1000);
  await first.append(record("request-003", "queued", 1000, { ttlSeconds: 1, expiresAt: 2000 }));
  await first.close();

  const second = new JsonlJournal(fileName);
  await second.init(2001);
  assert.equal(second.get("request-003").state, "expired");
  await second.close();
});

test("ignores only a torn final JSONL line", async (t) => {
  const fileName = await temporaryJournal(t);
  const valid = JSON.stringify(record("request-004", "queued", 1000));
  await fs.promises.writeFile(fileName, `${valid}\n{"v":1`, "utf8");
  const journal = new JsonlJournal(fileName);
  await journal.init(1100);
  assert.equal(journal.get("request-004").state, "queued");
  await journal.append(record("request-004b", "queued", 1100));
  await journal.close();

  const reopened = new JsonlJournal(fileName);
  await reopened.init(1200);
  assert.equal(reopened.get("request-004b").state, "queued");
  await reopened.close();
});

test("preserves a complete final record that only lacks a newline", async (t) => {
  const fileName = await temporaryJournal(t);
  await fs.promises.writeFile(fileName, JSON.stringify(record("request-004c", "queued", 1000)), "utf8");
  const journal = new JsonlJournal(fileName);
  await journal.init(1100);
  await journal.append(record("request-004d", "queued", 1100));
  await journal.close();

  const reopened = new JsonlJournal(fileName);
  await reopened.init(1200);
  assert.equal(reopened.get("request-004c").state, "queued");
  assert.equal(reopened.get("request-004d").state, "queued");
  await reopened.close();
});

test("fails closed on corruption or a schema-invalid complete tail", async (t) => {
  const fileName = await temporaryJournal(t);
  const valid = JSON.stringify(record("request-005", "queued", 1000));
  await fs.promises.writeFile(fileName, `${valid}\nnot-json\n${valid}\n`, "utf8");
  const journal = new JsonlJournal(fileName);
  await assert.rejects(journal.init(1100), /Corrupt broadcast journal at line 2/);

  const invalidTail = await temporaryJournal(t);
  await fs.promises.writeFile(invalidTail, `${valid}\n${JSON.stringify({ v: 1 })}`, "utf8");
  const second = new JsonlJournal(invalidTail);
  await assert.rejects(second.init(1100), /Corrupt broadcast journal at line 2.*missing fields/);
});

test("redacts expired text, compacts atomically and bounds retained records", async (t) => {
  const fileName = await temporaryJournal(t);
  const journal = new JsonlJournal(fileName, { retentionMs: 1000, maxRecords: 2 });
  await journal.init(1000);
  await journal.append(record("request-old-001", "acknowledged", 1000, { response: "private response" }));
  await journal.append(record("request-old-002", "failed", 1100, {
    errorCode: "rcon_failed",
    errorMessage: "private failure detail",
  }));
  await journal.append(record("request-old-003", "acknowledged", 1200, { response: "newest" }));
  await journal.maintain(5000, true);

  const health = journal.health();
  assert.equal(health.ok, true);
  assert.equal(health.record_count, 2);
  assert.equal(health.redacted_count, 2);
  assert.ok(health.last_compaction_at);
  const retained = journal.all();
  assert.deepEqual(retained.map((item) => item.requestId).sort(), ["request-old-002", "request-old-003"]);
  assert.ok(retained.every((item) => item.sentMessage === null && item.response === null && item.errorMessage === null));
  await journal.close();

  const text = await fs.promises.readFile(fileName, "utf8");
  assert.equal(text.includes("private"), false);
  assert.equal(text.trim().split("\n").length, 2);
  assert.equal(await fs.promises.stat(`${fileName}.compact-tmp`).catch((error) => error.code), "ENOENT");
  assert.equal(await fs.promises.stat(`${fileName}.compact-backup`).catch((error) => error.code), "ENOENT");
});

test("strict validation rejects unknown fields and malformed hashes", async (t) => {
  const fileName = await temporaryJournal(t);
  const journal = new JsonlJournal(fileName);
  await journal.init(1000);
  await assert.rejects(journal.append({ ...record("request-bad-001", "queued", 1000), extra: true }), /unknown fields/);
  await assert.rejects(journal.append({ ...record("request-bad-002", "queued", 1000), payloadHash: "hash" }), /SHA-256/);
  await assert.rejects(
    journal.append({ ...record("request-bad-003", "queued", 1000), apiVersion: "v1" }),
    /must start with cmd_/,
  );
  assert.equal(journal.health().last_error_code, null);
  await journal.close();
});

test("fails closed before the journal can exceed its byte limit", async (t) => {
  const fileName = await temporaryJournal(t);
  const journal = new JsonlJournal(fileName, { maxBytes: 1024 });
  await journal.init(1000);
  await assert.rejects(
    journal.append(record("request-large-001", "acknowledged", 1000, { response: "x".repeat(2000) })),
    /byte limit/,
  );
  assert.equal(journal.health().ok, false);
  assert.equal((await fs.promises.stat(fileName)).size, 0);
  await journal.close();
});
