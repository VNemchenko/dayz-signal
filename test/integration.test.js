const test = require("node:test");
const assert = require("node:assert/strict");
const dgram = require("node:dgram");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { BroadcastService } = require("../src/broadcastService");
const { createHttpHandler } = require("../src/httpApp");
const { JsonlJournal } = require("../src/journal");
const { DayzRconClient, buildPacket, parsePacket } = require("../src/rconClient");

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

function httpJson(port, method, route, apiKey, body) {
  return new Promise((resolve, reject) => {
    const data = body === undefined ? "" : JSON.stringify(body);
    const req = http.request({
      host: "127.0.0.1",
      port,
      method,
      path: route,
      headers: {
        authorization: `Bearer ${apiKey}`,
        ...(body && body.command_id ? { "idempotency-key": body.command_id } : {}),
        ...(data ? { "content-type": "application/json", "content-length": Buffer.byteLength(data) } : {}),
      },
    }, (res) => {
      const chunks = [];
      res.on("data", (chunk) => chunks.push(chunk));
      res.on("end", () => resolve({
        status: res.statusCode,
        body: JSON.parse(Buffer.concat(chunks).toString("utf8")),
      }));
    });
    req.on("error", reject);
    if (data) {
      req.write(data);
    }
    req.end();
  });
}

test("HTTP to journal to fake UDP RCON works end to end", async (t) => {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), "dayz-signal-e2e-"));
  const udp = dgram.createSocket("udp4");
  const receivedCommands = [];
  udp.on("message", (message, rinfo) => {
    const packet = parsePacket(message);
    if (packet.type === 0) {
      udp.send(buildPacket(0, Buffer.from([1])), rinfo.port, rinfo.address);
    } else if (packet.type === 1) {
      if (packet.data.length > 0) {
        receivedCommands.push(packet.data.toString("ascii"));
      }
      udp.send(buildPacket(1, Buffer.from([packet.sequence])), rinfo.port, rinfo.address);
    }
  });
  await new Promise((resolve) => udp.bind(0, "127.0.0.1", resolve));

  const rcon = new DayzRconClient({
    host: "127.0.0.1",
    port: udp.address().port,
    password: "test-password",
    connectionType: "udp4",
    connectionTimeoutMs: 250,
    connectionIntervalMs: 25,
    keepAliveMs: 100,
    commandTimeoutMs: 100,
    reconnectBaseMs: 20,
    reconnectMaxMs: 40,
  }, { random: () => 0.5 });
  const journal = new JsonlJournal(path.join(directory, "broadcasts.jsonl"));
  const broadcasts = new BroadcastService({
    rcon,
    journal,
    config: {
      queueCapacity: 10,
      defaultTtlSeconds: 30,
      maxTtlSeconds: 300,
      maxFutureSkewSeconds: 30,
      maxMessageLength: 160,
      ratePerMinute: 60,
      rateBurst: 5,
    },
  });
  await broadcasts.init();
  await rcon.init();
  await waitFor(() => rcon.status().connected);

  const apiKey = "e".repeat(32);
  const config = {
    serviceName: "e2e",
    serverId: "livonia-1",
    apiKey,
    httpBodyLimitBytes: 4096,
    httpWaitMs: 200,
    broadcasts: { maxTtlSeconds: 300 },
  };
  const server = http.createServer(createHttpHandler({ config, broadcasts, rcon }));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    if (!broadcasts.closed) {
      await broadcasts.close(0);
    }
    await new Promise((resolve) => udp.close(resolve));
    await fs.promises.rm(directory, { recursive: true, force: true });
  });

  const createdAt = Date.now();
  const commandId = "cmd_world-event-e2e-001";
  const posted = await httpJson(server.address().port, "POST", "/v1/broadcasts", apiKey, {
    schema: "dayz.command.v1",
    command_id: commandId,
    server_id: "livonia-1",
    created_at: new Date(createdAt).toISOString(),
    expires_at: new Date(createdAt + 30000).toISOString(),
    channel: "global",
    message: "Внимание: через 10 минут рестарт",
    metadata: { event_id: "evt_world-event-e2e-001", policy_version: "v1" },
  });
  assert.ok(posted.status === 200 || posted.status === 202);

  await waitFor(() => broadcasts.get(commandId).state === "acknowledged");
  const status = await httpJson(server.address().port, "GET", `/v1/broadcasts/${commandId}`, apiKey);
  assert.equal(status.status, 200);
  assert.equal(status.body.sent_message, "Vnimanie: cherez 10 minut restart");
  assert.deepEqual(receivedCommands, ["say -1 Vnimanie: cherez 10 minut restart"]);

  const journalStates = (await fs.promises.readFile(path.join(directory, "broadcasts.jsonl"), "utf8"))
    .trim().split("\n").map(JSON.parse).map((record) => record.state);
  assert.deepEqual(journalStates, ["queued", "sending", "acknowledged"]);

  const futureCreatedAt = Date.now() + 31000;
  const future = await httpJson(server.address().port, "POST", "/v1/broadcasts", apiKey, {
    schema: "dayz.command.v1",
    command_id: "cmd_world-event-e2e-future",
    server_id: "livonia-1",
    created_at: new Date(futureCreatedAt).toISOString(),
    expires_at: new Date(futureCreatedAt + 1000).toISOString(),
    channel: "global",
    message: "Test",
    metadata: { event_id: "evt_world-event-e2e-future", policy_version: "v1" },
  });
  assert.equal(future.status, 400);
  assert.equal(future.body.error_code, "created_at_in_future");
  assert.equal(receivedCommands.length, 1);
});
