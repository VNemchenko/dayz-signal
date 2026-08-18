const test = require("node:test");
const assert = require("node:assert/strict");
const dgram = require("node:dgram");
const EventEmitter = require("node:events");
const {
  DayzRconClient,
  buildPacket,
  parsePacket,
} = require("../src/rconClient");

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

class FakeRconServer extends EventEmitter {
  constructor() {
    super();
    this.socket = dgram.createSocket("udp4");
    this.port = 0;
    this.client = null;
    this.loginCount = 0;
    this.commands = [];
    this.acks = [];
    this.autoLogin = true;
    this.onCommand = null;
    this.socket.on("message", (message, rinfo) => this.handle(message, rinfo));
  }

  async start() {
    await new Promise((resolve) => this.socket.bind(0, "127.0.0.1", resolve));
    this.port = this.socket.address().port;
  }

  handle(message, rinfo) {
    const packet = parsePacket(message);
    if (!packet) {
      return;
    }
    this.client = rinfo;
    if (packet.type === 0) {
      this.loginCount += 1;
      this.emit("login");
      if (this.autoLogin) {
        this.send(buildPacket(0, Buffer.from([1])));
      }
    } else if (packet.type === 1) {
      const item = { sequence: packet.sequence, command: packet.data.toString("ascii") };
      this.commands.push(item);
      this.emit("command", item);
      if (this.onCommand) {
        this.onCommand(item);
      }
    } else if (packet.type === 2) {
      this.acks.push(packet.sequence);
      this.emit("ack", packet.sequence);
    }
  }

  send(packet) {
    if (!this.client) {
      throw new Error("No RCON client address captured");
    }
    this.socket.send(packet, this.client.port, this.client.address);
  }

  respond(sequence, response = "") {
    this.send(buildPacket(1, Buffer.concat([Buffer.from([sequence]), Buffer.from(response, "ascii")])));
  }

  close() {
    return new Promise((resolve) => this.socket.close(resolve));
  }
}

function clientConfig(port, overrides = {}) {
  return {
    host: "127.0.0.1",
    port,
    password: "test-password",
    connectionType: "udp4",
    connectionTimeoutMs: 250,
    connectionIntervalMs: 25,
    keepAliveMs: 100,
    commandTimeoutMs: 100,
    reconnectBaseMs: 20,
    reconnectMaxMs: 40,
    ...overrides,
  };
}

async function connectedPair(t, overrides = {}) {
  const server = new FakeRconServer();
  await server.start();
  const client = new DayzRconClient(clientConfig(server.port, overrides), { random: () => 0.5 });
  t.after(async () => {
    client.close();
    await server.close();
  });
  await client.init();
  await waitFor(() => client.status().connected);
  return { client, server };
}

test("packet codec validates prefix, CRC and packet shape", () => {
  const packet = buildPacket(1, Buffer.from([7, 65, 66]));
  assert.deepEqual(parsePacket(packet), { type: 1, sequence: 7, data: Buffer.from("AB") });
  const corrupt = Buffer.from(packet);
  corrupt[corrupt.length - 1] ^= 0xff;
  assert.equal(parsePacket(corrupt), null);
  assert.equal(parsePacket(Buffer.from("BE")), null);
});

test("logs in and correlates a single command response", async (t) => {
  const { client, server } = await connectedPair(t);
  server.onCommand = (item) => server.respond(item.sequence, "accepted");
  const response = await client.sendGlobalMessage("Restart in 10 minutes");
  assert.equal(response, "accepted");
  assert.equal(server.commands[0].command, "say -1 Restart in 10 minutes");
});

test("assembles multipart command responses out of order", async (t) => {
  const { client, server } = await connectedPair(t);
  server.onCommand = (item) => {
    const part1 = buildPacket(1, Buffer.concat([Buffer.from([item.sequence, 0, 2, 1]), Buffer.from("world")]));
    const part0 = buildPacket(1, Buffer.concat([Buffer.from([item.sequence, 0, 2, 0]), Buffer.from("hello ")]));
    server.send(part1);
    server.send(part0);
  };
  assert.equal(await client.sendGlobalMessage("test"), "hello world");
});

test("ACKs code 2 but never treats it as command delivery", async (t) => {
  const { client, server } = await connectedPair(t);
  let command;
  server.onCommand = (item) => { command = item; };
  let settled = false;
  const pending = client.sendGlobalMessage("test").finally(() => { settled = true; });
  await waitFor(() => Boolean(command));

  server.send(buildPacket(2, Buffer.concat([Buffer.from([91]), Buffer.from("console message")])));
  await waitFor(() => server.acks.includes(91));
  assert.equal(settled, false);

  server.respond(command.sequence, "");
  assert.equal(await pending, "");
  assert.equal(client.status().serverMessages, 1);
});

test("rejects invalid source and invalid CRC packets", async (t) => {
  const { client, server } = await connectedPair(t);
  const before = client.status().invalidPackets;
  const corrupt = buildPacket(2, Buffer.from([2, 65]));
  corrupt[corrupt.length - 1] ^= 1;
  server.send(corrupt);

  const stranger = dgram.createSocket("udp4");
  t.after(() => stranger.close());
  stranger.send(buildPacket(2, Buffer.from([3, 66])), server.client.port, server.client.address);
  await waitFor(() => client.status().invalidPackets >= before + 2);
});

test("marks a sent command timeout as delivery unknown and reconnects", async (t) => {
  const { client, server } = await connectedPair(t, { commandTimeoutMs: 40 });
  server.onCommand = () => {};
  await assert.rejects(client.sendGlobalMessage("test"), (error) => {
    assert.equal(error.code, "command_timeout");
    assert.equal(error.deliveryUnknown, true);
    return true;
  });
  await waitFor(() => server.loginCount >= 2);
});

test("wraps command sequence from 255 back to 0", async (t) => {
  const { client, server } = await connectedPair(t);
  client.sequence = 254;
  server.onCommand = (item) => server.respond(item.sequence, "");
  await client.sendGlobalMessage("first");
  await client.sendGlobalMessage("second");
  assert.deepEqual(server.commands.slice(-2).map((item) => item.sequence), [255, 0]);
});

test("sends keepalive command packets and accepts their responses", async (t) => {
  const { client, server } = await connectedPair(t, { keepAliveMs: 25 });
  server.onCommand = (item) => server.respond(item.sequence, "");
  await waitFor(() => server.commands.some((item) => item.command === ""), 500);
  assert.equal(client.status().connected, true);
});

test("rejects non-ASCII RCON passwords before opening a socket", async () => {
  const client = new DayzRconClient(clientConfig(2305, { password: "пароль" }));
  await assert.rejects(client.init(), /at least 12 printable ASCII/);
  assert.equal(client.status().state, "stopped");
});

test("rejects printable RCON passwords shorter than 12 characters", async () => {
  const client = new DayzRconClient(clientConfig(2305, { password: "short-pass" }));
  await assert.rejects(client.init(), /at least 12 printable ASCII/);
  assert.equal(client.status().state, "stopped");
});

test("ignores a stale keepalive send failure from an older generation", async () => {
  const client = new DayzRconClient(clientConfig(2305), { now: () => 1000 });
  client.closed = false;
  client.state = "connected";
  client.generation = 7;
  client.lastOutboundAt = 0;
  let rejectSend;
  client.sendDatagram = () => new Promise((resolve, reject) => { rejectSend = reject; });
  let backoffs = 0;
  client.beginBackoff = () => { backoffs += 1; };

  client.sendKeepalive();
  client.generation = 8;
  rejectSend(new Error("old socket failed"));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(backoffs, 0);
  client.closed = true;
});

test("closing during DNS lookup prevents a late socket connection", async () => {
  let finishLookup;
  const lookup = () => new Promise((resolve) => { finishLookup = resolve; });
  let sockets = 0;
  const client = new DayzRconClient(clientConfig(2305), {
    lookup,
    dgram: { createSocket() { sockets += 1; throw new Error("must not create socket"); } },
  });
  const starting = client.init();
  assert.equal(client.status().state, "resolving");
  client.close();
  finishLookup({ address: "127.0.0.1" });
  await assert.rejects(starting, (error) => error.code === "client_closed");
  assert.equal(client.status().state, "stopped");
  assert.equal(sockets, 0);
});
