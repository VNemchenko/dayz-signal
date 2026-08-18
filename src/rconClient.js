const dgram = require("node:dgram");
const dns = require("node:dns");
const EventEmitter = require("node:events");
const CRC32 = require("crc-32");
const { RconError } = require("./errors");

function buildPacket(type, body = Buffer.alloc(0)) {
  const payload = Buffer.concat([Buffer.from([0xff, type]), body]);
  const header = Buffer.alloc(6);
  header.write("BE", 0, "ascii");
  header.writeInt32LE(CRC32.buf(payload) | 0, 2);
  return Buffer.concat([header, payload]);
}

function parsePacket(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 8 || buffer.toString("ascii", 0, 2) !== "BE") {
    return null;
  }
  const payload = buffer.subarray(6);
  if (payload[0] !== 0xff || buffer.readInt32LE(2) !== (CRC32.buf(payload) | 0)) {
    return null;
  }
  const type = payload[1];
  const data = payload.subarray(2);
  if (type === 0) {
    return data.length >= 1 ? { type, success: data[0] === 1 } : null;
  }
  if (type === 1 || type === 2) {
    return data.length >= 1 ? { type, sequence: data[0], data: data.subarray(1) } : null;
  }
  return null;
}

function loginPacket(password) {
  return buildPacket(0, Buffer.from(password, "ascii"));
}

function commandPacket(sequence, command = "") {
  return buildPacket(1, Buffer.concat([Buffer.from([sequence]), Buffer.from(command, "ascii")]));
}

function serverMessageAckPacket(sequence) {
  return buildPacket(2, Buffer.from([sequence]));
}

function normalizeAddress(address) {
  return String(address || "").replace(/^::ffff:/, "").toLowerCase();
}

class DayzRconClient extends EventEmitter {
  constructor(config, dependencies = {}) {
    super();
    this.config = { ...config };
    this.dgram = dependencies.dgram || dgram;
    this.lookup = dependencies.lookup || dns.promises.lookup.bind(dns.promises);
    this.random = dependencies.random || Math.random;
    this.now = dependencies.now || Date.now;
    this.setTimer = dependencies.setTimeout || setTimeout;
    this.clearTimer = dependencies.clearTimeout || clearTimeout;
    this.setInterval = dependencies.setInterval || setInterval;
    this.clearInterval = dependencies.clearInterval || clearInterval;

    this.state = "stopped";
    this.closed = true;
    this.socket = null;
    this.targetAddress = null;
    this.generation = 0;
    this.sequence = -1;
    this.pending = null;
    this.keepaliveSequences = new Set();
    this.loginRetryTimer = null;
    this.loginDeadlineTimer = null;
    this.keepaliveTimer = null;
    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
    this.lastOutboundAt = 0;
    this.lastErrorCode = null;
    this.serverMessages = 0;
    this.invalidPackets = 0;
  }

  async init() {
    if (
      typeof this.config.password !== "string" ||
      this.config.password.length < 12 ||
      !/^[\x20-\x7e]+$/.test(this.config.password)
    ) {
      throw new Error("RCON_PASSWORD must contain at least 12 printable ASCII characters");
    }
    if (!this.closed) {
      throw new Error("RCON client is already initialized");
    }
    const family = this.config.connectionType === "udp6" ? 6 : 4;
    this.closed = false;
    const lookupGeneration = ++this.generation;
    this.setState("resolving");
    let resolved;
    try {
      resolved = await this.lookup(this.config.host, { family });
    } catch (error) {
      if (lookupGeneration === this.generation) {
        this.closed = true;
        this.lastErrorCode = "dns_lookup_failed";
        this.setState("stopped");
      }
      throw error;
    }
    if (this.closed || lookupGeneration !== this.generation) {
      throw new RconError("client_closed", "RCON client closed during address resolution", { safeToRetry: true });
    }
    this.targetAddress = typeof resolved === "string" ? resolved : resolved.address;
    this.connect();
  }

  connect() {
    if (this.closed || this.state === "connecting" || this.state === "connected") {
      return;
    }
    this.clearReconnectTimer();
    this.cleanupSocket();
    const generation = ++this.generation;
    this.socket = this.dgram.createSocket(this.config.connectionType || "udp4");
    this.socket.on("message", (message, rinfo) => this.handleMessage(generation, message, rinfo));
    this.socket.on("error", () => this.beginBackoff("socket_error", generation));
    this.socket.on("close", () => this.beginBackoff("socket_closed", generation));
    this.setState("connecting");
    this.sendLogin(generation);
    this.loginRetryTimer = this.setInterval(() => this.sendLogin(generation), this.config.connectionIntervalMs);
    this.loginDeadlineTimer = this.setTimer(() => this.beginBackoff("login_timeout", generation), this.config.connectionTimeoutMs);
    this.unref(this.loginRetryTimer);
    this.unref(this.loginDeadlineTimer);
  }

  sendLogin(generation) {
    if (this.closed || generation !== this.generation || this.state !== "connecting") {
      return;
    }
    this.sendDatagram(loginPacket(this.config.password), generation).catch(() => {
      this.beginBackoff("login_send_failed", generation);
    });
  }

  sendDatagram(packet, generation = this.generation) {
    return new Promise((resolve, reject) => {
      if (!this.socket || this.closed || generation !== this.generation) {
        reject(new Error("RCON socket is unavailable"));
        return;
      }
      this.socket.send(packet, this.config.port, this.targetAddress, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  handleMessage(generation, message, rinfo) {
    if (generation !== this.generation || this.closed || !this.isExpectedSource(rinfo)) {
      this.invalidPackets += 1;
      return;
    }
    const packet = parsePacket(message);
    if (!packet) {
      this.invalidPackets += 1;
      return;
    }

    if (packet.type === 0) {
      if (this.state !== "connecting") {
        return;
      }
      if (packet.success) {
        this.onConnected();
      } else {
        this.beginBackoff("authentication_failed", generation);
      }
      return;
    }
    if (this.state !== "connected") {
      return;
    }
    if (packet.type === 1) {
      this.handleCommandResponse(packet);
      return;
    }
    if (packet.type === 2) {
      this.serverMessages += 1;
      this.sendDatagram(serverMessageAckPacket(packet.sequence), generation).catch(() => {
        this.beginBackoff("server_message_ack_failed", generation);
      });
      this.emit("serverMessage", { sequence: packet.sequence, length: packet.data.length });
    }
  }

  isExpectedSource(rinfo) {
    return Boolean(
      rinfo &&
      Number(rinfo.port) === Number(this.config.port) &&
      normalizeAddress(rinfo.address) === normalizeAddress(this.targetAddress)
    );
  }

  onConnected() {
    this.clearLoginTimers();
    this.reconnectAttempts = 0;
    this.lastErrorCode = null;
    this.keepaliveSequences.clear();
    this.lastOutboundAt = this.now();
    this.setState("connected");
    this.keepaliveTimer = this.setInterval(() => this.sendKeepalive(), this.config.keepAliveMs);
    this.unref(this.keepaliveTimer);
  }

  sendKeepalive() {
    if (this.closed || this.state !== "connected" || this.pending) {
      return;
    }
    if (this.now() - this.lastOutboundAt < this.config.keepAliveMs) {
      return;
    }
    if (this.keepaliveSequences.size >= 2) {
      this.beginBackoff("keepalive_timeout", this.generation);
      return;
    }
    const generation = this.generation;
    const sequence = this.nextSequence();
    this.keepaliveSequences.add(sequence);
    this.lastOutboundAt = this.now();
    this.sendDatagram(commandPacket(sequence), generation).catch(() => {
      if (generation !== this.generation || this.closed) {
        return;
      }
      this.keepaliveSequences.delete(sequence);
      this.beginBackoff("keepalive_send_failed", generation);
    });
  }

  handleCommandResponse(packet) {
    if (this.keepaliveSequences.delete(packet.sequence)) {
      return;
    }
    const pending = this.pending;
    if (!pending || pending.sequence !== packet.sequence) {
      return;
    }
    if (packet.data.length >= 3 && packet.data[0] === 0x00) {
      const expected = packet.data[1];
      const index = packet.data[2];
      if (expected === 0 || index >= expected || (pending.expectedParts && pending.expectedParts !== expected)) {
        return;
      }
      pending.expectedParts = expected;
      pending.parts.set(index, Buffer.from(packet.data.subarray(3)));
      if (pending.parts.size < expected) {
        return;
      }
      const parts = [];
      for (let part = 0; part < expected; part += 1) {
        if (!pending.parts.has(part)) {
          return;
        }
        parts.push(pending.parts.get(part));
      }
      this.finishPending(Buffer.concat(parts).toString("ascii"));
      return;
    }
    this.finishPending(packet.data.toString("ascii"));
  }

  nextSequence() {
    for (let attempts = 0; attempts < 256; attempts += 1) {
      this.sequence = this.sequence >= 255 ? 0 : this.sequence + 1;
      if ((!this.pending || this.pending.sequence !== this.sequence) && !this.keepaliveSequences.has(this.sequence)) {
        return this.sequence;
      }
    }
    throw new RconError("sequence_exhausted", "No free RCON sequence number", { safeToRetry: true });
  }

  sendGlobalMessage(message) {
    return this.sendCommand(`say -1 ${message}`);
  }

  sendCommand(command) {
    if (this.closed || this.state !== "connected" || !this.socket) {
      return Promise.reject(new RconError("not_connected", "RCON is not connected", { safeToRetry: true }));
    }
    if (this.pending) {
      return Promise.reject(new RconError("command_busy", "Another RCON command is in flight", { safeToRetry: true }));
    }
    if (!/^[\x20-\x7e]+$/.test(command)) {
      return Promise.reject(new RconError("invalid_command", "RCON command must contain printable ASCII only"));
    }

    const sequence = this.nextSequence();
    const generation = this.generation;
    return new Promise((resolve, reject) => {
      const pending = {
        sequence,
        generation,
        written: false,
        timer: null,
        expectedParts: 0,
        parts: new Map(),
        resolve,
        reject,
      };
      this.pending = pending;
      this.sendDatagram(commandPacket(sequence, command), generation).then(() => {
        if (this.pending !== pending) {
          return;
        }
        pending.written = true;
        this.lastOutboundAt = this.now();
        pending.timer = this.setTimer(() => {
          if (this.pending !== pending) {
            return;
          }
          this.pending = null;
          reject(new RconError("command_timeout", "RCON command response timed out", { deliveryUnknown: true }));
          this.beginBackoff("command_timeout", generation);
        }, this.config.commandTimeoutMs);
        this.unref(pending.timer);
      }).catch(() => {
        if (this.pending === pending) {
          this.pending = null;
          reject(new RconError("command_send_failed", "RCON command was not written to UDP", { safeToRetry: true }));
        }
        this.beginBackoff("command_send_failed", generation);
      });
    });
  }

  finishPending(response) {
    const pending = this.pending;
    if (!pending) {
      return;
    }
    this.pending = null;
    if (pending.timer) {
      this.clearTimer(pending.timer);
    }
    pending.resolve(response);
  }

  rejectPending(code) {
    const pending = this.pending;
    if (!pending) {
      return;
    }
    this.pending = null;
    if (pending.timer) {
      this.clearTimer(pending.timer);
    }
    pending.reject(new RconError(code, "RCON connection was lost", {
      safeToRetry: !pending.written,
      deliveryUnknown: pending.written,
    }));
  }

  beginBackoff(code, generation) {
    if (this.closed || generation !== this.generation || this.state === "backoff") {
      return;
    }
    this.lastErrorCode = code;
    this.rejectPending(code);
    this.clearLoginTimers();
    this.clearKeepaliveTimer();
    this.cleanupSocket();
    this.setState("backoff");
    const cap = Math.min(
      this.config.reconnectMaxMs,
      this.config.reconnectBaseMs * (2 ** Math.min(this.reconnectAttempts, 10)),
    );
    this.reconnectAttempts += 1;
    const delay = Math.max(1, Math.floor(this.random() * cap));
    this.reconnectTimer = this.setTimer(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
    this.unref(this.reconnectTimer);
  }

  setState(state) {
    this.state = state;
    this.emit("state", this.status());
  }

  status() {
    return {
      state: this.state,
      connected: this.state === "connected",
      lastErrorCode: this.lastErrorCode,
      reconnectAttempts: this.reconnectAttempts,
      serverMessages: this.serverMessages,
      invalidPackets: this.invalidPackets,
    };
  }

  clearLoginTimers() {
    if (this.loginRetryTimer) {
      this.clearInterval(this.loginRetryTimer);
      this.loginRetryTimer = null;
    }
    if (this.loginDeadlineTimer) {
      this.clearTimer(this.loginDeadlineTimer);
      this.loginDeadlineTimer = null;
    }
  }

  clearKeepaliveTimer() {
    if (this.keepaliveTimer) {
      this.clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
    this.keepaliveSequences.clear();
  }

  clearReconnectTimer() {
    if (this.reconnectTimer) {
      this.clearTimer(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  cleanupSocket() {
    if (!this.socket) {
      return;
    }
    const socket = this.socket;
    this.socket = null;
    socket.removeAllListeners();
    try {
      socket.close();
    } catch (_error) {
      // A UDP socket that never bound cannot be closed.
    }
  }

  unref(timer) {
    if (timer && typeof timer.unref === "function") {
      timer.unref();
    }
  }

  close() {
    if (this.closed && this.state === "stopped") {
      return;
    }
    this.closed = true;
    this.generation += 1;
    this.clearLoginTimers();
    this.clearKeepaliveTimer();
    this.clearReconnectTimer();
    this.rejectPending("client_closed");
    this.cleanupSocket();
    this.lastErrorCode = null;
    this.setState("stopped");
  }
}

module.exports = {
  DayzRconClient,
  buildPacket,
  parsePacket,
  loginPacket,
  commandPacket,
  serverMessageAckPacket,
};
