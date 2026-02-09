function parsePacket(buffer) {
  if (!buffer || buffer.length < 8) {
    return null;
  }
  if (buffer.toString("utf8", 0, 2) !== "BE") {
    return null;
  }
  const payload = buffer.subarray(7);
  if (payload.length < 2) {
    return null;
  }
  return {
    code: payload.readUInt8(0),
    sequence: payload.readUInt8(1),
    data: payload.subarray(2),
  };
}

async function loadRconClass() {
  const mod = await import("battleye-node");
  return mod.default || mod;
}

class DayzRconClient {
  constructor(config) {
    this.config = { ...config };
    this.state = "disconnected";
    this.lastError = null;
    this.closed = false;

    this.RCONClass = null;
    this.client = null;

    this.socketRef = null;
    this.socketMessageHandler = null;
    this.socketErrorHandler = null;
    this.socketCloseHandler = null;

    this.pending = null;
    this.commandChain = Promise.resolve();

    this.connectPromise = null;
    this.connectResolve = null;
    this.connectReject = null;
    this.connectTimer = null;

    this.reconnectTimer = null;
    this.reconnectAttempts = 0;
  }

  async init() {
    if (this.client) {
      return;
    }
    this.RCONClass = await loadRconClass();
    this.client = this.createClient();
    this.setupHandlers();
    this.connect();
  }

  createClient() {
    return new this.RCONClass({
      address: this.config.host,
      port: this.config.port,
      password: this.config.password,
      connectionType: this.config.connectionType || "udp4",
      connectionTimeout: this.config.connectionTimeoutMs,
      connectionInterval: this.config.connectionIntervalMs,
      keepAliveInterval: this.config.keepAliveMs,
    });
  }

  setupHandlers() {
    this.client.on("onConnect", (isConnected) => {
      if (isConnected) {
        this.state = "connected";
        this.lastError = null;
        this.reconnectAttempts = 0;
        this.clearReconnectTimer();
        this.resolveConnect();
      } else {
        this.state = "error";
        this.lastError = "Disconnected";
        this.failPending("Disconnected");
        this.rejectConnect(new Error("Disconnected"));
        this.scheduleReconnect("Disconnected");
      }
      this.attachSocket();
    });

    this.client.on("error", (msg) => {
      const text = typeof msg === "string" ? msg : String(msg);
      this.state = "error";
      this.lastError = text;
      if (!this.isTransientError(text)) {
        this.failPending(text);
        this.rejectConnect(new Error(text));
      }
      if (this.shouldScheduleReconnect(text)) {
        this.scheduleReconnect(text);
      }
    });
  }

  isTransientError(message) {
    const lower = String(message || "").toLowerCase();
    return lower.includes("trying to connect") || lower.includes("already connected");
  }

  shouldScheduleReconnect(message) {
    const lower = String(message || "").toLowerCase();
    if (!lower) {
      return true;
    }
    if (lower.includes("trying to connect")) {
      return false;
    }
    if (lower.includes("already connected")) {
      return false;
    }
    return true;
  }

  connect() {
    if (!this.client || this.closed) {
      return;
    }
    this.attachSocket();
    if (this.client.isRconConnected) {
      return;
    }
    if (this.client.loginConnectionInterval) {
      return;
    }
    this.state = "connecting";
    this.client.login();
  }

  attachSocket() {
    const socket = this.client && this.client.udp ? this.client.udp.socket : null;
    if (!socket || socket === this.socketRef) {
      return;
    }

    this.detachSocket();
    this.socketRef = socket;

    if (!this.socketMessageHandler) {
      this.socketMessageHandler = (msg) => this.handleRawMessage(msg);
    }
    if (!this.socketErrorHandler) {
      this.socketErrorHandler = (err) => this.handleSocketError(err);
    }
    if (!this.socketCloseHandler) {
      this.socketCloseHandler = () => this.handleSocketClose();
    }

    socket.on("message", this.socketMessageHandler);
    socket.on("error", this.socketErrorHandler);
    socket.on("close", this.socketCloseHandler);
  }

  detachSocket() {
    if (!this.socketRef) {
      return;
    }
    if (this.socketMessageHandler) {
      this.socketRef.off("message", this.socketMessageHandler);
    }
    if (this.socketErrorHandler) {
      this.socketRef.off("error", this.socketErrorHandler);
    }
    if (this.socketCloseHandler) {
      this.socketRef.off("close", this.socketCloseHandler);
    }
    this.socketRef = null;
  }

  handleSocketError(err) {
    const message = err && err.message ? err.message : String(err);
    this.state = "error";
    this.lastError = message;
    this.failPending(message);
    this.rejectConnect(new Error(message));
    this.scheduleReconnect(message);
  }

  handleSocketClose() {
    this.state = "error";
    this.lastError = "UDP socket closed";
    this.failPending("UDP socket closed");
    this.rejectConnect(new Error("UDP socket closed"));
    this.scheduleReconnect("UDP socket closed");
  }

  scheduleReconnect() {
    if (this.closed || this.reconnectTimer) {
      return;
    }
    const delay = this.computeReconnectDelay();
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.recreateClient();
      this.connect();
    }, delay);
  }

  computeReconnectDelay() {
    const base = Math.max(Number(this.config.reconnectBaseMs || 1000), 200);
    const max = Math.max(Number(this.config.reconnectMaxMs || 30000), base);
    this.reconnectAttempts = Math.min(this.reconnectAttempts + 1, 10);
    return Math.min(max, base * Math.pow(2, this.reconnectAttempts - 1));
  }

  clearReconnectTimer() {
    if (!this.reconnectTimer) {
      return;
    }
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
  }

  recreateClient() {
    this.clearConnectWait();
    this.detachSocket();
    if (this.client) {
      this.client.removeAllListeners();
      try {
        this.client.logout();
      } catch (_err) {
        // noop
      }
    }
    this.client = this.createClient();
    this.setupHandlers();
    this.attachSocket();
  }

  status() {
    return {
      state: this.state,
      connected: Boolean(this.client && this.client.isRconConnected),
      lastError: this.lastError,
    };
  }

  async sendGlobalMessage(message) {
    const text = String(message || "").replace(/[\r\n]+/g, " ").trim();
    if (!text) {
      throw new Error("message is required");
    }
    return this.sendCommand("say", ["-1", text]);
  }

  async sendCommand(command, args = []) {
    const task = this.commandChain.then(() => this.sendCommandInternal(command, args));
    this.commandChain = task.catch(() => {});
    return task;
  }

  async sendCommandInternal(command, args = []) {
    await this.ensureConnected();
    const fullCommand = [command, ...args].join(" ").trim();
    if (!fullCommand) {
      throw new Error("RCON command is empty");
    }
    const sequence = this.sendRawCommand(fullCommand);
    return this.awaitResponse(sequence);
  }

  sendRawCommand(command) {
    const before = Number(this.client ? this.client.sequence : -1);
    this.client.commandSend(command);
    const after = Number(this.client ? this.client.sequence : -1);

    if (after === before) {
      if (!this.client || !this.client.isRconConnected) {
        throw new Error("RCON is not connected");
      }
      if (this.client.loginConnectionInterval) {
        throw new Error("RCON is connecting");
      }
      throw new Error("RCON command was not sent");
    }
    return after;
  }

  awaitResponse(sequence) {
    const timeoutMs = Math.max(Number(this.config.commandTimeoutMs || 7000), 1000);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending && this.pending.sequence === sequence) {
          this.pending = null;
        }
        this.scheduleReconnect("Command timed out");
        reject(new Error("RCON command timed out"));
      }, timeoutMs);

      this.pending = {
        sequence,
        expectedParts: null,
        parts: new Map(),
        timer,
        resolve,
        reject,
      };
    });
  }

  handleRawMessage(msg) {
    const parsed = parsePacket(msg);
    if (!parsed) {
      return;
    }
    if (parsed.code === 1) {
      this.handleCommandResponse(parsed.sequence, parsed.data);
    }
  }

  handleCommandResponse(sequence, data) {
    const pending = this.pending;
    if (!pending || pending.sequence !== sequence) {
      return;
    }

    if (!data || data.length === 0) {
      this.finalizePending("");
      return;
    }

    if (data.length >= 3 && data[0] === 0x00) {
      const expected = data[1];
      const index = data[2];
      const text = data.subarray(3).toString("utf8");

      if (expected) {
        pending.expectedParts = expected;
      }
      pending.parts.set(index, text);

      if (pending.expectedParts && pending.parts.size >= pending.expectedParts) {
        const ordered = [];
        for (let i = 0; i < pending.expectedParts; i += 1) {
          ordered.push(pending.parts.get(i) || "");
        }
        this.finalizePending(ordered.join(""));
      }
      return;
    }

    this.finalizePending(data.toString("utf8"));
  }

  finalizePending(text) {
    if (!this.pending) {
      return;
    }
    const pending = this.pending;
    this.pending = null;
    clearTimeout(pending.timer);
    pending.resolve(text);
  }

  failPending(reason) {
    if (!this.pending) {
      return;
    }
    const pending = this.pending;
    this.pending = null;
    clearTimeout(pending.timer);
    pending.reject(new Error(reason || "RCON request canceled"));
  }

  ensureConnected() {
    if (this.closed) {
      return Promise.reject(new Error("Client is closed"));
    }
    if (this.client && this.client.isRconConnected) {
      return Promise.resolve();
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.connectPromise = new Promise((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
      const timeoutMs = Math.max(Number(this.config.connectionTimeoutMs || 50000), 50000) + 1000;
      this.connectTimer = setTimeout(() => {
        this.rejectConnect(new Error("RCON connection timed out"));
      }, timeoutMs);
    });

    this.connect();
    return this.connectPromise;
  }

  resolveConnect() {
    if (this.connectResolve) {
      this.connectResolve();
    }
    this.clearConnectWait();
  }

  rejectConnect(error) {
    if (this.connectReject) {
      this.connectReject(error);
    }
    this.clearConnectWait();
  }

  clearConnectWait() {
    if (this.connectTimer) {
      clearTimeout(this.connectTimer);
    }
    this.connectTimer = null;
    this.connectPromise = null;
    this.connectResolve = null;
    this.connectReject = null;
  }

  close() {
    this.closed = true;
    this.clearReconnectTimer();
    this.failPending("Stopped");
    this.rejectConnect(new Error("Stopped"));
    this.detachSocket();

    if (this.client) {
      this.client.removeAllListeners();
      try {
        this.client.logout();
      } catch (_err) {
        // noop
      }
    }
    this.client = null;
    this.state = "disconnected";
  }
}

module.exports = { DayzRconClient };
