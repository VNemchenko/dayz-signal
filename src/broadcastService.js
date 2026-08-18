const crypto = require("node:crypto");
const EventEmitter = require("node:events");
const { AppError } = require("./errors");
const { TokenBucket } = require("./rateLimiter");
const { toGameAscii } = require("./transliterate");

const TERMINAL_STATES = new Set(["acknowledged", "delivery_unknown", "expired", "failed"]);
const REQUEST_ID_PATTERN = /^[A-Za-z0-9._:-]{8,128}$/;

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value).sort().map((key) => [key, canonicalize(value[key])]),
    );
  }
  return value;
}

function hashPayload(payload) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify(canonicalize(payload)), "utf8")
    .digest("hex");
}

class BroadcastService extends EventEmitter {
  constructor(options) {
    super();
    this.rcon = options.rcon;
    this.journal = options.journal;
    this.config = options.config;
    this.now = options.now || Date.now;
    this.randomUUID = options.randomUUID || crypto.randomUUID;
    this.setTimer = options.setTimeout || setTimeout;
    this.clearTimer = options.clearTimeout || clearTimeout;
    this.queue = [];
    this.activeRequestId = null;
    this.processing = false;
    this.closed = false;
    this.expiryTimer = null;
    this.maintenanceTimer = null;
    this.submitChain = Promise.resolve();
    this.rateLimiter = new TokenBucket(this.config.ratePerMinute, this.config.rateBurst, this.now());
    this.onRconState = (status) => {
      if (status.connected) {
        void this.pump();
      }
      this.emit("status");
    };
  }

  async init() {
    const records = await this.journal.init(this.now());
    this.queue = records
      .filter((record) => record.state === "queued")
      .sort((left, right) => (left.acceptedAt || left.createdAt) - (right.acceptedAt || right.createdAt))
      .map((record) => record.requestId);
    this.rcon.on("state", this.onRconState);
    this.scheduleExpiry();
    this.scheduleMaintenance();
    void this.pump();
  }

  submit(input) {
    const operation = this.submitChain.then(() => this.submitInternal(input));
    this.submitChain = operation.catch(() => {});
    return operation;
  }

  async submitInternal(input) {
    if (this.closed) {
      throw new AppError(503, "shutting_down", "Service is shutting down", { retryAfter: 1 });
    }

    const requestId = input.requestId || this.randomUUID();
    if (!REQUEST_ID_PATTERN.test(requestId)) {
      throw new AppError(400, "invalid_request_id", "request_id must match [A-Za-z0-9._:-]{8,128}");
    }
    if (typeof input.message !== "string") {
      throw new AppError(400, "invalid_message", "message must be a string");
    }

    const acceptedAt = this.now();
    let createdAt;
    let expiresAt;
    let ttlSeconds;
    if (input.expiresAt !== undefined || input.createdAt !== undefined) {
      createdAt = input.createdAt;
      expiresAt = input.expiresAt;
      if (!Number.isSafeInteger(createdAt) || !Number.isSafeInteger(expiresAt)) {
        throw new AppError(400, "invalid_expiry", "created_at and expires_at must be valid timestamps");
      }
      const lifetimeMs = expiresAt - createdAt;
      if (lifetimeMs < 1000 || lifetimeMs > this.config.maxTtlSeconds * 1000) {
        throw new AppError(400, "invalid_ttl", `command lifetime must be between 1 and ${this.config.maxTtlSeconds} seconds`);
      }
      if (expiresAt - acceptedAt > this.config.maxTtlSeconds * 1000) {
        throw new AppError(400, "invalid_expiry", `expires_at must be no more than ${this.config.maxTtlSeconds} seconds in the future`);
      }
      const maxFutureSkewMs = (this.config.maxFutureSkewSeconds || 0) * 1000;
      if (createdAt > acceptedAt + maxFutureSkewMs) {
        throw new AppError(400, "created_at_in_future", "created_at is too far in the future");
      }
      ttlSeconds = Math.ceil(lifetimeMs / 1000);
    } else {
      ttlSeconds = input.ttlSeconds === undefined ? this.config.defaultTtlSeconds : input.ttlSeconds;
      if (!Number.isInteger(ttlSeconds) || ttlSeconds < 1 || ttlSeconds > this.config.maxTtlSeconds) {
        throw new AppError(400, "invalid_ttl", `ttl_seconds must be an integer between 1 and ${this.config.maxTtlSeconds}`);
      }
      createdAt = acceptedAt;
      expiresAt = acceptedAt + ttlSeconds * 1000;
    }

    const sentMessage = toGameAscii(input.message);
    if (!sentMessage) {
      throw new AppError(422, "empty_after_transliteration", "message is empty after ASCII transliteration");
    }
    if (sentMessage.length > this.config.maxMessageLength) {
      throw new AppError(422, "message_too_long", `transliterated message exceeds ${this.config.maxMessageLength} characters`);
    }

    await this.journal.maintain(acceptedAt);
    const payloadHash = hashPayload(input.idempotencyPayload || {
      schema: "legacy.broadcast.v1",
      request_id: requestId,
      message: input.message,
      ttl_seconds: ttlSeconds,
    });
    const existing = this.journal.get(requestId);
    if (existing) {
      if (existing.payloadHash !== payloadHash) {
        throw new AppError(409, "idempotency_conflict", "request_id is already used with a different payload");
      }
      return { record: existing, created: false };
    }

    const record = {
      v: 1,
      requestId,
      apiVersion: input.apiVersion || "legacy",
      payloadHash,
      sentMessage,
      ttlSeconds,
      createdAt,
      acceptedAt,
      expiresAt,
      updatedAt: acceptedAt,
      state: "queued",
      response: null,
      errorCode: null,
      errorMessage: null,
      redactedAt: null,
    };

    const rate = this.rateLimiter.consume(acceptedAt);
    if (!rate.allowed) {
      throw new AppError(429, "rate_limited", "Broadcast rate limit exceeded", { retryAfter: rate.retryAfter });
    }

    if (expiresAt <= acceptedAt) {
      const expired = {
        ...record,
        state: "expired",
        errorCode: "command_expired",
        errorMessage: "Broadcast expired before it was accepted",
      };
      await this.journal.append(expired);
      this.emitUpdate(expired);
      return { record: expired, created: true };
    }

    if (this.queueDepth() >= this.config.queueCapacity) {
      throw new AppError(503, "queue_full", "Broadcast queue is full", { retryAfter: 1 });
    }

    await this.journal.append(record);
    this.queue.push(requestId);
    this.emitUpdate(record);
    this.scheduleExpiry();
    void this.pump();
    return { record, created: true };
  }

  get(requestId) {
    if (!REQUEST_ID_PATTERN.test(String(requestId || ""))) {
      throw new AppError(400, "invalid_request_id", "Invalid request_id");
    }
    return this.journal.get(requestId);
  }

  health() {
    return this.journal.health();
  }

  queueDepth() {
    return this.queue.length;
  }

  isSaturated() {
    return this.queueDepth() >= this.config.queueCapacity;
  }

  async transition(record, state, fields = {}) {
    const next = {
      ...record,
      ...fields,
      state,
      updatedAt: this.now(),
    };
    await this.journal.append(next);
    this.emitUpdate(next);
    return next;
  }

  emitUpdate(record) {
    this.emit("update", record);
    this.emit("status");
  }

  async expireQueued() {
    const now = this.now();
    const retained = [];
    for (const requestId of this.queue) {
      const record = this.journal.get(requestId);
      if (!record || record.state !== "queued") {
        continue;
      }
      if (record.expiresAt <= now) {
        await this.transition(record, "expired", {
          errorCode: "ttl_expired",
          errorMessage: "Broadcast expired before delivery",
        });
      } else {
        retained.push(requestId);
      }
    }
    this.queue = retained;
  }

  async pump() {
    if (this.processing || this.closed) {
      return;
    }
    this.processing = true;
    try {
      await this.expireQueued();
      while (!this.closed && this.queue.length > 0) {
        if (!this.rcon.status().connected) {
          break;
        }
        const requestId = this.queue[0];
        let record = this.journal.get(requestId);
        if (!record || record.state !== "queued") {
          this.queue.shift();
          continue;
        }
        if (record.expiresAt <= this.now()) {
          this.queue.shift();
          await this.transition(record, "expired", {
            errorCode: "ttl_expired",
            errorMessage: "Broadcast expired before delivery",
          });
          continue;
        }

        this.activeRequestId = requestId;
        record = await this.transition(record, "sending");
        try {
          const response = await this.rcon.sendGlobalMessage(record.sentMessage);
          this.queue.shift();
          await this.transition(record, "acknowledged", {
            response: String(response || "").slice(0, 8192),
            errorCode: null,
            errorMessage: null,
          });
        } catch (error) {
          if (error.safeToRetry) {
            record = await this.transition(record, "queued", {
              errorCode: "waiting_for_rcon",
              errorMessage: "RCON disconnected before UDP send",
            });
            break;
          }
          this.queue.shift();
          if (error.deliveryUnknown) {
            await this.transition(record, "delivery_unknown", {
              errorCode: error.code || "rcon_delivery_unknown",
              errorMessage: error.message,
            });
          } else {
            await this.transition(record, "failed", {
              errorCode: error.code || "rcon_failed",
              errorMessage: error.message,
            });
          }
        } finally {
          this.activeRequestId = null;
        }
        await this.expireQueued();
      }
    } catch (error) {
      this.emit("fatal", error);
    } finally {
      this.processing = false;
      this.scheduleExpiry();
      this.emit("status");
      this.emit("idle");
    }
  }

  scheduleExpiry() {
    if (this.expiryTimer) {
      this.clearTimer(this.expiryTimer);
      this.expiryTimer = null;
    }
    if (this.closed || this.queue.length === 0) {
      return;
    }
    const expirations = this.queue
      .map((requestId) => this.journal.get(requestId))
      .filter(Boolean)
      .map((record) => record.expiresAt);
    if (expirations.length === 0) {
      return;
    }
    const delay = Math.max(0, Math.min(...expirations) - this.now());
    this.expiryTimer = this.setTimer(() => {
      this.expiryTimer = null;
      void this.pump();
    }, delay);
    if (typeof this.expiryTimer.unref === "function") {
      this.expiryTimer.unref();
    }
  }

  scheduleMaintenance() {
    if (this.maintenanceTimer) {
      this.clearTimer(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }
    if (this.closed) {
      return;
    }
    const retentionMs = this.config.retentionMs || 86400000;
    const delay = Math.max(1000, Math.min(60000, Math.floor(retentionMs / 2)));
    this.maintenanceTimer = this.setTimer(async () => {
      this.maintenanceTimer = null;
      try {
        await this.journal.maintain(this.now());
      } catch (error) {
        this.emit("fatal", error);
      } finally {
        this.scheduleMaintenance();
      }
    }, delay);
    if (typeof this.maintenanceTimer.unref === "function") {
      this.maintenanceTimer.unref();
    }
  }

  waitForTerminal(requestId, timeoutMs) {
    const existing = this.journal.get(requestId);
    if (!existing || TERMINAL_STATES.has(existing.state) || timeoutMs <= 0) {
      return Promise.resolve(existing);
    }
    return new Promise((resolve) => {
      let timer;
      const onUpdate = (record) => {
        if (record.requestId === requestId && TERMINAL_STATES.has(record.state)) {
          cleanup();
          resolve(record);
        }
      };
      const cleanup = () => {
        this.off("update", onUpdate);
        if (timer) {
          this.clearTimer(timer);
        }
      };
      this.on("update", onUpdate);
      timer = this.setTimer(() => {
        cleanup();
        resolve(this.journal.get(requestId));
      }, timeoutMs);
    });
  }

  waitForIdle(timeoutMs) {
    if (!this.processing) {
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      let settled = false;
      let timer;
      const finish = (idle) => {
        if (settled) {
          return;
        }
        settled = true;
        this.off("idle", onIdle);
        if (timer) {
          this.clearTimer(timer);
        }
        resolve(idle);
      };
      const onIdle = () => finish(true);
      this.on("idle", onIdle);
      timer = this.setTimer(() => finish(false), Math.max(1, timeoutMs));
    });
  }

  waitForSubmissions(timeoutMs) {
    let timer;
    return Promise.race([
      this.submitChain.then(() => true),
      new Promise((resolve) => {
        timer = this.setTimer(() => resolve(false), Math.max(1, timeoutMs));
      }),
    ]).finally(() => {
      if (timer) {
        this.clearTimer(timer);
      }
    });
  }

  async close(graceMs = 0) {
    this.closed = true;
    this.rcon.off("state", this.onRconState);
    if (this.expiryTimer) {
      this.clearTimer(this.expiryTimer);
      this.expiryTimer = null;
    }
    if (this.maintenanceTimer) {
      this.clearTimer(this.maintenanceTimer);
      this.maintenanceTimer = null;
    }

    const submissionWaitMs = Math.floor(Math.max(0, graceMs) / 3);
    const submissionsFinished = await this.waitForSubmissions(submissionWaitMs);
    if (!submissionsFinished) {
      this.rcon.close();
      throw new Error("Broadcast shutdown timed out while accepting a command");
    }
    const remainingMs = Math.max(0, graceMs - submissionWaitMs);
    const firstWaitMs = Math.floor(remainingMs / 2);
    const secondWaitMs = Math.max(1, remainingMs - firstWaitMs);
    let idle = await this.waitForIdle(firstWaitMs);
    this.rcon.close();
    if (!idle) {
      idle = await this.waitForIdle(secondWaitMs);
    }
    if (!idle) {
      throw new Error("Broadcast shutdown timed out while a command was in flight");
    }
    await this.journal.close();
  }
}

module.exports = {
  BroadcastService,
  TERMINAL_STATES,
  REQUEST_ID_PATTERN,
  canonicalize,
  hashPayload,
};
