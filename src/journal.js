const fs = require("node:fs");
const path = require("node:path");

const VALID_STATES = new Set([
  "queued",
  "sending",
  "acknowledged",
  "delivery_unknown",
  "expired",
  "failed",
]);
const TERMINAL_STATES = new Set(["acknowledged", "delivery_unknown", "expired", "failed"]);
const VALID_API_VERSIONS = new Set(["legacy", "v1"]);
const RECORD_FIELDS = new Set([
  "v",
  "requestId",
  "apiVersion",
  "payloadHash",
  "sentMessage",
  "ttlSeconds",
  "createdAt",
  "acceptedAt",
  "expiresAt",
  "updatedAt",
  "state",
  "response",
  "errorCode",
  "errorMessage",
  "redactedAt",
]);
const REQUEST_ID_PATTERN = /^[A-Za-z0-9._:-]{8,128}$/;
const ERROR_CODE_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/;

function clone(value) {
  return value ? JSON.parse(JSON.stringify(value)) : value;
}

function safeTimestamp(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function validateSnapshot(record) {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    throw new Error("record must be an object");
  }
  const unknown = Object.keys(record).filter((field) => !RECORD_FIELDS.has(field));
  if (unknown.length > 0) {
    throw new Error(`record contains unknown fields: ${unknown.join(", ")}`);
  }
  const missing = Array.from(RECORD_FIELDS).filter((field) => !Object.hasOwn(record, field));
  if (missing.length > 0) {
    throw new Error(`record is missing fields: ${missing.join(", ")}`);
  }
  if (record.v !== 1) {
    throw new Error("record version must be 1");
  }
  if (typeof record.requestId !== "string" || !REQUEST_ID_PATTERN.test(record.requestId)) {
    throw new Error("record requestId is invalid");
  }
  if (!VALID_API_VERSIONS.has(record.apiVersion)) {
    throw new Error("record apiVersion is invalid");
  }
  if (record.apiVersion === "v1" && !record.requestId.startsWith("cmd_")) {
    throw new Error("v1 record requestId must start with cmd_");
  }
  if (typeof record.payloadHash !== "string" || !/^[a-f0-9]{64}$/.test(record.payloadHash)) {
    throw new Error("record payloadHash must be a lowercase SHA-256 digest");
  }
  if (!Number.isInteger(record.ttlSeconds) || record.ttlSeconds < 1 || record.ttlSeconds > 300) {
    throw new Error("record ttlSeconds is invalid");
  }
  if (![record.createdAt, record.acceptedAt, record.expiresAt, record.updatedAt].every(safeTimestamp)) {
    throw new Error("record timestamps are invalid");
  }
  if (record.expiresAt <= record.createdAt) {
    throw new Error("record expiresAt must be later than createdAt");
  }
  if (record.updatedAt < record.acceptedAt) {
    throw new Error("record updatedAt must not be earlier than acceptedAt");
  }
  if (record.ttlSeconds !== Math.ceil((record.expiresAt - record.createdAt) / 1000)) {
    throw new Error("record ttlSeconds does not match its absolute lifetime");
  }
  if (!VALID_STATES.has(record.state)) {
    throw new Error("record state is invalid");
  }
  if (record.sentMessage !== null && (
    typeof record.sentMessage !== "string" ||
    record.sentMessage.length < 1 ||
    record.sentMessage.length > 160 ||
    !/^[\x20-\x7e]+$/.test(record.sentMessage)
  )) {
    throw new Error("record sentMessage must be 1 to 160 printable ASCII characters or null");
  }
  if (record.response !== null && (typeof record.response !== "string" || record.response.length > 8192)) {
    throw new Error("record response is invalid");
  }
  if (record.errorCode !== null && (
    typeof record.errorCode !== "string" || !ERROR_CODE_PATTERN.test(record.errorCode)
  )) {
    throw new Error("record errorCode is invalid");
  }
  if (record.errorMessage !== null && (
    typeof record.errorMessage !== "string" || record.errorMessage.length > 512
  )) {
    throw new Error("record errorMessage is invalid");
  }
  if (record.redactedAt !== null && !safeTimestamp(record.redactedAt)) {
    throw new Error("record redactedAt is invalid");
  }
  if (record.redactedAt !== null && record.redactedAt < record.updatedAt) {
    throw new Error("record redactedAt must not be earlier than updatedAt");
  }
  if (record.redactedAt !== null && (
    !TERMINAL_STATES.has(record.state) ||
    record.sentMessage !== null ||
    record.response !== null ||
    record.errorMessage !== null
  )) {
    throw new Error("redacted records must be terminal and contain no message or response text");
  }
  if (record.redactedAt === null && record.sentMessage === null) {
    throw new Error("non-redacted records must contain sentMessage");
  }
  if (!TERMINAL_STATES.has(record.state) && record.redactedAt !== null) {
    throw new Error("active records cannot be redacted");
  }
  return record;
}

class JsonlJournal {
  constructor(filePath, options = {}) {
    this.filePath = filePath;
    this.retentionMs = options.retentionMs || 86400000;
    this.maxRecords = options.maxRecords || 10000;
    this.maxBytes = options.maxBytes || 16777216;
    this.fs = options.fs || fs;
    this.handle = null;
    this.records = new Map();
    this.writeChain = Promise.resolve();
    this.lineCount = 0;
    this.byteCount = 0;
    this.initialized = false;
    this.lastSyncAt = null;
    this.lastCompactionAt = null;
    this.lastErrorCode = null;
  }

  async recoverCompactionArtifacts() {
    const backupPath = `${this.filePath}.compact-backup`;
    const tempPath = `${this.filePath}.compact-tmp`;
    let mainExists = true;
    try {
      await this.fs.promises.access(this.filePath);
    } catch (error) {
      if (error.code !== "ENOENT") {
        throw error;
      }
      mainExists = false;
    }
    if (!mainExists) {
      try {
        await this.fs.promises.rename(backupPath, this.filePath);
        mainExists = true;
      } catch (error) {
        if (error.code !== "ENOENT") {
          throw error;
        }
      }
    }
    if (mainExists) {
      await this.fs.promises.rm(backupPath, { force: true });
    }
    await this.fs.promises.rm(tempPath, { force: true });
  }

  async init(now = Date.now()) {
    try {
      await this.fs.promises.mkdir(path.dirname(this.filePath), { recursive: true });
      await this.recoverCompactionArtifacts();
      let content = "";
      let rawContent = Buffer.alloc(0);
      try {
        const stat = await this.fs.promises.stat(this.filePath);
        if (stat.size > this.maxBytes) {
          throw new Error(`Broadcast journal exceeds the ${this.maxBytes} byte safety limit`);
        }
        rawContent = await this.fs.promises.readFile(this.filePath);
        content = rawContent.toString("utf8");
      } catch (error) {
        if (error.code !== "ENOENT") {
          throw error;
        }
      }

      const lines = content.split("\n");
      const hasPartialTail = content.length > 0 && !content.endsWith("\n");
      let tornTail = false;
      for (let index = 0; index < lines.length; index += 1) {
        const line = lines[index].trim();
        if (!line) {
          continue;
        }
        let parsed;
        try {
          parsed = JSON.parse(line);
        } catch (error) {
          const isLastPartial = hasPartialTail && index === lines.length - 1;
          if (!isLastPartial) {
            throw new Error(`Corrupt broadcast journal at line ${index + 1}: ${error.message}`);
          }
          tornTail = true;
          continue;
        }
        try {
          const record = validateSnapshot(parsed);
          this.records.set(record.requestId, record);
          this.lineCount += 1;
        } catch (error) {
          throw new Error(`Corrupt broadcast journal at line ${index + 1}: ${error.message}`);
        }
      }

      if (hasPartialTail) {
        if (tornTail) {
          const lastNewline = rawContent.lastIndexOf(0x0a);
          await this.fs.promises.truncate(this.filePath, lastNewline + 1);
          rawContent = rawContent.subarray(0, lastNewline + 1);
        } else {
          await this.fs.promises.appendFile(this.filePath, "\n", { encoding: "utf8", mode: 0o600 });
          rawContent = Buffer.concat([rawContent, Buffer.from("\n")]);
        }
      }
      this.byteCount = rawContent.length;
      this.handle = await this.fs.promises.open(this.filePath, "a", 0o600);
      this.initialized = true;

      const recovered = Array.from(this.records.values());
      for (const record of recovered) {
        if (record.state === "sending") {
          await this.append({
            ...record,
            state: "delivery_unknown",
            updatedAt: now,
            errorCode: "recovered_after_sending",
            errorMessage: "Service restarted after the durable sending marker",
          });
        } else if (record.state === "queued" && record.expiresAt <= now) {
          await this.append({
            ...record,
            state: "expired",
            updatedAt: now,
            errorCode: "ttl_expired",
            errorMessage: "Broadcast expired before delivery",
          });
        }
      }
      await this.maintain(now, true);
      return this.all();
    } catch (error) {
      this.lastErrorCode = "journal_init_failed";
      throw error;
    }
  }

  applyRetention(now = Date.now()) {
    const cutoff = now - this.retentionMs;
    const before = Array.from(this.records.values());
    const transformed = before.map((record) => {
      if (
        TERMINAL_STATES.has(record.state) &&
        record.updatedAt < cutoff &&
        record.redactedAt === null
      ) {
        return {
          ...record,
          sentMessage: null,
          response: null,
          errorMessage: null,
          redactedAt: now,
        };
      }
      return record;
    });
    const active = transformed
      .filter((record) => !TERMINAL_STATES.has(record.state))
      .sort((left, right) => right.updatedAt - left.updatedAt);
    const terminal = transformed
      .filter((record) => TERMINAL_STATES.has(record.state))
      .sort((left, right) => right.updatedAt - left.updatedAt);
    const kept = [...active, ...terminal.slice(0, Math.max(0, this.maxRecords - active.length))];
    const next = new Map(kept.map((record) => [record.requestId, record]));
    const changed = JSON.stringify(Array.from(this.records.entries())) !== JSON.stringify(Array.from(next.entries()));
    this.records = next;
    return changed;
  }

  get(requestId) {
    return clone(this.records.get(requestId));
  }

  all() {
    return Array.from(this.records.values(), clone);
  }

  health() {
    const activeCount = Array.from(this.records.values())
      .filter((record) => !TERMINAL_STATES.has(record.state)).length;
    return {
      ok: this.initialized && Boolean(this.handle) && !this.lastErrorCode && activeCount <= this.maxRecords,
      writable: Boolean(this.handle),
      record_count: this.records.size,
      active_count: activeCount,
      redacted_count: Array.from(this.records.values()).filter((record) => record.redactedAt !== null).length,
      journal_bytes: this.byteCount,
      last_sync_at: this.lastSyncAt === null ? null : new Date(this.lastSyncAt).toISOString(),
      last_compaction_at: this.lastCompactionAt === null ? null : new Date(this.lastCompactionAt).toISOString(),
      last_error_code: this.lastErrorCode,
    };
  }

  async append(record) {
    if (!this.handle) {
      throw new Error("Broadcast journal is not initialized");
    }
    const snapshot = validateSnapshot(clone(record));
    const line = `${JSON.stringify(snapshot)}\n`;
    const lineBytes = Buffer.byteLength(line);
    const operation = this.writeChain.then(async () => {
      try {
        if (this.byteCount + lineBytes > this.maxBytes) {
          this.applyRetention(Date.now());
          await this.compactLocked(Date.now());
        }
        if (this.byteCount + lineBytes > this.maxBytes) {
          throw new Error("Broadcast journal has reached its byte limit");
        }
        await this.handle.appendFile(line, "utf8");
        await this.handle.sync();
        this.lastSyncAt = Date.now();
        this.records.set(snapshot.requestId, snapshot);
        this.lineCount += 1;
        this.byteCount += lineBytes;
        const duplicateSnapshots = this.lineCount > Math.max(100, this.records.size * 2);
        if (
          this.lineCount > Math.max(100, this.maxRecords * 4) ||
          (this.byteCount > this.maxBytes * 0.75 && duplicateSnapshots)
        ) {
          this.applyRetention(Date.now());
          await this.compactLocked(Date.now());
        }
        this.lastErrorCode = null;
      } catch (error) {
        this.lastErrorCode = "journal_write_failed";
        throw error;
      }
    });
    this.writeChain = operation.catch(() => {});
    await operation;
    return clone(snapshot);
  }

  maintain(now = Date.now(), force = false) {
    if (!this.handle) {
      return Promise.reject(new Error("Broadcast journal is not initialized"));
    }
    const operation = this.writeChain.then(async () => {
      try {
        const changed = this.applyRetention(now);
        if (changed || force || this.lineCount > Math.max(100, this.maxRecords * 4)) {
          await this.compactLocked(now);
        }
        this.lastErrorCode = null;
      } catch (error) {
        this.lastErrorCode = "journal_compaction_failed";
        throw error;
      }
    });
    this.writeChain = operation.catch(() => {});
    return operation;
  }

  async compactLocked(now) {
    const tempPath = `${this.filePath}.compact-tmp`;
    const backupPath = `${this.filePath}.compact-backup`;
    const snapshots = Array.from(this.records.values())
      .sort((left, right) => left.updatedAt - right.updatedAt || left.requestId.localeCompare(right.requestId));
    const data = snapshots.length > 0
      ? `${snapshots.map((record) => JSON.stringify(validateSnapshot(record))).join("\n")}\n`
      : "";
    const dataBytes = Buffer.byteLength(data);
    if (dataBytes > this.maxBytes) {
      throw new Error("Compacted broadcast journal exceeds its byte limit");
    }

    await this.fs.promises.rm(tempPath, { force: true });
    const tempHandle = await this.fs.promises.open(tempPath, "wx", 0o600);
    try {
      if (data) {
        await tempHandle.writeFile(data, "utf8");
      }
      await tempHandle.sync();
    } finally {
      await tempHandle.close();
    }

    await this.handle.sync();
    await this.handle.close();
    this.handle = null;
    try {
      try {
        await this.fs.promises.rename(tempPath, this.filePath);
      } catch (error) {
        if (error.code !== "EEXIST" && error.code !== "EPERM") {
          throw error;
        }
        await this.fs.promises.rm(backupPath, { force: true });
        await this.fs.promises.rename(this.filePath, backupPath);
        try {
          await this.fs.promises.rename(tempPath, this.filePath);
        } catch (renameError) {
          await this.fs.promises.rename(backupPath, this.filePath);
          throw renameError;
        }
        await this.fs.promises.rm(backupPath, { force: true });
      }
    } finally {
      this.handle = await this.fs.promises.open(this.filePath, "a", 0o600);
    }

    try {
      const directory = await this.fs.promises.open(path.dirname(this.filePath), "r");
      try {
        await directory.sync();
      } finally {
        await directory.close();
      }
    } catch (_error) {
      // Directory fsync is not supported on every host filesystem (notably Windows).
    }
    this.lineCount = snapshots.length;
    this.byteCount = dataBytes;
    this.lastSyncAt = now;
    this.lastCompactionAt = now;
  }

  async close() {
    if (!this.initialized) {
      return;
    }
    await this.maintain(Date.now(), true);
    await this.writeChain;
    if (this.handle) {
      await this.handle.close();
      this.handle = null;
    }
    this.initialized = false;
  }
}

module.exports = { JsonlJournal, VALID_STATES, validateSnapshot };
