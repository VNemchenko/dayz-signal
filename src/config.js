const fs = require("node:fs");
const path = require("node:path");

function integer(value, fallback, name, min, max) {
  const actual = value === undefined || value === "" ? fallback : Number(value);
  if (!Number.isInteger(actual) || actual < min || actual > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }
  return actual;
}

function required(value, name) {
  const actual = String(value || "").trim();
  if (!actual) {
    throw new Error(`${name} is required`);
  }
  return actual;
}

function serverId(value) {
  const actual = String(value || "livonia-1").trim();
  if (!/^[a-z0-9][a-z0-9_-]{1,47}$/.test(actual)) {
    throw new Error("SERVER_ID must match [a-z0-9][a-z0-9_-]{1,47}");
  }
  return actual;
}

function secret(env, name, readFileSync) {
  const inlineValue = String(env[name] || "").trim();
  const fileName = String(env[`${name}_FILE`] || "").trim();
  if (inlineValue && fileName) {
    throw new Error(`${name} and ${name}_FILE are mutually exclusive`);
  }
  if (fileName) {
    return required(readFileSync(fileName, "utf8"), name);
  }
  return required(inlineValue, name);
}

function loadConfig(env = process.env, options = {}) {
  const readFileSync = options.readFileSync || fs.readFileSync;
  const apiKey = secret(env, "API_KEY", readFileSync);
  const rconPassword = secret(env, "RCON_PASSWORD", readFileSync);

  const placeholderPattern = /^(?:change[_-]?me|replace[_-]?with)/i;
  if (apiKey.length < 32 || placeholderPattern.test(apiKey)) {
    throw new Error("API_KEY must contain at least 32 characters and must not be a placeholder");
  }
  if (rconPassword.length < 12 || placeholderPattern.test(rconPassword)) {
    throw new Error("RCON_PASSWORD must contain at least 12 characters and must not be a placeholder");
  }
  if (!/^[\x20-\x7e]+$/.test(rconPassword)) {
    throw new Error("RCON_PASSWORD must contain printable ASCII characters only");
  }

  const connectionType = String(env.RCON_CONNECTION_TYPE || "udp4").trim();
  if (connectionType !== "udp4" && connectionType !== "udp6") {
    throw new Error("RCON_CONNECTION_TYPE must be udp4 or udp6");
  }

  const commandTimeoutMs = integer(env.RCON_COMMAND_TIMEOUT_MS, 7000, "RCON_COMMAND_TIMEOUT_MS", 250, 60000);
  const defaultJournal = path.resolve(process.cwd(), "data", "broadcasts.jsonl");

  const reconnectBaseMs = integer(env.RCON_RECONNECT_BASE_MS, 1000, "RCON_RECONNECT_BASE_MS", 50, 60000);
  const reconnectMaxMs = integer(env.RCON_RECONNECT_MAX_MS, 30000, "RCON_RECONNECT_MAX_MS", 100, 300000);
  if (reconnectMaxMs < reconnectBaseMs) {
    throw new Error("RCON_RECONNECT_MAX_MS must be greater than or equal to RCON_RECONNECT_BASE_MS");
  }
  const defaultTtlSeconds = integer(env.BROADCAST_TTL_SECONDS, 30, "BROADCAST_TTL_SECONDS", 1, 300);
  const maxTtlSeconds = integer(env.BROADCAST_MAX_TTL_SECONDS, 300, "BROADCAST_MAX_TTL_SECONDS", 1, 300);
  if (defaultTtlSeconds > maxTtlSeconds) {
    throw new Error("BROADCAST_TTL_SECONDS must not exceed BROADCAST_MAX_TTL_SECONDS");
  }
  const queueCapacity = integer(env.QUEUE_CAPACITY, 50, "QUEUE_CAPACITY", 1, 10000);
  const maxRecords = integer(env.IDEMPOTENCY_MAX_RECORDS, 10000, "IDEMPOTENCY_MAX_RECORDS", 100, 1000000);
  if (queueCapacity > maxRecords) {
    throw new Error("QUEUE_CAPACITY must not exceed IDEMPOTENCY_MAX_RECORDS");
  }

  return {
    serviceName: String(env.SERVICE_NAME || "dayz-signal").trim() || "dayz-signal",
    serverId: serverId(env.SERVER_ID),
    httpHost: String(env.HTTP_HOST || "0.0.0.0").trim(),
    httpPort: integer(env.HTTP_PORT, 8080, "HTTP_PORT", 1, 65535),
    httpBodyLimitBytes: integer(env.HTTP_BODY_LIMIT_BYTES, 4096, "HTTP_BODY_LIMIT_BYTES", 512, 65536),
    httpWaitMs: integer(env.HTTP_WAIT_MS, commandTimeoutMs + 1000, "HTTP_WAIT_MS", 0, 60000),
    startupTimeoutMs: integer(env.STARTUP_TIMEOUT_MS, 15000, "STARTUP_TIMEOUT_MS", 1000, 60000),
    shutdownGraceMs: integer(env.SHUTDOWN_GRACE_MS, 10000, "SHUTDOWN_GRACE_MS", 0, 60000),
    apiKey,
    rcon: {
      host: required(env.RCON_HOST, "RCON_HOST"),
      port: integer(env.RCON_PORT, 0, "RCON_PORT", 1, 65535),
      password: rconPassword,
      connectionType,
      connectionTimeoutMs: integer(env.RCON_CONNECTION_TIMEOUT_MS, 50000, "RCON_CONNECTION_TIMEOUT_MS", 1000, 300000),
      connectionIntervalMs: integer(env.RCON_CONNECTION_INTERVAL_MS, 5000, "RCON_CONNECTION_INTERVAL_MS", 100, 60000),
      keepAliveMs: integer(env.RCON_KEEPALIVE_MS, 10000, "RCON_KEEPALIVE_MS", 1000, 44000),
      commandTimeoutMs,
      reconnectBaseMs,
      reconnectMaxMs,
    },
    broadcasts: {
      journalPath: path.resolve(String(env.JOURNAL_PATH || defaultJournal)),
      queueCapacity,
      defaultTtlSeconds,
      maxTtlSeconds,
      maxFutureSkewSeconds: integer(
        env.COMMAND_MAX_FUTURE_SKEW_SECONDS,
        30,
        "COMMAND_MAX_FUTURE_SKEW_SECONDS",
        0,
        300,
      ),
      retentionMs: integer(env.IDEMPOTENCY_RETENTION_MS, 86400000, "IDEMPOTENCY_RETENTION_MS", 60000, 604800000),
      maxRecords,
      maxBytes: integer(env.JOURNAL_MAX_BYTES, 16777216, "JOURNAL_MAX_BYTES", 1048576, 268435456),
      ratePerMinute: integer(env.RATE_LIMIT_PER_MINUTE, 30, "RATE_LIMIT_PER_MINUTE", 1, 100000),
      rateBurst: integer(env.RATE_LIMIT_BURST, 5, "RATE_LIMIT_BURST", 1, 10000),
      maxMessageLength: 160,
    },
  };
}

module.exports = { loadConfig };
