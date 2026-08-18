const test = require("node:test");
const assert = require("node:assert/strict");
const { loadConfig } = require("../src/config");

function validEnv(overrides = {}) {
  return {
    API_KEY: "a".repeat(32),
    RCON_HOST: "127.0.0.1",
    RCON_PORT: "2305",
    RCON_PASSWORD: "secret-rcon-password",
    ...overrides,
  };
}

test("loads a strict, fail-closed configuration", () => {
  const config = loadConfig(validEnv());
  assert.equal(config.httpPort, 8080);
  assert.equal(config.serverId, "livonia-1");
  assert.equal(config.rcon.connectionType, "udp4");
  assert.equal(config.broadcasts.queueCapacity, 50);
  assert.equal(config.broadcasts.maxMessageLength, 160);
  assert.equal(config.broadcasts.maxTtlSeconds, 300);
  assert.equal(config.broadcasts.maxFutureSkewSeconds, 30);
  assert.equal(config.broadcasts.maxBytes, 16777216);
});

test("rejects missing, short and placeholder API keys", () => {
  assert.throws(() => loadConfig(validEnv({ API_KEY: "" })), /API_KEY is required/);
  assert.throws(() => loadConfig(validEnv({ API_KEY: "short" })), /at least 32/);
  assert.throws(() => loadConfig(validEnv({ API_KEY: `change_me_${"x".repeat(40)}` })), /placeholder/);
  assert.throws(() => loadConfig(validEnv({ API_KEY: "replace_with_random_key_at_least_32_chars" })), /placeholder/);
});

test("rejects placeholder RCON password and invalid ranges", () => {
  assert.throws(() => loadConfig(validEnv({ RCON_PASSWORD: "change_me_rcon" })), /placeholder/);
  assert.throws(() => loadConfig(validEnv({ RCON_PASSWORD: "too-short" })), /at least 12/);
  assert.throws(() => loadConfig(validEnv({ RCON_PORT: "70000" })), /RCON_PORT/);
  assert.throws(() => loadConfig(validEnv({ RCON_KEEPALIVE_MS: "45000" })), /RCON_KEEPALIVE_MS/);
  assert.throws(() => loadConfig(validEnv({ BROADCAST_MAX_TTL_SECONDS: "301" })), /BROADCAST_MAX_TTL_SECONDS/);
  assert.throws(() => loadConfig(validEnv({ COMMAND_MAX_FUTURE_SKEW_SECONDS: "301" })), /COMMAND_MAX_FUTURE_SKEW_SECONDS/);
  assert.throws(() => loadConfig(validEnv({ SERVER_ID: "Livonia 1" })), /SERVER_ID/);
  assert.throws(
    () => loadConfig(validEnv({ QUEUE_CAPACITY: "101", IDEMPOTENCY_MAX_RECORDS: "100" })),
    /QUEUE_CAPACITY must not exceed/,
  );
});

test("reads secrets from files and forbids ambiguous sources", () => {
  const reads = [];
  const config = loadConfig(validEnv({
    API_KEY: "",
    API_KEY_FILE: "/run/secrets/api",
    RCON_PASSWORD: "",
    RCON_PASSWORD_FILE: "/run/secrets/rcon",
  }), {
    readFileSync(fileName) {
      reads.push(fileName);
      return fileName.endsWith("api") ? "b".repeat(32) : "rcon-secret-12";
    },
  });
  assert.equal(config.apiKey, "b".repeat(32));
  assert.deepEqual(reads, ["/run/secrets/api", "/run/secrets/rcon"]);
  assert.throws(() => loadConfig(validEnv({ API_KEY_FILE: "/also" })), /mutually exclusive/);
});
