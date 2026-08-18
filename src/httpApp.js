const crypto = require("node:crypto");
const { URL } = require("node:url");
const { AppError } = require("./errors");

const V1_BODY_FIELDS = new Set([
  "schema",
  "command_id",
  "server_id",
  "created_at",
  "expires_at",
  "channel",
  "message",
  "metadata",
]);
const LEGACY_BODY_FIELDS = new Set(["message", "request_id", "ttl_seconds"]);
const METADATA_FIELDS = new Set(["event_id", "policy_version"]);
const COMMAND_ID_PATTERN = /^cmd_[A-Za-z0-9][A-Za-z0-9._:-]{3,123}$/;
const SERVER_ID_PATTERN = /^[a-z0-9][a-z0-9_-]{1,47}$/;
const METADATA_VALUE_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
const RFC3339_UTC_PATTERN = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z$/;

function safeEqual(leftValue, rightValue) {
  const left = Buffer.from(String(leftValue || ""), "utf8");
  const right = Buffer.from(String(rightValue || ""), "utf8");
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function authenticate(req, apiKey) {
  const headerKey = String(req.headers["x-api-key"] || "").trim();
  const authorization = String(req.headers.authorization || "");
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  const bearer = match ? match[1].trim() : "";
  if (headerKey && bearer && !safeEqual(headerKey, bearer)) {
    return false;
  }
  return safeEqual(headerKey || bearer, apiKey);
}

function writeJson(res, statusCode, body, headers = {}) {
  const data = Buffer.from(JSON.stringify(body), "utf8");
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": data.length,
    "cache-control": "no-store",
    ...headers,
  });
  res.end(data);
}

function rejectUnknownFields(body, allowedFields, location = "body") {
  const unknown = Object.keys(body).filter((key) => !allowedFields.has(key));
  if (unknown.length > 0) {
    throw new AppError(400, "unknown_fields", `Unknown ${location} fields: ${unknown.join(", ")}`);
  }
}

async function readJson(req, limitBytes, allowedFields) {
  const contentType = String(req.headers["content-type"] || "").split(";", 1)[0].trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new AppError(415, "unsupported_media_type", "Content-Type must be application/json");
  }
  const contentLengthHeader = req.headers["content-length"];
  if (contentLengthHeader !== undefined && !/^\d+$/.test(String(contentLengthHeader))) {
    throw new AppError(400, "invalid_content_length", "Content-Length must be a non-negative integer");
  }
  const contentLength = Number(contentLengthHeader || 0);
  if (contentLength > limitBytes) {
    throw new AppError(413, "body_too_large", `JSON body exceeds ${limitBytes} bytes`);
  }

  const chunks = [];
  let length = 0;
  for await (const chunk of req) {
    length += chunk.length;
    if (length > limitBytes) {
      throw new AppError(413, "body_too_large", `JSON body exceeds ${limitBytes} bytes`);
    }
    chunks.push(chunk);
  }
  let body;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch (_error) {
    throw new AppError(400, "invalid_json", "Request body must be valid JSON");
  }
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new AppError(400, "invalid_body", "Request body must be a JSON object");
  }
  rejectUnknownFields(body, allowedFields);
  return body;
}

function parseUtcTimestamp(value, field) {
  if (typeof value !== "string" || !RFC3339_UTC_PATTERN.test(value)) {
    throw new AppError(400, "invalid_timestamp", `${field} must be an RFC3339 UTC timestamp ending in Z`);
  }
  const timestamp = Date.parse(value);
  if (!Number.isSafeInteger(timestamp)) {
    throw new AppError(400, "invalid_timestamp", `${field} is not a valid timestamp`);
  }
  const match = value.match(/^(.*:\d{2})(?:\.(\d{1,3}))?Z$/);
  const canonical = `${match[1]}.${String(match[2] || "").padEnd(3, "0")}Z`;
  if (new Date(timestamp).toISOString() !== canonical) {
    throw new AppError(400, "invalid_timestamp", `${field} is not a valid calendar timestamp`);
  }
  return timestamp;
}

function validateV1Command(req, body, config) {
  const required = Array.from(V1_BODY_FIELDS).filter((field) => body[field] === undefined);
  if (required.length > 0) {
    throw new AppError(400, "missing_fields", `Missing required fields: ${required.join(", ")}`);
  }
  if (body.schema !== "dayz.command.v1") {
    throw new AppError(400, "invalid_schema", "schema must be dayz.command.v1");
  }
  if (typeof body.command_id !== "string" || !COMMAND_ID_PATTERN.test(body.command_id)) {
    throw new AppError(400, "invalid_command_id", "command_id must start with cmd_ and contain 8 to 128 safe characters");
  }
  const idempotencyKey = String(req.headers["idempotency-key"] || "").trim();
  if (!idempotencyKey) {
    throw new AppError(400, "missing_idempotency_key", "Idempotency-Key is required");
  }
  if (!safeEqual(idempotencyKey, body.command_id)) {
    throw new AppError(400, "command_id_mismatch", "Idempotency-Key and command_id must match");
  }
  if (typeof body.server_id !== "string" || !SERVER_ID_PATTERN.test(body.server_id)) {
    throw new AppError(400, "invalid_server_id", "server_id has an invalid format");
  }
  if (body.server_id !== config.serverId) {
    throw new AppError(409, "wrong_server", "command is addressed to a different server");
  }
  if (body.channel !== "global") {
    throw new AppError(400, "invalid_channel", "channel must be global");
  }
  if (typeof body.message !== "string") {
    throw new AppError(400, "invalid_message", "message must be a string");
  }
  const createdAt = parseUtcTimestamp(body.created_at, "created_at");
  const expiresAt = parseUtcTimestamp(body.expires_at, "expires_at");
  const lifetimeMs = expiresAt - createdAt;
  if (lifetimeMs < 1000 || lifetimeMs > config.broadcasts.maxTtlSeconds * 1000) {
    throw new AppError(
      400,
      "invalid_ttl",
      `expires_at minus created_at must be between 1 and ${config.broadcasts.maxTtlSeconds} seconds`,
    );
  }
  if (!body.metadata || typeof body.metadata !== "object" || Array.isArray(body.metadata)) {
    throw new AppError(400, "invalid_metadata", "metadata must be an object");
  }
  rejectUnknownFields(body.metadata, METADATA_FIELDS, "metadata");
  for (const field of METADATA_FIELDS) {
    if (typeof body.metadata[field] !== "string" || !METADATA_VALUE_PATTERN.test(body.metadata[field])) {
      throw new AppError(400, "invalid_metadata", `metadata.${field} must contain 1 to 128 safe characters`);
    }
  }
  return {
    requestId: body.command_id,
    message: body.message,
    createdAt,
    expiresAt,
    apiVersion: "v1",
    idempotencyPayload: body,
  };
}

function resolveLegacyRequestId(req, body) {
  const headerId = String(req.headers["idempotency-key"] || "").trim();
  const bodyId = body.request_id === undefined ? "" : String(body.request_id).trim();
  if (headerId && bodyId && !safeEqual(headerId, bodyId)) {
    throw new AppError(400, "request_id_mismatch", "Idempotency-Key and request_id must match");
  }
  return headerId || bodyId || undefined;
}

function formatRecord(record, serviceName) {
  const ok = record.state === "queued" || record.state === "sending" || record.state === "acknowledged";
  const result = {
    ok,
    service: serviceName,
    request_id: record.requestId,
    status: record.state,
    created_at: new Date(record.createdAt).toISOString(),
    expires_at: new Date(record.expiresAt).toISOString(),
    updated_at: new Date(record.updatedAt).toISOString(),
  };
  if (record.apiVersion === "v1") {
    result.command_id = record.requestId;
  }
  if (record.sentMessage !== null) {
    result.sent_message = record.sentMessage;
  }
  if (record.redactedAt) {
    result.redacted = true;
  }
  if (record.state === "acknowledged") {
    result.command = "say -1 <message>";
    result.response = record.response || "";
  }
  if (record.errorCode) {
    result.error_code = record.errorCode;
    result.error = record.errorMessage || record.errorCode;
  }
  return result;
}

function stateStatusCode(state) {
  switch (state) {
    case "acknowledged": return 200;
    case "queued":
    case "sending": return 202;
    case "expired": return 410;
    case "delivery_unknown": return 504;
    case "failed": return 502;
    default: return 500;
  }
}

function createHttpHandler(options) {
  const { config, broadcasts, rcon } = options;
  const isShuttingDown = options.isShuttingDown || (() => false);

  return async function handler(req, res) {
    try {
      const url = new URL(req.url, "http://localhost");
      if (req.method === "GET" && url.pathname === "/livez") {
        writeJson(res, 200, { status: "ok", service: config.serviceName });
        return;
      }
      if (req.method === "GET" && url.pathname === "/readyz") {
        const rconStatus = rcon.status();
        const journal = broadcasts.health();
        const ready = !isShuttingDown() && rconStatus.connected && journal.ok && !broadcasts.isSaturated();
        writeJson(res, ready ? 200 : 503, {
          status: ready ? "ready" : "not_ready",
          service: config.serviceName,
          rcon: { state: rconStatus.state, connected: rconStatus.connected, last_error_code: rconStatus.lastErrorCode },
          journal,
          queue_depth: broadcasts.queueDepth(),
        });
        return;
      }
      if (req.method === "GET" && url.pathname === "/health") {
        const rconStatus = rcon.status();
        const journal = broadcasts.health();
        writeJson(res, 200, {
          status: journal.ok ? "ok" : "degraded",
          service: config.serviceName,
          rcon: { state: rconStatus.state, connected: rconStatus.connected, last_error_code: rconStatus.lastErrorCode },
          journal,
          queue_depth: broadcasts.queueDepth(),
        });
        return;
      }

      if (!authenticate(req, config.apiKey)) {
        throw new AppError(401, "unauthorized", "Unauthorized");
      }

      const statusMatch = req.method === "GET" && url.pathname.match(/^\/v1\/broadcasts\/([^/]+)$/);
      if (statusMatch) {
        let requestId;
        try {
          requestId = decodeURIComponent(statusMatch[1]);
        } catch (_error) {
          throw new AppError(400, "invalid_request_id", "Invalid command_id encoding");
        }
        const record = broadcasts.get(requestId);
        if (!record) {
          throw new AppError(404, "not_found", "Broadcast not found");
        }
        writeJson(res, 200, formatRecord(record, config.serviceName));
        return;
      }

      if (req.method === "POST" && url.pathname === "/v1/broadcasts") {
        const body = await readJson(req, config.httpBodyLimitBytes, V1_BODY_FIELDS);
        const result = await broadcasts.submit(validateV1Command(req, body, config));
        const record = broadcasts.get(result.record.requestId) || result.record;
        writeJson(res, 202, formatRecord(record, config.serviceName), {
          location: `/v1/broadcasts/${encodeURIComponent(record.requestId)}`,
        });
        return;
      }

      if (req.method === "POST" && url.pathname === "/broadcast") {
        const body = await readJson(req, config.httpBodyLimitBytes, LEGACY_BODY_FIELDS);
        const result = await broadcasts.submit({
          requestId: resolveLegacyRequestId(req, body),
          message: body.message,
          ttlSeconds: body.ttl_seconds,
          apiVersion: "legacy",
        });
        let record = broadcasts.get(result.record.requestId) || result.record;
        if (record.state === "queued" || record.state === "sending") {
          record = await broadcasts.waitForTerminal(record.requestId, config.httpWaitMs) || record;
        }
        writeJson(res, stateStatusCode(record.state), formatRecord(record, config.serviceName));
        return;
      }

      throw new AppError(404, "not_found", "Not found");
    } catch (error) {
      const mapped = error instanceof AppError
        ? error
        : new AppError(500, "internal_error", "Internal server error");
      const headers = mapped.retryAfter ? { "retry-after": String(mapped.retryAfter) } : {};
      writeJson(res, mapped.statusCode, {
        ok: false,
        error: mapped.message,
        error_code: mapped.code,
      }, headers);
    }
  };
}

module.exports = {
  createHttpHandler,
  authenticate,
  readJson,
  validateV1Command,
  formatRecord,
  stateStatusCode,
  V1_BODY_FIELDS,
  LEGACY_BODY_FIELDS,
};
