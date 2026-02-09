const express = require("express");
const dotenv = require("dotenv");
const { DayzRconClient } = require("./rconClient");

dotenv.config();

function readNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function classifyError(err) {
  const message = err && err.message ? err.message : String(err);
  const lower = message.toLowerCase();

  if (lower.includes("message is required")) {
    return { code: 400, message };
  }
  if (lower.includes("timed out")) {
    return { code: 504, message };
  }
  if (
    lower.includes("not connected") ||
    lower.includes("connecting") ||
    lower.includes("disconnected") ||
    lower.includes("command was not sent")
  ) {
    return { code: 503, message };
  }
  return { code: 500, message };
}

function safeEqual(a, b) {
  const left = Buffer.from(String(a || ""), "utf8");
  const right = Buffer.from(String(b || ""), "utf8");
  if (left.length !== right.length) {
    return false;
  }
  return require("crypto").timingSafeEqual(left, right);
}

const config = {
  httpHost: process.env.HTTP_HOST || "0.0.0.0",
  httpPort: readNumber(process.env.HTTP_PORT, 8080),
  apiKey: String(process.env.API_KEY || "").trim(),
  rconHost: String(process.env.RCON_HOST || "").trim(),
  rconPort: readNumber(process.env.RCON_PORT, 0),
  rconPassword: String(process.env.RCON_PASSWORD || "").trim(),
  rconConnectionType: process.env.RCON_CONNECTION_TYPE || "udp4",
  rconConnectionTimeoutMs: readNumber(process.env.RCON_CONNECTION_TIMEOUT_MS, 50000),
  rconConnectionIntervalMs: readNumber(process.env.RCON_CONNECTION_INTERVAL_MS, 5000),
  rconKeepAliveMs: readNumber(process.env.RCON_KEEPALIVE_MS, 10000),
  rconCommandTimeoutMs: readNumber(process.env.RCON_COMMAND_TIMEOUT_MS, 7000),
  rconReconnectBaseMs: readNumber(process.env.RCON_RECONNECT_BASE_MS, 1000),
  rconReconnectMaxMs: readNumber(process.env.RCON_RECONNECT_MAX_MS, 30000),
};

if (!config.rconHost || !config.rconPort || !config.rconPassword) {
  throw new Error("RCON_HOST, RCON_PORT and RCON_PASSWORD are required");
}

const rcon = new DayzRconClient({
  host: config.rconHost,
  port: config.rconPort,
  password: config.rconPassword,
  connectionType: config.rconConnectionType,
  connectionTimeoutMs: config.rconConnectionTimeoutMs,
  connectionIntervalMs: config.rconConnectionIntervalMs,
  keepAliveMs: config.rconKeepAliveMs,
  commandTimeoutMs: config.rconCommandTimeoutMs,
  reconnectBaseMs: config.rconReconnectBaseMs,
  reconnectMaxMs: config.rconReconnectMaxMs,
});

const app = express();
app.use(express.json({ limit: "256kb" }));

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    rcon: rcon.status(),
  });
});

function requireApiKey(req, res, next) {
  if (!config.apiKey) {
    return next();
  }

  const headerKey = req.header("x-api-key");
  const auth = String(req.header("authorization") || "");
  const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";
  const provided = headerKey || bearer;

  if (!safeEqual(provided, config.apiKey)) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  return next();
}

app.post("/broadcast", requireApiKey, async (req, res) => {
  const rawMessage = req.body && typeof req.body.message === "string" ? req.body.message : "";
  try {
    const response = await rcon.sendGlobalMessage(rawMessage);
    return res.json({
      ok: true,
      command: "say -1 <message>",
      response,
    });
  } catch (err) {
    const mapped = classifyError(err);
    return res.status(mapped.code).json({ ok: false, error: mapped.message });
  }
});

app.use((_req, res) => {
  res.status(404).json({ error: "Not found" });
});

async function start() {
  await rcon.init();

  const server = app.listen(config.httpPort, config.httpHost, () => {
    // eslint-disable-next-line no-console
    console.log(`HTTP server: http://${config.httpHost}:${config.httpPort}`);
  });

  const shutdown = () => {
    server.close(() => process.exit(0));
    rcon.close();
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("Fatal startup error:", err && err.message ? err.message : err);
  process.exit(1);
});
