const http = require("node:http");
const { BroadcastService } = require("./broadcastService");
const { loadConfig } = require("./config");
const { createHttpHandler } = require("./httpApp");
const { JsonlJournal } = require("./journal");
const { DayzRconClient } = require("./rconClient");

function log(event, fields = {}) {
  process.stdout.write(`${JSON.stringify({ timestamp: new Date().toISOString(), event, ...fields })}\n`);
}

function withTimeout(promise, timeoutMs, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function listen(server, port, host, timeoutMs) {
  return withTimeout(new Promise((resolve, reject) => {
    const onError = (error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, host);
  }), timeoutMs, "HTTP listen");
}

function closeHttpServer(server, timeoutMs) {
  return new Promise((resolve) => {
    let settled = false;
    let timer;
    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    server.close(finish);
    if (typeof server.closeIdleConnections === "function") {
      server.closeIdleConnections();
    }
    timer = setTimeout(() => {
      if (typeof server.closeAllConnections === "function") {
        server.closeAllConnections();
      }
      finish();
    }, Math.max(1, timeoutMs));
  });
}

async function start() {
  const config = loadConfig();
  let shuttingDown = false;
  const rcon = new DayzRconClient(config.rcon);
  const journal = new JsonlJournal(config.broadcasts.journalPath, {
    retentionMs: config.broadcasts.retentionMs,
    maxRecords: config.broadcasts.maxRecords,
    maxBytes: config.broadcasts.maxBytes,
  });
  const broadcasts = new BroadcastService({ rcon, journal, config: config.broadcasts });

  try {
    await withTimeout(broadcasts.init(), config.startupTimeoutMs, "journal startup");
    await withTimeout(rcon.init(), config.startupTimeoutMs, "RCON startup");
  } catch (error) {
    rcon.close();
    try {
      await withTimeout(broadcasts.close(1000), 2000, "startup cleanup");
    } catch (_cleanupError) {
      // The original startup error is the actionable failure.
    }
    throw error;
  }

  broadcasts.on("update", (record) => {
    log("broadcast_state", {
      service: config.serviceName,
      request_id: record.requestId,
      state: record.state,
      queue_depth: broadcasts.queueDepth(),
      latency_ms: Date.now() - (record.acceptedAt || record.createdAt),
      error_code: record.errorCode || undefined,
    });
  });
  let pendingFatal = false;
  let shutdown = async () => { pendingFatal = true; };
  broadcasts.on("fatal", (error) => {
    log("broadcast_fatal", { service: config.serviceName, error_code: "journal_or_queue_failure", detail: error.message });
    void shutdown("fatal");
  });
  rcon.on("state", (status) => {
    log("rcon_state", {
      service: config.serviceName,
      state: status.state,
      error_code: status.lastErrorCode || undefined,
      reconnect_attempts: status.reconnectAttempts,
    });
  });

  const handler = createHttpHandler({ config, broadcasts, rcon, isShuttingDown: () => shuttingDown });
  const server = http.createServer(handler);
  server.requestTimeout = Math.max(5000, config.httpWaitMs + 2000);
  server.headersTimeout = 5000;
  server.keepAliveTimeout = 5000;
  server.on("clientError", (_error, socket) => {
    if (socket.writable) {
      socket.end("HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n");
    }
  });

  try {
    await listen(server, config.httpPort, config.httpHost, config.startupTimeoutMs);
  } catch (error) {
    await Promise.allSettled([
      closeHttpServer(server, 1000),
      withTimeout(broadcasts.close(1000), 2000, "listen failure cleanup"),
    ]);
    throw error;
  }
  log("http_started", { service: config.serviceName, host: config.httpHost, port: config.httpPort });

  let shutdownPromise;
  shutdown = (reason) => {
    if (shutdownPromise) {
      return shutdownPromise;
    }
    shuttingDown = true;
    shutdownPromise = (async () => {
      log("shutdown_started", { service: config.serviceName, reason });
      const results = await Promise.allSettled([
        closeHttpServer(server, config.shutdownGraceMs),
        withTimeout(
          broadcasts.close(config.shutdownGraceMs),
          Math.max(1000, config.shutdownGraceMs + 1000),
          "broadcast shutdown",
        ),
      ]);
      const failed = results.filter((result) => result.status === "rejected");
      if (failed.length > 0) {
        process.exitCode = 1;
        log("shutdown_incomplete", {
          service: config.serviceName,
          error_code: "shutdown_timeout_or_failure",
          failure_count: failed.length,
        });
      } else {
        log("shutdown_complete", { service: config.serviceName });
      }
    })();
    return shutdownPromise;
  };

  if (pendingFatal) {
    void shutdown("fatal_during_startup");
  }

  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  return { server, broadcasts, rcon, shutdown };
}

if (require.main === module) {
  start().catch((error) => {
    process.stderr.write(`${JSON.stringify({
      timestamp: new Date().toISOString(),
      event: "startup_failed",
      error: error.message,
    })}\n`);
    process.exitCode = 1;
  });
}

module.exports = { start, withTimeout, closeHttpServer };
