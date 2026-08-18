const test = require("node:test");
const assert = require("node:assert/strict");
const { closeHttpServer, withTimeout } = require("../src/server");

test("withTimeout bounds a stalled startup operation", async () => {
  const started = Date.now();
  await assert.rejects(withTimeout(new Promise(() => {}), 20, "startup"), /startup timed out/);
  assert.ok(Date.now() - started < 500);
});

test("closeHttpServer force-closes lingering connections within the deadline", async () => {
  let idleClosed = 0;
  let allClosed = 0;
  const fakeServer = {
    close() {},
    closeIdleConnections() { idleClosed += 1; },
    closeAllConnections() { allClosed += 1; },
  };
  await closeHttpServer(fakeServer, 20);
  assert.equal(idleClosed, 1);
  assert.equal(allClosed, 1);
});
