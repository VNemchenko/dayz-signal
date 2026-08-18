const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const localFixturePath = path.join(__dirname, "fixtures", "n8n-command.json");
const adjacentFixturePath = path.resolve(__dirname, "..", "..", "dayz-log-monitor", "n8n", "fixtures", "signal", "command.json");

test("checked-in signal fixture matches the adjacent n8n command fixture", {
  skip: !fs.existsSync(adjacentFixturePath),
}, () => {
  const local = JSON.parse(fs.readFileSync(localFixturePath, "utf8"));
  const adjacent = JSON.parse(fs.readFileSync(adjacentFixturePath, "utf8"));
  assert.deepEqual(local, adjacent);
});
