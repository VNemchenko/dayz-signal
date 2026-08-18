const test = require("node:test");
const assert = require("node:assert/strict");
const { toGameAscii } = require("../src/transliterate");

test("transliterates Russian text deterministically", () => {
  assert.equal(toGameAscii("Через 10 минут перезапуск сервера"), "Cherez 10 minut perezapusk servera");
  assert.equal(toGameAscii("Ёж, щит, подъём, Юлия, ЯМА"), "Yozh, shchit, podyom, Yuliya, YAMA");
});

test("preserves all-caps words and title-case digraphs", () => {
  assert.equal(toGameAscii("Жизнь ЩИТ ЧС"), "Zhizn SHCHIT CHS");
});

test("normalizes punctuation, whitespace and unsupported symbols", () => {
  assert.equal(toGameAscii("  «Тест»\r\n— дождь… №5 🌧️  "), "Test - dozhd... No.5");
});

test("keeps printable ASCII and drops control or non-ASCII leftovers", () => {
  assert.equal(toGameAscii("SOS\u0000\t café 中文"), "SOS caf");
});

test("normalizes compatibility characters with NFKC", () => {
  assert.equal(toGameAscii("ＡＢＣ １２３"), "ABC 123");
});
