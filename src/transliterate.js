const LOWER_MAP = new Map(Object.entries({
  а: "a", б: "b", в: "v", г: "g", д: "d", е: "e", ё: "yo",
  ж: "zh", з: "z", и: "i", й: "y", к: "k", л: "l", м: "m",
  н: "n", о: "o", п: "p", р: "r", с: "s", т: "t", у: "u",
  ф: "f", х: "kh", ц: "ts", ч: "ch", ш: "sh", щ: "shch",
  ъ: "", ы: "y", ь: "", э: "e", ю: "yu", я: "ya",
  і: "i", ї: "yi", є: "ye", ґ: "g", ў: "u",
}));

const PUNCTUATION = new Map(Object.entries({
  "\u2010": "-", "\u2011": "-", "\u2012": "-", "\u2013": "-", "\u2014": "-", "\u2212": "-",
  "\u2018": "'", "\u2019": "'", "\u201A": "'", "\u201B": "'",
  "\u201C": "\"", "\u201D": "\"", "\u201E": "\"", "\u201F": "\"",
  "\u2026": "...", "\u2116": "No.",
}));

function isCyrillicLetter(char) {
  return LOWER_MAP.has(char.toLocaleLowerCase("ru-RU"));
}

function transliterateCyrillic(char, next) {
  const lower = char.toLocaleLowerCase("ru-RU");
  const mapped = LOWER_MAP.get(lower);
  if (mapped === undefined) {
    return null;
  }
  if (char === lower || !mapped) {
    return mapped;
  }
  const nextIsLower = next && isCyrillicLetter(next) && next === next.toLocaleLowerCase("ru-RU");
  return nextIsLower ? mapped[0].toUpperCase() + mapped.slice(1) : mapped.toUpperCase();
}

function toGameAscii(input) {
  const normalized = String(input ?? "").replace(/\u2116/g, "No.").normalize("NFKC");
  const chars = Array.from(normalized);
  let result = "";

  for (let index = 0; index < chars.length; index += 1) {
    const char = chars[index];
    const code = char.codePointAt(0);

    if (/\s/u.test(char) || code < 0x20 || code === 0x7f) {
      result += " ";
      continue;
    }
    if (PUNCTUATION.has(char)) {
      result += PUNCTUATION.get(char);
      continue;
    }
    if (code >= 0x20 && code <= 0x7e) {
      result += char;
      continue;
    }
    const mapped = transliterateCyrillic(char, chars[index + 1]);
    if (mapped !== null) {
      result += mapped;
    }
  }

  return result.replace(/\s+/g, " ").trim();
}

module.exports = { toGameAscii };
