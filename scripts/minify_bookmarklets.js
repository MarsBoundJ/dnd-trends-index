#!/usr/bin/env node
/**
 * Bookmarklet minifier / packer.
 *
 * Regenerates every `scripts/*_bookmarklet.txt` from its `.js` source as a
 * SINGLE-LINE, comment-free `javascript:` URL.
 *
 * Why single-line matters: a bookmarklet URL containing literal newlines can be
 * mangled when pasted into a browser's single-line bookmark URL field. When the
 * newlines are lost, every `//` line comment swallows the code that follows it,
 * and the bookmarklet dies with a silent SyntaxError. That is exactly how the
 * "Amazon D&D Harvest" bookmark got corrupted (its 'https://...' collapsed to
 * 'https:'). Packing to one line with no comments removes the whole class of bug.
 *
 * Deliberately conservative — this is a *packer*, not an optimiser:
 *   - strips comments, collapses whitespace between tokens
 *   - does NOT rename identifiers or restructure code
 *   - preserves string and template-literal contents BYTE FOR BYTE, so the HTML
 *     emitted into the relay popups is unchanged
 *
 * Usage:
 *   node scripts/minify_bookmarklets.js          # rewrite all .txt files
 *   node scripts/minify_bookmarklets.js --check  # verify only, exit 1 on drift
 */

const fs = require('fs');
const path = require('path');

const SCRIPTS_DIR = __dirname;

// ── Scanner ─────────────────────────────────────────────────────────────────
// A `/` starts a regex literal only when the previous significant character
// cannot end an expression. After an identifier, number, `)`, `]` or `}` it is
// division instead.
function regexAllowed(prev) {
  if (!prev) return true;
  return !/[A-Za-z0-9_$)\]}]/.test(prev);
}

function readString(src, i) {
  const quote = src[i];
  let out = quote;
  i++;
  while (i < src.length) {
    const ch = src[i];
    if (ch === '\\') { out += ch + (src[i + 1] ?? ''); i += 2; continue; }
    out += ch;
    i++;
    if (ch === quote) break;
  }
  return [out, i];
}

// Template literals are copied verbatim, including newlines: those newlines are
// part of the emitted payload (the popup HTML), so collapsing them would change
// behaviour. They survive as %0A after encoding, which keeps the URL one line.
function readTemplate(src, i) {
  let out = '`';
  i++;
  let braceDepth = 0;
  while (i < src.length) {
    const ch = src[i];
    if (ch === '\\') { out += ch + (src[i + 1] ?? ''); i += 2; continue; }
    if (braceDepth === 0 && ch === '`') { out += ch; i++; break; }
    if (ch === '$' && src[i + 1] === '{') { out += '${'; i += 2; braceDepth++; continue; }
    if (braceDepth > 0) {
      if (ch === '{') braceDepth++;
      else if (ch === '}') braceDepth--;
      else if (ch === '`') { const [inner, ni] = readTemplate(src, i); out += inner; i = ni; continue; }
      else if (ch === "'" || ch === '"') { const [inner, ni] = readString(src, i); out += inner; i = ni; continue; }
    }
    out += ch;
    i++;
  }
  return [out, i];
}

function readRegex(src, i) {
  let out = '/';
  i++;
  let inClass = false;
  while (i < src.length) {
    const ch = src[i];
    if (ch === '\\') { out += ch + (src[i + 1] ?? ''); i += 2; continue; }
    if (ch === '[') inClass = true;
    else if (ch === ']') inClass = false;
    else if (ch === '/' && !inClass) { out += ch; i++; break; }
    out += ch;
    i++;
  }
  while (i < src.length && /[a-z]/i.test(src[i])) { out += src[i]; i++; } // flags
  return [out, i];
}

// A separating space is only ever REQUIRED between two tokens that would
// otherwise glue into one. Deciding this per-gap (rather than post-processing
// the whole string) is essential: a global regex would also eat spaces inside
// string literals, silently corrupting data like 'Games & Accessories'.
const isWord = ch => !!ch && /[A-Za-z0-9_$]/.test(ch);
function needSpace(prev, next) {
  if (!prev || !next) return false;
  if (isWord(prev) && isWord(next)) return true;          // `const x`, `new Foo`
  if ('+-'.includes(prev) && '+-'.includes(next)) return true; // `a + +b` ≠ `a++b`
  if (prev === '/' && next === '/') return true;          // `a / /re/` ≠ a comment
  if (isWord(prev) && (next === '+' || next === '-')) return true; // `case -1`
  return false;
}

function pack(src) {
  let out = '';
  let prev = '';
  let i = 0;
  const nextNonWs = j => { while (j < src.length && /\s/.test(src[j])) j++; return src[j] || ''; };

  while (i < src.length) {
    const c = src[i];
    const c2 = src[i + 1];

    if (c === '/' && c2 === '/') {
      while (i < src.length && src[i] !== '\n' && src[i] !== '\r') i++;
      continue;
    }
    if (c === '/' && c2 === '*') {
      i += 2;
      while (i < src.length && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i += 2;
      if (needSpace(prev, nextNonWs(i))) { out += ' '; prev = ' '; }
      continue;
    }
    if (c === "'" || c === '"') { const [s, ni] = readString(src, i); out += s; prev = c; i = ni; continue; }
    if (c === '`') { const [s, ni] = readTemplate(src, i); out += s; prev = '`'; i = ni; continue; }
    if (c === '/' && regexAllowed(prev)) { const [s, ni] = readRegex(src, i); out += s; prev = '/'; i = ni; continue; }

    if (/\s/.test(c)) {
      while (i < src.length && /\s/.test(src[i])) i++;
      if (needSpace(prev, src[i] || '')) { out += ' '; prev = ' '; }
      continue;
    }

    out += c;
    prev = c;
    i++;
  }
  return out.trim();
}

// ── ASI hazard check ────────────────────────────────────────────────────────
// Joining lines changes meaning if a restricted-production keyword was followed
// by a newline before its operand (`return\n  5`). Flag those rather than
// silently altering behaviour.
function asiHazards(src) {
  const stripped = src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
  const hits = [];
  const re = /\b(return|throw|yield|break|continue)[ \t]*\r?\n[ \t]*([^\s})\]])/g;
  let m;
  while ((m = re.exec(stripped))) hits.push(`${m[1]} → ${m[2]}`);
  return hits;
}

// ── Build ───────────────────────────────────────────────────────────────────
const check = process.argv.includes('--check');
const files = fs.readdirSync(SCRIPTS_DIR).filter(f => f.endsWith('_bookmarklet.js')).sort();

let failures = 0;
const rows = [];

for (const jsFile of files) {
  const jsPath = path.join(SCRIPTS_DIR, jsFile);
  const txtPath = jsPath.replace(/\.js$/, '.txt');
  const src = fs.readFileSync(jsPath, 'utf8');

  const hazards = asiHazards(src);
  const packed = pack(src);
  const url = 'javascript:' + encodeURIComponent(packed);

  // Verification gates
  const problems = [];
  try { new Function(packed); } catch (e) { problems.push('does not parse: ' + e.message); }
  if (/[\r\n]/.test(url)) problems.push('URL still contains a literal newline');
  if (decodeURIComponent(url.slice('javascript:'.length)) !== packed) problems.push('round-trip decode mismatch');
  if (hazards.length) problems.push('ASI hazard: ' + hazards.join(', '));
  // Sanity: critical literals must survive intact
  for (const needle of ['bouncer-api', 'X-Ritual-Key']) {
    if (src.includes(needle) && !packed.includes(needle)) problems.push(`lost literal "${needle}"`);
  }
  if (src.includes('https://') && !packed.includes('https://')) problems.push('lost "https://" (comment-strip damage)');

  const oldLen = fs.existsSync(txtPath) ? fs.readFileSync(txtPath, 'utf8').trim().length : 0;

  if (problems.length) {
    failures++;
    rows.push({ file: jsFile, status: 'FAIL', detail: problems.join(' | '), oldLen, newLen: url.length });
    continue;
  }

  if (!check) fs.writeFileSync(txtPath, url, 'utf8');
  rows.push({ file: jsFile, status: check ? 'ok' : 'written', detail: '', oldLen, newLen: url.length });
}

const pad = (s, n) => String(s).padEnd(n);
console.log(pad('bookmarklet', 34) + pad('status', 9) + pad('old', 8) + pad('new', 8) + 'change');
for (const r of rows) {
  const delta = r.oldLen ? Math.round(((r.newLen - r.oldLen) / r.oldLen) * 100) + '%' : '-';
  console.log(pad(r.file, 34) + pad(r.status, 9) + pad(r.oldLen, 8) + pad(r.newLen, 8) + delta + (r.detail ? '  ← ' + r.detail : ''));
}
process.exit(failures ? 1 : 0);
