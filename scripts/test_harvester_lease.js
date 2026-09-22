// Tests for the catalog harvester's in-progress lease.
//
//   node scripts/test_harvester_lease.js
//
// WHY THIS EXISTS. On Sep 22 2026 the extension ran for the first time. DMs
// Guild harvested cleanly — 1090 products, tiers correct — and then Chrome
// evicted the Manifest V3 service worker while DriveThruRPG was loading. The
// promise tracking the run, the tabs.onUpdated listener and the setTimeout that
// would have failed the site all died with the worker.
//
// What survived was `harvestInProgress: true` in chrome.storage, which survives
// even an extension reload. Nothing left alive could clear it, and the popup
// gated Run Now on that raw flag, so the extension sat at "Harvest in progress"
// permanently. The only way out was writing to chrome.storage by hand from the
// service-worker console. A harvester that bricks itself whenever Chrome does a
// completely routine thing is not a harvester you can leave on a schedule.
//
// The fix is to stop treating the flag as the answer. It is a LEASE, it carries
// the time it was taken, and a lease older than HARVEST_STALE_MS is a dead run
// rather than a live one. evaluateLease is pure precisely so this rule can be
// tested here instead of by waiting a week to see what Monday does.
//
// The logic is sliced out of background.js rather than copied, for the same
// reason as the tier tests: a copy drifts, and a green test against stale code
// is worse than no test.

const fs = require('fs');
const path = require('path');

const REPO = path.join(__dirname, '..');
const BG = path.join(REPO, 'catalog_harvester_extension/background.js');
const POPUP_JS = path.join(REPO, 'catalog_harvester_extension/popup.js');
const POPUP_HTML = path.join(REPO, 'catalog_harvester_extension/popup.html');

const SOURCE = fs.readFileSync(BG, 'utf8');
const POPUP = fs.readFileSync(POPUP_JS, 'utf8');
const HTML = fs.readFileSync(POPUP_HTML, 'utf8');

const START = '// ---------- Harvest lease ----------';
const END = '// ---------- end harvest lease ----------';

function buildLease() {
  const a = SOURCE.indexOf(START);
  const b = SOURCE.indexOf(END);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error(
      'Could not find the lease block in background.js. If it moved, update ' +
      'START/END here — do not delete this test.');
  }
  // eslint-disable-next-line no-new-func
  return new Function(SOURCE.slice(a, b) + '\nreturn { evaluateLease, HARVEST_STALE_MS };')();
}

const { evaluateLease, HARVEST_STALE_MS } = buildLease();

let pass = 0;
let fail = 0;
function check(name, actual, expected) {
  const ok = actual === expected;
  if (ok) { pass++; } else { fail++; }
  console.log((ok ? '  ok   ' : '  FAIL ') + name +
    (ok ? '' : '\n         expected ' + JSON.stringify(expected) +
               ', got ' + JSON.stringify(actual)));
}

const NOW = 1_800_000_000_000; // an arbitrary fixed "now"
const at = (ageMs) => evaluateLease({ harvestInProgress: true, harvestStartedAt: NOW - ageMs }, NOW);

// ── No lease held ───────────────────────────────────────────────────────────

console.log('\nNothing held:');
check('empty storage is not held', evaluateLease({}, NOW).held, false);
check('empty storage is not running', evaluateLease({}, NOW).running, false);
check('null storage does not throw', evaluateLease(null, NOW).held, false);
check('an explicitly false flag is not held',
  evaluateLease({ harvestInProgress: false, harvestStartedAt: NOW }, NOW).held, false);

// ── A live run ──────────────────────────────────────────────────────────────

console.log('\nA run that really is in flight:');
check('just started → running', at(0).running, true);
check('just started → not stale', at(0).stale, false);
check('one minute in → running', at(60 * 1000).running, true);
check('a second inside the window → running', at(HARVEST_STALE_MS - 1000).running, true);
check('exactly at the window → still running', at(HARVEST_STALE_MS).running, true);

// ── A dead run ──────────────────────────────────────────────────────────────

console.log('\nA run nothing is behind any more:');
check('a second past the window → not running', at(HARVEST_STALE_MS + 1000).running, false);
check('a second past the window → stale', at(HARVEST_STALE_MS + 1000).stale, true);
check('an hour old → not running', at(60 * 60 * 1000).running, false);
check('a day old → not running', at(24 * 60 * 60 * 1000).running, false);
check('a stale lease is still HELD (so it can be cleaned up, not ignored)',
  at(60 * 60 * 1000).held, true);

// ── The exact shape that bricked it ─────────────────────────────────────────
// This is what was actually sitting in chrome.storage on Sep 22: the flag set,
// no timestamp, because the build that wrote it had no concept of one.

console.log('\nThe Sep 22 storage state — flag set, no timestamp:');
const legacy = evaluateLease({ harvestInProgress: true }, NOW);
check('is held', legacy.held, true);
check('is NOT running', legacy.running, false);
check('is stale', legacy.stale, true);
check('a null timestamp is treated the same',
  evaluateLease({ harvestInProgress: true, harvestStartedAt: null }, NOW).running, false);
check('a non-numeric timestamp is treated the same',
  evaluateLease({ harvestInProgress: true, harvestStartedAt: '1800000000000' }, NOW).running, false);
check('a zero timestamp is treated the same',
  evaluateLease({ harvestInProgress: true, harvestStartedAt: 0 }, NOW).running, false);

// ── Clock going backwards ───────────────────────────────────────────────────
// A dead lease must not become un-clearable because the system clock moved.

console.log('\nA clock that moved backwards:');
check('a lease stamped in the future is not running', at(-60 * 1000).running, false);
check('a lease stamped in the future is stale', at(-60 * 1000).stale, true);
check('an hour in the future is still not running', at(-60 * 60 * 1000).running, false);

// ── Source guards ───────────────────────────────────────────────────────────
// The lease alone does not save the harvester: the watchdog has to be an alarm
// (Chrome persists those across an eviction; setTimeout dies with the worker),
// and the paths that start a harvest have to consult the lease.

console.log('\nSource guards — background.js:');
check('a watchdog alarm is created when the lease is taken',
  /chrome\.alarms\.create\(WATCHDOG_ALARM/.test(SOURCE), true);
check('the watchdog is cleared when the lease is released',
  /chrome\.alarms\.clear\(WATCHDOG_ALARM\)/.test(SOURCE), true);
check('the alarm handler acts on the watchdog',
  /alarm\.name === WATCHDOG_ALARM/.test(SOURCE), true);
check('BOTH entry points consult the lease, not just one',
  (SOURCE.match(/if \(lease\.running\)/g) || []).length >= 2, true);
check('the scheduled path refuses to start on top of a live run',
  /already running .{0,40}skipping this fire/.test(SOURCE), true);
check('Run Now refuses to start on top of a live run',
  /"Harvest already in progress"/.test(SOURCE), true);
check('a stale lease is abandoned rather than obeyed',
  /if \(lease\.stale\) await abandonHarvest/.test(SOURCE), true);
check('the lease is stamped with a time when taken',
  /harvestStartedAt: Date\.now\(\)/.test(SOURCE), true);
check('tab ids are persisted, so a fresh worker can close orphaned tabs',
  /harvestTabIds/.test(SOURCE), true);
check('setTimeout is no longer the only thing failing a stuck site',
  SOURCE.indexOf('WATCHDOG_ALARM') !== -1, true);
check('a cancel path exists',
  /msg\.type === "CANCEL"/.test(SOURCE), true);
check('abandoning keeps the results of sites that did finish',
  /finished\.indexOf\(s\.name\) === -1/.test(SOURCE), true);

console.log('\nSource guards — popup:');
check('the popup gates on the computed answer, not the raw flag',
  /status\.harvestActive/.test(POPUP), true);
check('the popup no longer gates Run Now on harvestInProgress',
  /if \(status\.harvestInProgress\)/.test(POPUP), false);
check('the popup surfaces a stuck run',
  /status\.harvestStuck/.test(POPUP), true);
check('a Cancel control exists in the markup',
  /id="cancel-btn"/.test(HTML), true);
check('the popup can send the cancel message',
  /type: "CANCEL"/.test(POPUP), true);

console.log('\n' + pass + ' passed, ' + fail + ' failed\n');
process.exit(fail ? 1 : 0);
