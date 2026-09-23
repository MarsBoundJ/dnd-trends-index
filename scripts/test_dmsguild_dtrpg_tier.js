// Tests for the DMs Guild / DriveThruRPG shelf-tier detection.
//
//   node scripts/test_dmsguild_dtrpg_tier.js
//
// WHY THIS EXISTS. V9 decided a product's metal tier by walking backwards and
// asking "does any preceding element MENTION a metal?". Product titles mention
// metals, so the tier was routinely read off a NEIGHBOURING PRODUCT'S TITLE.
// metal.php has exactly three shelves — Adamantine, Mithral, Platinum — yet
// roughly 40% of every capture came back Gold, Silver or Copper: tiers with no
// section on the page at all. It was wrong from the stream's first run in March
// 2026 through Sep 16 2026.
//
// WHY IT NOW COVERS FOUR FILES. #144 fixed the bookmarklet and only the
// bookmarklet. Three other copies of the same tier logic were left on V9/V10:
// the two in utils/ (one of them the build the Arcane app actually serves) and
// the browser extension, which harvests unattended every Monday at 6am. So the
// fix shipped while the automated harvester kept writing the exact rows the fix
// existed to prevent. Four copies of one algorithm drifted once; this test is
// what stops them drifting again — every copy runs the same behavioural suite,
// and the identity check below asserts they are literally the same code.
//
// The contaminating titles below are not invented. They were read off the live
// DriveThruRPG metal.php page on Sep 16 2026, which is what makes this a
// regression test rather than a guess about what might go wrong.
//
// The code under test is sliced out of each source rather than copied here,
// for the same reason as the AO3 tests: a copy drifts, and a green test against
// stale code is worse than no test.

const fs = require('fs');
const path = require('path');

const REPO = path.join(__dirname, '..');

// Every file carrying the tier algorithm. The extension's copy lives inside a
// function that chrome.scripting serializes, so it sits one indent deeper —
// hence the per-target markers rather than one global pair.
const TARGETS = [
  {
    name: 'scripts/dmsguild_dtrpg_bookmarklet.js',
    file: path.join(REPO, 'scripts/dmsguild_dtrpg_bookmarklet.js'),
    start: '    const tiers = [',
    end: '    // --- UNIFIED HARVEST ---'
  },
  {
    name: 'utils/dmsguild_incursion_mini.js  (served by the Arcane app)',
    file: path.join(REPO, 'utils/dmsguild_incursion_mini.js'),
    start: '    const tiers = [',
    end: '    // --- UNIFIED HARVEST ---'
  },
  {
    name: 'utils/dmsguild_incursion.js',
    file: path.join(REPO, 'utils/dmsguild_incursion.js'),
    start: '    const tiers = [',
    end: '    // --- UNIFIED HARVEST ---'
  },
  {
    name: 'catalog_harvester_extension/background.js  (unattended, Mon 6am)',
    file: path.join(REPO, 'catalog_harvester_extension/background.js'),
    start: '        const tiers = [',
    end: '        // --- UNIFIED HARVEST ---'
  }
];

TARGETS.forEach(function (t) { t.source = fs.readFileSync(t.file, 'utf8'); });

function sliceTierBlock(target) {
  const a = target.source.indexOf(target.start);
  const b = target.source.indexOf(target.end);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error(
      'Could not find the tier block in ' + target.name + '. If it moved, ' +
      'update the markers in TARGETS here — do not delete this test.');
  }
  return target.source.slice(a, b);
}

function buildTierLogic(target, documentStub) {
  // eslint-disable-next-line no-new-func
  const f = new Function(
    'document', 'Node',
    sliceTierBlock(target) + '\nreturn { TIER_HEADING, shelves, findTierForElement };');
  return f(documentStub, { DOCUMENT_POSITION_FOLLOWING: 4 });
}

// ── A deliberately dumb DOM stub ────────────────────────────────────────────
// A page is a flat list of nodes in document order. That is all the tier lookup
// needs: it asks each heading whether a product comes after it.

function makePage(nodes) {
  const els = nodes.map(function (n, i) {
    return {
      index: i,
      innerText: n.text,
      isHeading: !!n.heading,
      compareDocumentPosition: function (other) {
        return other.index > this.index ? 4 /* FOLLOWING */ : 2 /* PRECEDING */;
      }
    };
  });
  const doc = {
    querySelectorAll: function () {
      return els.filter(function (e) { return e.isHeading; });
    }
  };
  return { doc: doc, els: els };
}

let pass = 0;
let fail = 0;
function check(name, actual, expected) {
  const ok = actual === expected;
  if (ok) { pass++; } else { fail++; }
  console.log((ok ? '  ok   ' : '  FAIL ') + name +
    (ok ? '' : '\n         expected ' + JSON.stringify(expected) +
               ', got ' + JSON.stringify(actual)));
}

// The real metal.php layout: three shelves, with contaminating titles sitting
// between a product and its heading.
const PAGE_NODES = [
  { text: 'Metal Legend', heading: false },
  { text: 'Adamantine Metal Products', heading: true },
  { text: 'Daggerheart Corebook' },
  { text: 'Suns of Gold: Merchant Campaigns for Stars Without Number' },
  { text: 'Interface RED Volume 4' },
  { text: 'Mithral Metal Products', heading: true },
  { text: 'Trophy Gold' },
  { text: 'GAZ12 The Golden Khan of Ethengar (Basic)' },
  { text: 'Platinum Metal Products', heading: true },
  { text: 'Dragon Delves: Chapter 9 - A Copper For A Song Battlemaps' },
  { text: 'Torque Borg | Waste of Water' }
];

// ── Behaviour — every copy must pass identically ────────────────────────────

TARGETS.forEach(function (target) {
  console.log('\n══ ' + target.name + ' ══');

  const { TIER_HEADING } = buildTierLogic(target, makePage([]).doc);
  const matches = (s) => TIER_HEADING.test(String(s).trim());

  console.log('\nReal shelf headings must match:');
  ['Adamantine Metal Products', 'Mithral Metal Products', 'Platinum Metal Products',
   'Gold Metal Products', 'Electrum Metal Products', 'Silver Metal Products',
   'Copper Metal Products'].forEach(function (h) {
    check(JSON.stringify(h), matches(h), true);
  });
  check('leading/trailing whitespace is tolerated', matches('  Platinum Metal Products  '), true);
  check('case is tolerated', matches('PLATINUM METAL PRODUCTS'), true);

  console.log('\nReal product titles from the live page must NOT match:');
  [
    'Trophy Gold',
    'Dragon Delves: Chapter 9 - A Copper For A Song Battlemaps',
    'B3 Palace of the Silver Princess (Basic)',
    'Suns of Gold: Merchant Campaigns for Stars Without Number',
    'GAZ12 The Golden Khan of Ethengar (Basic)'
  ].forEach(function (t) {
    check(JSON.stringify(t), matches(t), false);
  });

  console.log('\nNear-misses must NOT match (the pattern is anchored):');
  check('"Platinum Metal Products Bonus"', matches('Platinum Metal Products Bonus'), false);
  check('"Best Platinum Metal Products"', matches('Best Platinum Metal Products'), false);
  check('"Metal Products"', matches('Metal Products'), false);
  check('"Platinum"', matches('Platinum'), false);

  console.log('\nPlacement against a page laid out like the real metal.php:');
  const page = makePage(PAGE_NODES);
  const { findTierForElement, shelves } = buildTierLogic(target, page.doc);
  const tierOf = (i) => findTierForElement(page.els[i]);

  check('three shelves are found', shelves.length, 3);
  check('a product above every heading is unknown, not guessed', tierOf(0), null);
  check('first product under Adamantine', tierOf(2), 'Adamantine');
  check('product titled "Suns of Gold" is Adamantine, not Gold', tierOf(3), 'Adamantine');
  check('product AFTER "Suns of Gold" is still Adamantine', tierOf(4), 'Adamantine');
  check('product titled "Trophy Gold" is Mithral, not Gold', tierOf(6), 'Mithral');
  check('product AFTER "Trophy Gold" is Mithral, not Gold', tierOf(7), 'Mithral');
  check('product AFTER "A Copper For A Song" is Platinum, not Copper', tierOf(10), 'Platinum');
  check('a page with no headings yields no tier',
    buildTierLogic(target, makePage([{ text: 'Some Product' }]).doc).findTierForElement(
      makePage([{ text: 'Some Product' }]).els[0]), null);

  // ── Source-level regression guards ────────────────────────────────────────
  // These fail if the old behaviour is reintroduced. Both scan guards were
  // mutation-tested: pasting the V9 loop back into a copy of the source flips
  // each to false.
  console.log('\nSource guards:');
  const SOURCE = target.source;
  check('the loose "title mentions a metal" scan is gone',
    /for \(const t of tiers\) \{ if \(text\.includes\(t\)\) return t; \}/.test(SOURCE), false);
  check('no querySelector for .infoBoxHeading inside a sibling walk',
    /prev\.querySelector\(['"]\.infoBoxHeading['"]\)/.test(SOURCE), false);
  check('the heading pattern is anchored at both ends',
    SOURCE.indexOf("'^(' + tiers.join('|') + ')\\\\s+Metal\\\\s+Products$'") !== -1, true);
  check('product_url is populated (it was NULL on every row ever shipped)',
    /product_url: url/.test(SOURCE), true);
  check('an unplaced product on a metal page is Unknown, never a guessed tier',
    /isMetalPage \? "Unknown" : "Normal"/.test(SOURCE), true);
  check('no row still ships a pre-V11 version tag',
    /tier, "V(9|10)(-auto)?"/.test(SOURCE), false);
});

// ── Cross-file identity ─────────────────────────────────────────────────────
// The behavioural suite above would stay green if one copy silently regressed
// in a way the stub page happens not to exercise. This is the stronger claim:
// every copy is the SAME code, not merely code that agrees on these cases.
// Indentation is normalised because the extension's copy is nested one level
// deeper inside the injected function.

console.log('\n══ All four copies are the same algorithm ══');
const normalise = (s) => s.split('\n').map((l) => l.trim()).join('\n').trim();
const canonical = normalise(sliceTierBlock(TARGETS[0]));
TARGETS.slice(1).forEach(function (target) {
  check('identical to the bookmarklet: ' + target.name,
    normalise(sliceTierBlock(target)), canonical);
});

// ── The unattended harvester must refuse, not just report ───────────────────
// The bookmarklets draw a tier breakdown and a human decides whether to press
// Transmit. The extension runs at 6am into a background tab with nobody
// watching, so the same contradiction has to block the send instead.

console.log('\n══ The extension refuses an impossible capture ══');
const EXT = TARGETS[3].source;

// The refusal is sliced and RUN, not just grepped. A regex over the source only
// proves the words are present; this proves the branch actually returns a
// failure and stops short of the ingest call.
const GUARD_START = '        const tierCounts = {};';
const GUARD_END = '        let successCount = 0;';

function runGuard(products, shelfTiers, isMetalPage) {
  const a = EXT.indexOf(GUARD_START);
  const b = EXT.indexOf(GUARD_END);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error('Could not find the pre-transmit guard in background.js. If ' +
                    'it moved, update GUARD_START/GUARD_END — do not delete this test.');
  }
  // eslint-disable-next-line no-new-func
  const f = new Function('products', 'shelves', 'isMetalPage', 'siteName',
    EXT.slice(a, b) + '\nreturn { transmitted: true, tierSummary };');
  return f(products, shelfTiers.map((t) => ({ tier: t })), isMetalPage, 'DMs Guild');
}

const rows = (tiers) => tiers.map((t) => ({ seller_tier: t }));
const THREE_SHELVES = ['Adamantine', 'Mithral', 'Platinum'];

check('a clean three-shelf capture is transmitted',
  runGuard(rows(['Adamantine', 'Mithral', 'Platinum']), THREE_SHELVES, true).transmitted, true);

check('a Gold row on a three-shelf page is REFUSED, not filtered',
  runGuard(rows(['Adamantine', 'Gold']), THREE_SHELVES, true).transmitted, undefined);
check('...and the refusal names the offending tier',
  /Gold has no shelf/.test(runGuard(rows(['Adamantine', 'Gold']), THREE_SHELVES, true).error), true);
check('...and reports failure rather than a partial success',
  runGuard(rows(['Adamantine', 'Gold']), THREE_SHELVES, true).success, false);

check('the exact V9 signature (Gold/Silver/Copper) is refused',
  runGuard(rows(['Adamantine', 'Gold', 'Silver', 'Copper']), THREE_SHELVES, true).success, false);

check('Unknown alone does NOT block the send (it is honest, not impossible)',
  runGuard(rows(['Adamantine', 'Unknown']), THREE_SHELVES, true).transmitted, true);

check('a metal page with no shelf headings at all is refused',
  runGuard(rows(['Normal', 'Normal']), [], true).success, false);

// Off a metal page there are no shelves to contradict, so nothing is impossible.
check('a browse page with no shelves transmits normally',
  runGuard(rows(['Normal', 'Normal']), [], false).transmitted, true);
check('a browse page is not second-guessed even with odd tiers',
  runGuard(rows(['Normal', 'Gold']), [], false).transmitted, true);

check('the tier breakdown is carried back to the popup',
  runGuard(rows(['Adamantine', 'Adamantine', 'Mithral']), THREE_SHELVES, true).tierSummary,
  'Adamantine 2, Mithral 1');

check('the refusal happens before any fetch to the ingest endpoint',
  EXT.indexOf('Refused to transmit') < EXT.indexOf('await fetch(endpoint'), true);

// ── The price regex must be anchored, in every copy ─────────────────────────
// The identity check above covers the TIER block only, which ends at the
// "UNIFIED HARVEST" marker. The price logic lives after it and was therefore
// unpinned — all four copies carried the same unanchored regex, and nothing
// would have caught one copy being fixed while three stayed broken.
//
// Measured on dmsguild.com/metal.php 2026-09-23: unanchored, the fallback took
// the first number in the card text, and the card text starts with the TITLE.
// 420 of 1,090 products (38.5%) were priced from a digit in their own title.

console.log('\n══ The price fallback is anchored to a currency symbol in every copy ══');
var ANCHORED = 'match(/\\$\\s*([\\d,]+\\.?\\d*)/)';
var UNANCHORED = 'match(/\\$?([\\d.]+)/)';
TARGETS.forEach(function (target) {
  check('anchored: ' + target.name, target.source.indexOf(ANCHORED) !== -1, true);
  check('unanchored gone: ' + target.name, target.source.indexOf(UNANCHORED) !== -1, false);
});

// The same page proved parseFloat(".") is reachable: "Monster Loot Vol. 3 –
// Mordenkainen's Tome of Foes" matched "." and became $0.00 — a free product.
console.log('\n══ What the anchored pattern does to the measured failures ══');
var RE = /\$\s*([\d,]+\.?\d*)/;
check('a bare "Vol. 3" no longer matches at all', RE.test('Monster Loot Vol. 3'), false);
check('"Monster Loot Vol. 3 $4.95" yields 4.95, not 3',
  'Monster Loot Vol. 3 $4.95'.match(RE)[1], '4.95');
check('"(5e) ... $14.99" yields 14.99, not 5',
  "Minsc and Boo's Journal of Villainy (5e) $14.99".match(RE)[1], '14.99');
check('"80 Maps ... $8.99" yields 8.99, not 80',
  'Tessa Presents 80 Maps $8.99'.match(RE)[1], '8.99');
check('"EB-01 ... $4.99" yields 4.99, not 01',
  'EB-01 The Night Land $4.99'.match(RE)[1], '4.99');
check('thousands separators survive', '$1,299.00'.match(RE)[1], '1,299.00');
// DriveThruRPG, same day: a year in the title became the price.
check('"Update 2022 ... $30.00" yields 30.00, not 2022',
  'Traveller Core Rulebook Update 2022 $30.00'.match(RE)[1], '30.00');
check('"Volume 4 ... $10.00" yields 10.00, not 4',
  'Interface RED Volume 4 $10.00'.match(RE)[1], '10.00');
check('"Second Edition (2E) ... $19.99" yields 19.99, not 2',
  'Knave: Second Edition (2E) $19.99'.match(RE)[1], '19.99');
check('a card with no currency symbol yields nothing rather than a title digit',
  RE.test('Blood Hunter Class for D&D 5e (2020)'), false);

console.log('\n' + pass + ' passed, ' + fail + ' failed\n');
process.exit(fail ? 1 : 0);
