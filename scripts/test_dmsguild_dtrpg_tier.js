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
// The contaminating titles below are not invented. They were read off the live
// DriveThruRPG metal.php page on Sep 16 2026, which is what makes this a
// regression test rather than a guess about what might go wrong.
//
// The code under test is sliced out of the bookmarklet rather than copied here,
// for the same reason as the AO3 tests: a copy drifts, and a green test against
// stale code is worse than no test.

const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, 'dmsguild_dtrpg_bookmarklet.js');
const SOURCE = fs.readFileSync(SRC, 'utf8');

const START = '    const tiers = [';
const END = '    // --- UNIFIED HARVEST ---';

function buildTierLogic(documentStub) {
  const a = SOURCE.indexOf(START);
  const b = SOURCE.indexOf(END);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error(
      'Could not find the tier block in dmsguild_dtrpg_bookmarklet.js. If it ' +
      'moved, update START/END here — do not delete this test.');
  }
  // eslint-disable-next-line no-new-func
  const f = new Function(
    'document', 'Node',
    SOURCE.slice(a, b) + '\nreturn { TIER_HEADING, shelves, findTierForElement };');
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

// ── 1. The heading pattern ──────────────────────────────────────────────────

const { TIER_HEADING } = buildTierLogic(makePage([]).doc);
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

// ── 2. Placement on a simulated shelf page ──────────────────────────────────
// Mirrors the real page: three shelves, with contaminating titles sitting
// between a product and its heading.

console.log('\nPlacement against a page laid out like the real metal.php:');
const page = makePage([
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
]);
const { findTierForElement, shelves } = buildTierLogic(page.doc);
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
  buildTierLogic(makePage([{ text: 'Some Product' }]).doc).findTierForElement(
    makePage([{ text: 'Some Product' }]).els[0]), null);

// ── 3. Source-level regression guards ───────────────────────────────────────
// These fail if the old behaviour is reintroduced. Both were mutation-tested:
// pasting the V9 loop back into a copy of the source flips each to false.

console.log('\nSource guards:');
check('the loose "title mentions a metal" scan is gone',
  /for \(const t of tiers\) \{ if \(text\.includes\(t\)\) return t; \}/.test(SOURCE), false);
check('no querySelector for .infoBoxHeading inside a sibling walk',
  SOURCE.indexOf("prev.querySelector('.infoBoxHeading')") !== -1, false);
check('the heading pattern is anchored at both ends',
  SOURCE.indexOf("'^(' + tiers.join('|') + ')\\\\s+Metal\\\\s+Products$'") !== -1, true);
check('product_url is populated (it was NULL on every row ever shipped)',
  /product_url: url/.test(SOURCE), true);
check('an unplaced product on a metal page is Unknown, never a guessed tier',
  /isMetalPage \? "Unknown" : "Normal"/.test(SOURCE), true);

console.log('\n' + pass + ' passed, ' + fail + ' failed\n');
process.exit(fail ? 1 : 0);
