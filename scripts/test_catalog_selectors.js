// Pins the OneBookShelf probe to the selectors production actually reads.
//
//     node scripts/test_catalog_selectors.js
//
// WHY. A probe testing different selectors than the harvester reports health
// for markup nobody parses. The Amazon work proved this the expensive way: the
// byline selector scored 80% while returning "Kindle Edition" for every sample,
// and a probe checking a different selector would have agreed it was fine.
//
// The Amazon harvester solves this by naming its selectors in one AMZ_SEL block
// that the probe copies verbatim. That approach was tried here and REVERTED,
// because catalog_harvester_extension/background.js has a stronger invariant
// already: scripts/test_dmsguild_dtrpg_tier.js asserts the tier block is
// byte-identical across four copies of the extraction algorithm — the bookmarklet,
// two served utils copies, and the extension. That check is what catches a copy
// silently drifting, which is the bug #149 existed to fix. Naming the selectors
// in only one copy breaks it.
//
// So the probe carries its own copy and this asserts each string appears
// VERBATIM in the extension's source. Same guarantee, without weakening a
// protective invariant for cosmetics.

const fs = require('fs');
const path = require('path');

const BG = fs.readFileSync(
    path.join(__dirname, '..', 'catalog_harvester_extension', 'background.js'), 'utf8');
const PROBE = fs.readFileSync(path.join(__dirname, 'probe_catalog_selectors.js'), 'utf8');

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : `\n         expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`));
}

// Read the probe's declared selectors rather than hard-coding them here, so this
// test cannot drift from the probe either.
function probeSelectors() {
    const a = PROBE.indexOf('const CAT_SEL = {');
    const b = PROBE.indexOf('};', a);
    if (a < 0 || b < 0) throw new Error('CAT_SEL block not found in the probe');
    const block = PROBE.slice(a, b + 2);
    const out = {};
    const re = /(\w+):\s*"((?:[^"\\]|\\.)*)"/g;
    let m;
    while ((m = re.exec(block)) !== null) out[m[1]] = m[2].replace(/\\"/g, '"');
    return out;
}

const sel = probeSelectors();

console.log('\nthe probe declares all five selectors:');
['heading', 'link', 'container', 'price', 'desc'].forEach(k => {
    check('has ' + k, typeof sel[k] === 'string' && sel[k].length > 0, true);
});

console.log('\nevery one appears verbatim in background.js:');
// The selector must appear as a COMPLETE quoted literal, not merely as a
// substring. A plain indexOf() passes for a narrowed selector — "td, th" is a
// substring of "td, th, div.obs-title-card, …" — so a probe testing less than
// production does would have been reported as in sync. background.js mixes
// quote styles, so both are accepted.
function appearsAsLiteral(s) {
    return BG.indexOf('"' + s + '"') !== -1 || BG.indexOf("'" + s + "'") !== -1;
}
Object.keys(sel).sort().forEach(k => {
    check(k + ' -> ' + JSON.stringify(sel[k]).slice(0, 52), appearsAsLiteral(sel[k]), true);
});
check('a narrowed selector would NOT pass (the substring trap)',
    appearsAsLiteral('td, th'), false);

console.log('\nthe tier heading pattern matches production:');
// Built identically in both: ^(Adamantine|...)\s+Metal\s+Products$
check('the tier list is the same seven metals in the same order',
    /\['Adamantine', 'Mithral', 'Platinum', 'Gold', 'Silver', 'Electrum', 'Copper'\]/.test(BG) &&
    /\["Adamantine", "Mithral", "Platinum", "Gold", "Silver", "Electrum", "Copper"\]/.test(PROBE), true);
check('both anchor the heading pattern at both ends',
    /\^\(' \+ tiers\.join\('\|'\) \+ '\)\\\\s\+Metal\\\\s\+Products\$/.test(BG) &&
    /\^\(" \+ TIERS\.join\("\|"\) \+ "\)\\\\s\+Metal\\\\s\+Products\$/.test(PROBE), true);

console.log('\nthe probe mirrors production\'s two price paths:');
check('production has a selector path and a regex fallback',
    /querySelector\(".productSpecialPrice, .cy-prc"\)/.test(BG) &&
    /innerText\.match\(\/\\\$\?\(\[\\d\.\]\+\)\//.test(BG), true);
check('the probe records WHICH path fired',
    /priceVia = "selector"/.test(PROBE) && /priceVia = "regex-fallback"/.test(PROBE), true);
check('...and calls the fallback out as a hazard',
    /HAZARD/.test(PROBE), true);

console.log('\nthe probe reports the placeholder fields as placeholders:');
// These are hard-coded in the extractor, not read from the page. A probe that
// stayed silent about them would let a reader assume they are captured data.
check('production hard-codes rating: 0', /rating: 0,/.test(BG), true);
check('production hard-codes a "(Universal)" publisher',
    /publisher: siteName \+ " \(Universal\)"/.test(BG), true);
check('the probe says rating is never read', /never reads a rating/.test(PROBE), true);
check('the probe says publisher is a placeholder', /not a publisher/.test(PROBE), true);

console.log('\nthe probe is read-only:');
check('it never fetches', /fetch\(/.test(PROBE), false);
check('it never posts anywhere', /ritualKey|X-Ritual-Key/.test(PROBE), false);

console.log('\nit checks for the V9 failure mode by name:');
check('impossible tiers are detected', /impossible/i.test(PROBE), true);
check('...and named as the V9 bug', /V9 bug/.test(PROBE), true);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
