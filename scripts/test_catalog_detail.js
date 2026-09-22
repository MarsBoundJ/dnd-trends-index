// Tests for the detail-pass normaliser.
//
//   node scripts/test_catalog_detail.js
//
// WHY THIS EXISTS. Every coercion in normalizeDetail was earned by a measured
// inconsistency in the live API, not anticipated from a schema:
//
//   pagecount is "104" (a STRING) on DriveThruRPG and 27 (a NUMBER) on DMs
//     Guild. Compared raw, the two stores never sort together.
//   The top-level filesize read 0 while files[0].size carried the real bytes.
//     A zero that means "not populated" is the worst kind of value: it is a
//     measurement, so nothing downstream abstains on it.
//   reviewCount is 28 on a product whose star buckets sum to 94. reviewCount
//     counts written REVIEWS; the buckets count RATINGS. Using reviewCount as
//     the denominator of an average is wrong by a factor of three.
//   sku and isbn come back as "" rather than null.
//
// None of that fails loudly. It produces numbers that are simply wrong — the
// same shape as the V9 tier bug and the browse lists. So the normaliser is pure
// and sliced out of detail.js here, rather than tested through a browser.
//
// The fixtures below are trimmed from real responses: DriveThruRPG 535790
// (Single Player Mode) and DMs Guild 339645 (Claus for Concern).

const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(
    path.join(__dirname, '..', 'catalog_harvester_extension', 'detail.js'), 'utf8');

const START = '// ---------- Axis roots';
const END = '// ---------- end normalisation ----------';

function buildNormaliser() {
    const a = SRC.indexOf(START);
    const b = SRC.indexOf(END);
    if (a < 0 || b < 0 || b <= a) {
        throw new Error('Could not find the normalisation block in detail.js. ' +
                        'If it moved, update START/END — do not delete this test.');
    }
    // eslint-disable-next-line no-new-func
    return new Function(SRC.slice(a, b) +
        '\nreturn { normalizeDetail, resolveAxis, toInt, toFloat, toText };')();
}

const { normalizeDetail, resolveAxis, toInt, toText } = buildNormaliser();

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : `\n         expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`));
}

// ── Fixtures ────────────────────────────────────────────────────────────────

const DTRPG = {
    data: {
        attributes: {
            productId: 535790, pagecount: "104", price: 2.5, specialPrice: null,
            lowestDigitalPrice: 7.5, lowestPrintPrice: 20, onSale: false, isPwyw: false,
            rating: "4.8", reviewCount: 6,
            reviewRatings: { average: 4.8, countOne: 0, countTwo: 0, countThree: 0, countFour: 3, countFive: 9 },
            ranking: { humanName: "adamantine" },
            publisherId: 13, isCommunityContent: false, communityContentAuthorId: null,
            creatorLevelId: null, sku: "", isbn: "",
            authors: ["Peter Norton", "J Gray", "James Hutt"], artists: ["Alexander Dudar"],
            editors: [], contributors: [],
            dateCreated: "2025-09-16T13:41:27-05:00", dateAvailable: "2025-09-16T13:41:27-05:00",
            filesize: 0, files: [{ filename: "RTG-CPR.pdf", size: 69000 }],
            ai: false, handmade: true, scannedPDF: false, watermarked: false, isBundle: false,
            description: { name: "Single Player Mode" }
        }
    },
    included: [
        { type: "Publisher", attributes: { name: "R. Talsorian Games Inc." } },
        { type: "Filter", attributes: { filterId: 45755, parentId: 30, ancestors: [30], descriptions: [{ name: "Cyberpunk" }] } },
        { type: "Filter", attributes: { filterId: 510, parentId: 500, ancestors: [10, 500], descriptions: [{ name: "Cyberpunk" }] } },
        { type: "Filter", attributes: { filterId: 2110, parentId: 2150, ancestors: [20, 2150], descriptions: [{ name: "Campaigns, Adventures & Modules" }] } }
    ]
};

const DMG = {
    data: {
        attributes: {
            productId: 339645, pagecount: 27, price: 3.95, isPwyw: true,
            lowestDigitalPrice: 3.95, lowestPrintPrice: null, onSale: false,
            rating: "4.8", reviewCount: 28,
            reviewRatings: { average: 4.8, countOne: 0, countTwo: 0, countThree: 2, countFour: 18, countFive: 74 },
            ranking: { humanName: "adamantine" },
            publisherId: 8957, isCommunityContent: true,
            communityContentAuthorId: 433429, communityContentAuthorAlias: "B.J. Keeton",
            creatorLevelId: 3, sku: "XMAS-2020", isbn: "",
            authors: ["BJ Keeton"], artists: [], editors: [], contributors: [],
            files: [{ filename: "Claus.pdf", size: 4200 }], filesize: 0,
            ai: false, handmade: true,
            description: { name: "Claus for Concern: A Holiday One-Shot for Christmas" }
        }
    },
    included: [
        { type: "Publisher", attributes: { name: "B.J. Keeton" } },
        { type: "Filter", attributes: { filterId: 45418, parentId: 45393, ancestors: [45341, 45393], descriptions: [{ name: "1st Tier (Levels 1-4)" }] } },
        { type: "Filter", attributes: { filterId: 1000263, parentId: 1000261, ancestors: [45342, 1000261], descriptions: [{ name: "5.5e" }] } },
        { type: "Filter", attributes: { filterId: 45864, parentId: 999999, ancestors: [999999], descriptions: [{ name: "Staff Picks" }] } }
    ]
};

const d = normalizeDetail(DTRPG, "DriveThruRPG", "2026-09-22");
const g = normalizeDetail(DMG, "DMs Guild", "2026-09-22");

// ── The coercions ───────────────────────────────────────────────────────────

console.log('\npagecount — a string on one store, a number on the other:');
check('DriveThruRPG "104" becomes 104', d.page_count, 104);
check('DMs Guild 27 stays 27', g.page_count, 27);
check('both are numbers', typeof d.page_count === "number" && typeof g.page_count === "number", true);
check('absent is null, not 0', toInt(undefined), null);
check('empty string is null, not 0', toInt(""), null);

console.log('\nfile size — the top-level field lies:');
check('reads files[0].size, not filesize', d.file_size_bytes, 69000);
check('not the top-level 0', d.file_size_bytes === 0, false);

console.log('\nrating denominator — reviewCount is NOT it:');
check('DriveThruRPG buckets sum to 12', d.rating_count, 12);
check('...while reviewCount is 6', d.review_count, 6);
check('DMs Guild buckets sum to 94', g.rating_count, 94);
check('...while reviewCount is 28', g.review_count, 28);
check('the two are kept as separate fields', d.rating_count !== d.review_count, true);
check('no ratings at all is null, not 0',
    normalizeDetail({ data: { attributes: {} } }, "DMs Guild", "2026-09-22").rating_count, null);

console.log('\nempty strings are absences, not values:');
check('sku "" becomes null', d.sku, null);
check('isbn "" becomes null', g.isbn, null);
check('a real sku survives', g.sku, "XMAS-2020");

// ── Facts that would be lost by a naive read ────────────────────────────────

console.log('\nthe fields that separate creators from back catalogue:');
check('community flag', g.is_community_content, true);
check('community author id', g.community_author_id, 433429);
check('community author alias', g.community_author_alias, "B.J. Keeton");
check('creator level', g.creator_level_id, 3);
check('a publisher product reports false, not null', d.is_community_content, false);

console.log('\nPWYW travels with the price:');
check('flag captured', g.is_pwyw, true);
check('price is the SUGGESTED 3.95, not 0', g.price, 3.95);
check('a fixed-price product is not flagged', d.is_pwyw, false);

console.log('\ntier comes from the API, not a shelf heading:');
check('DriveThruRPG', d.ranking_tier, "adamantine");
check('DMs Guild', g.ranking_tier, "adamantine");
check('an unranked product is null, never "Normal"',
    normalizeDetail({ data: { attributes: {} } }, "DMs Guild", "2026-09-22").ranking_tier, null);

console.log('\nAI disclosure:');
check('ai flag', g.ai_disclosed, false);
check('handmade flag', g.handmade, true);

// ── Filters and axis resolution ─────────────────────────────────────────────

console.log('\nfilters resolve to their axis via ancestors:');
const byId = Object.fromEntries(d.filters.map(f => [f.filter_id, f]));
check('45755 Cyberpunk -> rule_system (root 30)', byId[45755].axis, "rule_system");
check('510 Cyberpunk -> genre (root 10)', byId[510].axis, "genre");
check('the two Cyberpunks are different facets', byId[45755].axis !== byId[510].axis, true);
check('2110 -> product_type (root 20)', byId[2110].axis, "product_type");
check('depth comes from the ancestor chain', byId[510].depth, 2);

const gById = Object.fromEntries(g.filters.map(f => [f.filter_id, f]));
check('45418 level band -> product_type', gById[45418].axis, "product_type");
check('1000263 5.5e -> edition', gById[1000263].axis, "edition");
check('...and 5.5e is captured at all', gById[1000263].label, "5.5e");

console.log('\npromotional rails are not taxonomy:');
check('Staff Picks is dropped', gById[45864], undefined);
check('the real filters survive', g.filters.length, 2);  // 3 in the fixture, 1 promotional

console.log('\nan unplaceable filter abstains rather than guessing:');
const odd = normalizeDetail({
    data: { attributes: {} },
    included: [{ type: "Filter", attributes: { filterId: 7, parentId: 8, ancestors: [8], descriptions: [{ name: "Unknown thing" }] } }]
}, "DMs Guild", "2026-09-22");
check('axis is null, not a guess', odd.filters[0].axis, null);
check('but the filter is still stored', odd.filters[0].filter_id, 7);

console.log('\nids are per-store — the same root number means different axes:');
check('10 is genre on DriveThruRPG',
    resolveAxis("DriveThruRPG", { filterId: 999, ancestors: [10] }), "genre");
check('10 means nothing on DMs Guild',
    resolveAxis("DMs Guild", { filterId: 999, ancestors: [10] }), null);

// ── Source guards ───────────────────────────────────────────────────────────

console.log('\nSource guards — detail.js:');
check('groupId and siteId are always sent',
    /groupId=\$\{cfg\.groupId\}&siteId=\$\{cfg\.siteId\}/.test(SRC), true);
check('a failed ingest does NOT advance the cursor',
    /Do NOT advance the cursor[\s\S]{0,400}return;/.test(SRC), true);
check('requests are spaced', /DETAIL_SPACING_MS\s*=\s*\d+/.test(SRC), true);
check('progress is persisted every batch', /detailCursor: cursor/.test(SRC), true);
check('per-product errors are recorded, not swallowed',
    /errors\.push\(\{ productId/.test(SRC), true);
check('taxonomy_version is stamped on every row',
    /taxonomy_version:/.test(SRC), true);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
