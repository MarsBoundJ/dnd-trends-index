// Tests for the Amazon harvest normaliser.
//
//     node scripts/test_amazon_harvest.js
//
// WHY THIS EXISTS. The bookmarklet this was ported from recorded an unmeasured
// value as zero, and then read meaning into the zero:
//
//     const rank = rankEl ? parseInt(...) || 0 : 0;
//     const rankTier = item.rank <= 10 ? 'Top 10' : ...
//
// A product whose rank badge did not parse was written as rank 0 and tier
// "Top 10" — the top of the scale, asserted about a product we could not rank.
// Nothing crashed and no row was missing; the number was just wrong, in the
// most flattering possible direction. That is the same shape as the V9 DMs
// Guild tier bug, and it is why every check below is about what happens when a
// field is ABSENT rather than when it is present.
//
// The normaliser lives inside the injected extraction function (chrome.scripting
// serializes it, so it cannot reference module scope) and is sliced out of
// amazon.js here rather than copied. A copy drifts.

const fs = require('fs');
const path = require('path');

const SRC = fs.readFileSync(
    path.join(__dirname, '..', 'catalog_harvester_extension', 'amazon.js'), 'utf8');

const START = '// ---------- Amazon normalisation';
const END = '// ---------- end Amazon normalisation ----------';

function buildNormaliser() {
    const a = SRC.indexOf(START);
    const b = SRC.indexOf(END);
    if (a < 0 || b < 0 || b <= a) {
        throw new Error('Could not find the normalisation block in amazon.js. ' +
                        'If it moved, update START/END — do not delete this test.');
    }
    // eslint-disable-next-line no-new-func
    return new Function(SRC.slice(a, b) +
        '\nreturn { amzInt, amzFloat, amzText, amazonRankTier, amzRatingFromLabel,' +
        ' buildAmazonRows, dedupeByAsin };')();
}

const { amzInt, amzFloat, amzText, amazonRankTier, amzRatingFromLabel,
        buildAmazonRows, dedupeByAsin } = buildNormaliser();

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : `\n         expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`));
}

const DAY = '2026-09-23';

// A card with every field present, as Amazon renders one.
function card(over) {
    return Object.assign({
        asin: 'B0CXYZ1234',
        title: "Player's Handbook (2024)",
        rankText: '#3',
        priceText: '$29.99',
        author: 'by Wizards RPG Team',
        ratingLabel: '4.8 out of 5 stars',
        reviewLabel: '12,431 ratings',
        label: 'D&D Books',
        listType: 'Best Sellers'
    }, over || {});
}

// ── The bug this port exists to fix ─────────────────────────────────────────

console.log('\nan unreadable rank has no tier — it is NOT "Top 10":');
const noRank = buildAmazonRows(card({ rankText: null }), DAY);
check('rank is null, not 0', noRank.rank, null);
check('seller_tier is null, not "Top 10"', noRank.catalog.seller_tier, null);
check('an empty badge is also null', buildAmazonRows(card({ rankText: '   ' }), DAY).rank, null);
check('a badge with no digits is null', buildAmazonRows(card({ rankText: '#' }), DAY).rank, null);
check('rank 0 itself earns no tier', amazonRankTier(0), null);
check('a negative rank earns no tier', amazonRankTier(-1), null);

console.log('\na real rank still tiers correctly:');
check('"#3" parses to 3', buildAmazonRows(card(), DAY).rank, 3);
check('...and earns Top 10', buildAmazonRows(card(), DAY).catalog.seller_tier, 'Top 10');
check('1 -> Top 10', amazonRankTier(1), 'Top 10');
check('10 -> Top 10 (boundary)', amazonRankTier(10), 'Top 10');
check('11 -> Top 50', amazonRankTier(11), 'Top 50');
check('50 -> Top 50 (boundary)', amazonRankTier(50), 'Top 50');
check('51 -> Top 100', amazonRankTier(51), 'Top 100');
check('100 -> Top 100 (boundary)', amazonRankTier(100), 'Top 100');
check('101 -> Top 200', amazonRankTier(101), 'Top 200');

// ── Every other absence abstains too ────────────────────────────────────────

console.log('\nprice:');
check('"$29.99" -> 29.99', buildAmazonRows(card(), DAY).catalog.price, 29.99);
check('price_cents is derived, not re-parsed', buildAmazonRows(card(), DAY).rankRow.price_cents, 2999);
const noPrice = buildAmazonRows(card({ priceText: null }), DAY);
check('missing price is null, not 0', noPrice.catalog.price, null);
check('missing price_cents is null, not 0', noPrice.rankRow.price_cents, null);

console.log('\nrating — the aria-label holds two numbers, and only one is the rating:');
check('"4.8 out of 5 stars" -> 4.8', amzRatingFromLabel('4.8 out of 5 stars'), 4.8);
check('NOT 4.85 (what stripping every non-digit would give)',
    amzRatingFromLabel('4.8 out of 5 stars') === 4.85, false);
check('a bare strip really would produce 4.85', amzFloat('4.8 out of 5 stars'), 4.85);
check('missing rating is null, not 0', buildAmazonRows(card({ ratingLabel: null }), DAY).catalog.rating, null);
check('an unparseable label is null', amzRatingFromLabel('stars'), null);

console.log('\nreview count:');
check('"12,431 ratings" -> 12431', buildAmazonRows(card(), DAY).rankRow.review_count, 12431);
check('missing review count is null, not 0',
    buildAmazonRows(card({ reviewLabel: null }), DAY).rankRow.review_count, null);

console.log('\nauthor byline:');
check('"by Wizards RPG Team" loses the "by"', buildAmazonRows(card(), DAY).catalog.publisher, 'Wizards RPG Team');
check('missing author is null, not ""', buildAmazonRows(card({ author: null }), DAY).catalog.publisher, null);
check('whitespace-only author is null', amzText('   '), null);

console.log('\ncoercion primitives:');
check('amzInt(undefined) is null', amzInt(undefined), null);
check('amzInt("") is null', amzInt(''), null);
check('amzFloat(undefined) is null', amzFloat(undefined), null);
check('amzFloat(".") is null, not NaN', amzFloat('.'), null);

// ── Dedup ───────────────────────────────────────────────────────────────────

console.log('\ndedup keeps the best rank, and null never wins:');
const dupes = [
    buildAmazonRows(card({ asin: 'A1', rankText: '#42' }), DAY),
    buildAmazonRows(card({ asin: 'A1', rankText: '#7' }), DAY),
    buildAmazonRows(card({ asin: 'A1', rankText: null }), DAY)
];
const deduped = dedupeByAsin(dupes);
check('one row survives', deduped.length, 1);
check('it is the lowest rank', deduped[0].rank, 7);

const nullFirst = dedupeByAsin([
    buildAmazonRows(card({ asin: 'A2', rankText: null }), DAY),
    buildAmazonRows(card({ asin: 'A2', rankText: '#9' }), DAY)
]);
check('a real rank displaces a null one', nullFirst[0].rank, 9);

const allNull = dedupeByAsin([
    buildAmazonRows(card({ asin: 'A3', rankText: null }), DAY),
    buildAmazonRows(card({ asin: 'A3', rankText: null }), DAY)
]);
check('two unranked copies still collapse to one', allNull.length, 1);
check('...and it stays unranked rather than becoming 0', allNull[0].rank, null);

check('distinct ASINs are not merged',
    dedupeByAsin([buildAmazonRows(card({ asin: 'A4' }), DAY),
                  buildAmazonRows(card({ asin: 'A5' }), DAY)]).length, 2);

// ── Row shape ───────────────────────────────────────────────────────────────

console.log('\nrow shape:');
const row = buildAmazonRows(card(), DAY);
check('catalog row is tagged Amazon', row.catalog.source, 'Amazon');
check('collected_date is the passed date', row.catalog.collected_date, DAY);
check('rank row carries the same date', row.rankRow.date, DAY);
check('category joins list type and label', row.rankRow.category, 'Best Sellers: D&D Books');
check('rows are tagged V12-ext, not V10-auto',
    row.catalog.tags.indexOf('V12-ext') !== -1, true);
check('the old tag is gone', row.catalog.tags.indexOf('V10-auto'), -1);
check('asin appears on both rows',
    row.catalog.asin === row.rankRow.asin && row.catalog.asin === 'B0CXYZ1234', true);

// ── Source guards ───────────────────────────────────────────────────────────

console.log('\nSource guards — amazon.js:');
check('the extractor refuses to run off amazon.com',
    /hostname\.endsWith\("amazon\.com"\)/.test(SRC), true);
check('both destination endpoints are posted',
    /postChunks\(catalogEndpoint/.test(SRC) && /postChunks\(ranksEndpoint/.test(SRC), true);
check('a failed ingest throws rather than counting as sent',
    /if \(!res\.ok\) throw new Error\("ingest HTTP/.test(SRC), true);
check('unranked products are reported, not hidden',
    /unrankedProducts:/.test(SRC), true);
check('the ranks endpoint is derived from ENDPOINT, not retyped',
    /ENDPOINT\.replace\("system\/library\/ingest-catalog", "system\/amazon\/ingest-ranks"\)/.test(SRC), true);
check('parse returns raw text, leaving coercion to the pure block',
    /rankText: rankEl \? rankEl\.textContent : null/.test(SRC), true);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
