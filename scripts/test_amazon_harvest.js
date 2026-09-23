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
        '\nreturn { amzInt, amzFloat, amzText, amazonRankTier, amzStars, amzPrice,' +
        ' pickByline, amzIsNotAByline, buildAmazonRows, dedupeByAsin };')();
}

const { amzInt, amzFloat, amzText, amazonRankTier, amzStars, amzPrice,
        pickByline, amzIsNotAByline, buildAmazonRows, dedupeByAsin } = buildNormaliser();

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : `\n         expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`));
}

const DAY = '2026-09-23';

// A card with every field present, as Amazon renders one.
// Shaped like a real card as captured 2026-09-23 from the D&D Books
// best-seller list: clamp lines in DOM order, one aria-label carrying both
// rating and review count.
function card(over) {
    return Object.assign({
        asin: 'B0CXYZ1234',
        title: 'The Dread from the Drows: Book 2 of The Nosam Chronicles',
        lines: [
            'The Dread from the Drows: Book 2 of The Nosam Chronicles',
            'Erich Sanchack',
            'Kindle Edition'
        ],
        rankText: '#3',
        priceText: '$29.99',
        starsLabel: '4.8 out of 5 stars, 12,431 ratings',
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

console.log('\nrating and review come from ONE aria-label:');
const REAL = '4.8 out of 5 stars, 3,762 ratings';   // measured, Player's Handbook card
check('rating parses to 4.8', amzStars(REAL).rating, 4.8);
check('review count parses to 3762', amzStars(REAL).reviews, 3762);
check('NOT 4.853762 (what stripping every non-digit gives)',
    amzStars(REAL).rating === 4.853762, false);
check('a bare strip really would mangle it', amzFloat(REAL), 4.853762);
check('"5.0 out of 5 stars, 3 ratings" -> 5.0 / 3', 
    [amzStars('5.0 out of 5 stars, 3 ratings').rating, amzStars('5.0 out of 5 stars, 3 ratings').reviews], [5, 3]);
check('a label with no count yields a rating and a null count',
    [amzStars('4.8 out of 5 stars').rating, amzStars('4.8 out of 5 stars').reviews], [4.8, null]);
check('no label at all is two nulls',
    [amzStars(null).rating, amzStars(null).reviews], [null, null]);
check('the built row carries both', 
    [buildAmazonRows(card(), DAY).catalog.rating, buildAmazonRows(card(), DAY).rankRow.review_count],
    [4.8, 12431]);
check('a card with no stars abstains on both',
    [buildAmazonRows(card({ starsLabel: null }), DAY).catalog.rating,
     buildAmazonRows(card({ starsLabel: null }), DAY).rankRow.review_count], [null, null]);

console.log('\nbyline — title and byline are the SAME element class, told apart by order:');
check('the second line is the byline', buildAmazonRows(card(), DAY).catalog.publisher, 'Erich Sanchack');
check('"Kindle Edition" is never the byline', amzIsNotAByline('Kindle Edition'), true);
check('"Hardcover" is never the byline', amzIsNotAByline('Hardcover'), true);
check('"Paperback" is never the byline', amzIsNotAByline('Paperback'), true);
check('"2 formats available" is never the byline', amzIsNotAByline('2 formats available'), true);
check('"18 pts" is never the byline', amzIsNotAByline('18 pts'), true);
check('a person is a byline', amzIsNotAByline('Erich Sanchack'), false);

// The measured case that matters: the 2024 Player's Handbook card has ONE
// clamp line and no byline at all. The old selector returned "Kindle Edition"
// for cards like this; the correct answer is null.
const noByline = buildAmazonRows(card({
    title: "Dungeons & Dragons 2024 Player's Handbook (D&D Core Rulebook)",
    lines: ["Dungeons & Dragons 2024 Player's Handbook (D&D Core Rulebook)"]
}), DAY);
check('a card with no byline yields null, not a format', noByline.catalog.publisher, null);
check('...and its title still parses', noByline.catalog.title.indexOf('Player') !== -1, true);

const formatOnly = buildAmazonRows(card({
    title: 'Some Title',
    lines: ['Some Title', 'Hardcover', '2 formats available']
}), DAY);
check('a card whose only extra lines are formats yields null',
    formatOnly.catalog.publisher, null);
check('pickByline skips a repeat of the title',
    pickByline(['T', 'T', 'Real Person'], 'T'), 'Real Person');

console.log('\nprice must look like money:');
check('"$39.99" -> 39.99', amzPrice('$39.99'), 39.99);
check('"18 pts" is NOT a price', amzPrice('18 pts'), null);
check('a bare number is not a price', amzPrice('18'), null);
check('missing price is null', amzPrice(null), null);
check('the row abstains on Kindle points',
    buildAmazonRows(card({ priceText: '18 pts' }), DAY).catalog.price, null);
check('...and its price_cents too',
    buildAmazonRows(card({ priceText: '18 pts' }), DAY).rankRow.price_cents, null);

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

// ── The probe must test what production reads ───────────────────────────────
// A probe that checks different selectors than the harvester reports health for
// markup nobody parses. Rather than trust a comment saying "keep these in sync",
// assert it.

console.log('\nthe DevTools probe and the harvester share one selector block:');
const PROBE = fs.readFileSync(path.join(__dirname, 'probe_amazon_selectors.js'), 'utf8');

function selectorBlock(src, label) {
    const a = src.indexOf('const AMZ_SEL = {');
    const b = src.indexOf('};', a);
    if (a < 0 || b < 0) throw new Error('AMZ_SEL block not found in ' + label);
    // Normalise indentation only — the selector strings themselves must match.
    return src.slice(a, b + 2).split('\n').map(l => l.trim()).join('\n');
}

check('amazon.js and the probe carry byte-identical selectors',
    selectorBlock(SRC, 'amazon.js') === selectorBlock(PROBE, 'probe'), true);
check('the block is not empty', selectorBlock(SRC, 'amazon.js').length > 100, true);
check('the probe reads the page rather than fetching it',
    /fetch\(/.test(PROBE), false);

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
check('the price selector excludes .a-color-price (it carries "18 pts")',
    /price:\s*"\[class\*=\\"p13n-sc-price/.test(SRC) && !/price:.*a-color-price/.test(SRC), true);
check('the stars selector is not restricted to <span>',
    /stars:\s*"\[aria-label\*=\\"out of 5/.test(SRC), true);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
