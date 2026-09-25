// Tests for the Kickstarter and BackerKit normalisers.
//
//     node scripts/test_crowdfunding_harvest.js
//
// WHY THESE TWO SHARE A SUITE. They are the only streams in the project that
// carry CARDINAL data — real dollars pledged, real backer counts. Every other
// stream is a position on a leaderboard. That makes a zero standing in for
// "unknown" worse here than anywhere else: a project that raised nothing and a
// project whose funding we failed to read are different facts, and summing them
// gives the same wrong answer either way.
//
// Both bookmarklets zero unmeasured values, and Kickstarter's additionally
// invents two defaults outright — an unknown state becomes "live", an unknown
// category becomes "Tabletop Games".
//
// The wire format is deliberately NOT changed by this port (see the headers of
// kickstarter.js and backerkit.js: sending null risks silent row drops on a
// REQUIRED BigQuery column, and BackerKit's bouncer route re-zeroes nulls
// anyway). So what these tests pin down is the MEASUREMENT: every coercion
// returns null internally, `defaulted` records which fields fell back, and the
// run reports the totals. The fabrications become countable before anyone
// argues about changing them.

const fs = require('fs');
const path = require('path');

const EXT = path.join(__dirname, '..', 'catalog_harvester_extension');
const KS_SRC = fs.readFileSync(path.join(EXT, 'kickstarter.js'), 'utf8');
const BK_SRC = fs.readFileSync(path.join(EXT, 'backerkit.js'), 'utf8');

function slice(src, startMark, endMark, exports, label) {
    const a = src.indexOf(startMark);
    const b = src.indexOf(endMark);
    if (a < 0 || b < 0 || b <= a) {
        throw new Error('Could not find the normalisation block in ' + label +
                        '. If it moved, update the markers — do not delete this test.');
    }
    // eslint-disable-next-line no-new-func
    return new Function(src.slice(a, b) + '\nreturn { ' + exports.join(', ') + ' };')();
}

const ks = slice(KS_SRC,
    '// ---------- Kickstarter normalisation',
    '// ---------- end Kickstarter normalisation ----------',
    ['ksDecodeId', 'ksAmount', 'ksDeadline', 'ksIsDndCentric', 'ksBuildRow', 'ksSummarise'],
    'kickstarter.js');

const bk = slice(BK_SRC,
    '// ---------- BackerKit normalisation',
    '// ---------- end BackerKit normalisation ----------',
    ['bkIsUsd', 'bkNum', 'bkMoney', 'bkBackers', 'bkTiming', 'bkRank',
     'bkIdsFromUrl', 'bkPrettySlug', 'bkClassify', 'bkBuildRow', 'bkSummarise'],
    'backerkit.js');

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : `\n         expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`));
}

// ════════════════════════ KICKSTARTER ════════════════════════

// A node as the GraphQL API returns one.
function ksNode(over) {
    return Object.assign({
        id: Buffer.from('Project-1234567').toString('base64'),
        name: 'Shadow of the Weird Wizard',
        description: 'A 5e compatible campaign setting for tabletop RPG play',
        state: 'LIVE',
        deadlineAt: 1790000000,
        backersCount: 1842,
        goal: { amount: '25000.00', currency: 'USD' },
        pledged: { amount: '187500.50', currency: 'USD' },
        creator: { name: 'Schwalb Entertainment' },
        url: 'https://www.kickstarter.com/projects/x/y',
        category: { name: 'Tabletop Games' }
    }, over || {});
}

console.log('\nKickstarter — the project id is base64 "Project-N":');
check('decodes to the numeric id', ks.ksDecodeId(Buffer.from('Project-1234567').toString('base64')), 1234567);
check('garbage decodes to null, not 0', ks.ksDecodeId('!!!not base64!!!'), null);
check('empty decodes to null', ks.ksDecodeId(''), null);
check('a node without an id is skipped entirely', ks.ksBuildRow({ id: null }), null);

console.log('\nKickstarter — money is the only cardinal data in the project:');
check('"187500.50" parses', ks.ksAmount('187500.50'), 187500.5);
check('a currency-prefixed amount parses', ks.ksAmount('$25,000.00'), 25000);
check('absent is null, NOT 0', ks.ksAmount(undefined), null);
check('empty is null, NOT 0', ks.ksAmount(''), null);
check('unparseable is null, NOT 0', ks.ksAmount('n/a'), null);
check('a real zero is still zero', ks.ksAmount('0'), 0);

const noMoney = ks.ksBuildRow(ksNode({ pledged: null, goal: null, backersCount: null }));
check('missing money is RECORDED as defaulted',
    noMoney.defaulted.indexOf('pledged_usd') !== -1 &&
    noMoney.defaulted.indexOf('goal_usd') !== -1 &&
    noMoney.defaulted.indexOf('backers_count') !== -1, true);
check('...even though the wire value stays 0 (deliberate — see the header)',
    [noMoney.row.pledged_usd, noMoney.row.goal_usd, noMoney.row.backers_count], [0, 0, 0]);
check('a project that genuinely raised 0 is NOT marked defaulted',
    ks.ksBuildRow(ksNode({ pledged: { amount: '0' } })).defaulted.indexOf('pledged_usd'), -1);

console.log('\nKickstarter — the two invented defaults are now countable:');
const noState = ks.ksBuildRow(ksNode({ state: null, category: null }));
check('an unknown state is recorded as defaulted', noState.defaulted.indexOf('status') !== -1, true);
check('an unknown category is recorded as defaulted', noState.defaulted.indexOf('category') !== -1, true);
check('the wire value still says "live" (unchanged contract)', noState.row.status, 'live');
check('the wire value still says "Tabletop Games"', noState.row.category, 'Tabletop Games');
check('a real state is not flagged', ks.ksBuildRow(ksNode()).defaulted.indexOf('status'), -1);
check('a real state is lowercased', ks.ksBuildRow(ksNode()).row.status, 'live');

console.log('\nKickstarter — percent_funded needs a goal to mean anything:');
check('187500.50 of 25000 -> 750%', ks.ksBuildRow(ksNode()).row.percent_funded, 750);
check('a zero goal yields 0, not Infinity',
    ks.ksBuildRow(ksNode({ goal: { amount: '0' } })).row.percent_funded, 0);
check('a missing goal yields 0, not NaN',
    ks.ksBuildRow(ksNode({ goal: null })).row.percent_funded, 0);

console.log('\nKickstarter — the deadline crash the bookmarklet would have taken:');
check('a unix-seconds deadline becomes ISO', ks.ksDeadline(1790000000).slice(0, 4), '2026');
check('absent is null', ks.ksDeadline(null), null);
check('zero is null, not 1970', ks.ksDeadline(0), null);
// new Date(NaN).toISOString() throws RangeError, which would abort the whole
// harvest rather than one field. The bookmarklet called it unguarded.
check('a non-numeric deadline is null rather than a thrown RangeError',
    ks.ksDeadline('not-a-date'), null);
// 'not-a-date' is caught by the Number.isFinite() guard before the Date is ever
// built. The SECOND guard only matters for a value that is finite and positive
// and still yields an Invalid Date — which is the case that actually throws.
check('a finite but absurd timestamp is null, not a RangeError', ks.ksDeadline(1e20), null);
check('...and the row survives it', ks.ksBuildRow(ksNode({ deadlineAt: 1e20 })).row.end_date, null);
check('...and building a row around it does not throw',
    ks.ksBuildRow(ksNode({ deadlineAt: 'not-a-date' })).row.end_date, null);

console.log('\nKickstarter — D&D relevance:');
check('a 5e description is flagged', ks.ksBuildRow(ksNode()).row.is_dnd_centric, true);
check('an unrelated project is not',
    ks.ksIsDndCentric('Artisan Coffee Subscription', 'Single origin beans'), false);
check('the keyword may appear in the title alone',
    ks.ksIsDndCentric('Pathfinder Adventure', ''), true);

console.log('\nKickstarter — the defaulted summary:');
check('counts each field', ks.ksSummarise([noState, noState]), 'category 2, status 2');
check('a clean run says so', ks.ksSummarise([ks.ksBuildRow(ksNode())]), 'no defaulted fields');
check('it is a string the popup can render', typeof ks.ksSummarise([noState]), 'string');

// ════════════════════════ BACKERKIT ════════════════════════
//
// V3. The first two designs read the wrong page. /c/collections/<slug> is a
// curated 10-item shelf that cannot be paged OR sorted — page, offset,
// per_page and ten sort_by values were all silently ignored, each returning
// the same ten projects with HTTP 200. /c/categories/<slug>/projects is the
// real listing: 700 projects behind infinite scroll, server-rendered HTML,
// no JSON representation at all (406 to Accept: application/json).
//
// So these test a DOM card, not an Inertia payload.

const card = (over) => Object.assign({
    url: "https://www.backerkit.com/c/projects/free-league-publishing/the-one-ring-rpg-bestiary",
    title: "The One Ring RPG: Bestiary",
    blurb: "An epic campaign expansion, also for 5E",
    creator: "Free League Publishing",
    rankText: "#1 Most funded",
    category: "Role-Playing Games",
    text: "20 days left The One Ring RPG: Bestiary $2,108,581 of $50,000 goal 8,807 backers"
}, over || {});

console.log('\n\nBackerKit — the money layout that covers 93% of cards:');
{
    const m = bk.bkMoney("$2,108,581 of $50,000 goal 8,807 backers");
    check('the raised amount', m.raised, 2108581);
    check('the goal', m.goal, 50000);
    check('the currency', m.currency, "$");
    check('backers read separately', bk.bkBackers("8,807 backers"), 8807);
}

console.log('\nBackerKit — and the 7% that say "funds raised" and have no goal:');
{
    const m = bk.bkMoney("ENNIES EMPORIUM 2026 Exalted Funeral $79,627 funds raised 623 backers");
    check('the raised amount still reads', m.raised, 79627);
    check('the goal abstains rather than guessing 0', m.goal, null);
    check('the currency still reads', m.currency, "$");
}

console.log('\nBackerKit — currency is not decoration:');
{
    // Measured across 700 projects: $ 67%, then EUR, GBP, C$, A$, NZ$, CHF, S$,
    // exactly one currency per card in 696 of 700. The old bkAmount stripped
    // every non-digit including the marker, so A$228,597 went into a column
    // named funding_usd as 228,587 US dollars.
    const aud = bk.bkMoney("A$228,597 of A$10,000 goal 786 backers");
    check('an A$ amount parses', aud.raised, 228597);
    check('...and is tagged AUD, not assumed USD', aud.currency, "A$");
    check('A$ is not treated as USD', bk.bkIsUsd("A$"), false);
    check('the longer prefix wins over the bare $', bk.bkMoney("NZ$1,000 funds raised").currency, "NZ$");
    check('C$ likewise', bk.bkMoney("C$5,250 funds raised").currency, "C$");
    check('€ parses', bk.bkMoney("€20,659 funds raised").currency, "€");
    check('£ parses', bk.bkMoney("£10,056 funds raised").currency, "£");
    check('only $ and US$ are USD', [bk.bkIsUsd("$"), bk.bkIsUsd("US$"), bk.bkIsUsd("€")],
        [true, true, false]);

    // Every marker, one by one. This is the invariant that actually protects
    // the parse: a marker that is a strict prefix of another WOULD break, and
    // only exercising all of them would show it. Reordering the list does not
    // break anything -- alternation is leftmost-by-position, not by
    // alternative order -- so no test can or should catch that.
    [["$", 100], ["US$", 100], ["€", 100], ["£", 100], ["¥", 100],
     ["C$", 100], ["CA$", 100], ["A$", 100], ["NZ$", 100], ["S$", 100],
     ["HK$", 100], ["R$", 100], ["CHF", 100], ["SEK", 100], ["NOK", 100],
     ["DKK", 100], ["PLN", 100]].forEach(([marker]) => {
        const m = bk.bkMoney(marker + "1,234 of " + marker + "500 goal");
        check('  ' + marker + ' round-trips', [m.currency, m.raised, m.goal],
            [marker, 1234, 500]);
    });
}

console.log('\nBackerKit — a non-USD row leaves funding_usd NULL:');
{
    const b = bk.bkBuildRow(card({ text: "8 days left X A$228,597 of A$10,000 goal 786 backers" }));
    check('funding_usd is null, not the AUD number', b.row.funding_usd, null);
    check('the amount is kept verbatim', b.row.funding_amount, 228597);
    check('with its unit beside it', b.row.funding_currency, "A$");
    check('and the run counts it', b.defaulted.indexOf("non_usd_A$") !== -1, true);
}
{
    const b = bk.bkBuildRow(card());
    check('a USD row DOES fill funding_usd', b.row.funding_usd, 2108581);
    check('...and funding_amount too', b.row.funding_amount, 2108581);
    check('...and is not counted as non-USD',
        b.defaulted.filter(d => d.indexOf("non_usd") === 0).length, 0);
}

console.log('\nBackerKit — "Ended" is not "ends today":');
{
    // The old code returned 0 for ended, unparseable and months-ago alike.
    // The card states no end DATE, so we know it finished, not when.
    check('a live project reports its days', bk.bkTiming("20 days left").days, 20);
    check('...and its status', bk.bkTiming("20 days left").status, "live");
    check('an ended project abstains on days', bk.bkTiming("Ended").days, null);
    check('...but records that it ended', bk.bkTiming("Ended").status, "ended");
    check('hours-left still counts as live', bk.bkTiming("6 hours left").status, "live");
    check('silence gives neither', bk.bkTiming("").status, null);
}

console.log('\nBackerKit — ids come from the url, which is a slug:');
{
    const ids = bk.bkIdsFromUrl("https://www.backerkit.com/c/projects/free-league-publishing/the-one-ring-rpg-bestiary");
    check('the project id is the last segment', ids.projectId, "the-one-ring-rpg-bestiary");
    check('the creator slug is the one before', ids.creatorSlug, "free-league-publishing");
    check('a malformed url abstains', bk.bkIdsFromUrl("https://www.backerkit.com/c/").projectId, null);
    check('a slug prettifies as a fallback name',
        bk.bkPrettySlug("free-league-publishing"), "Free League Publishing");
}

console.log('\nBackerKit — the classifier now reads the blurb, not just the title:');
{
    // "Ink Ribbon - A Survival Horror Tabletop RPG" says nothing about system.
    // Its summary does. The project id is still never passed: it is a slug.
    check('a title alone can miss it',
        bk.bkClassify("Ink Ribbon - A Survival Horror Tabletop RPG", ""), "RPG (Other)");
    check('the blurb catches it',
        bk.bkClassify("Ink Ribbon - A Survival Horror Tabletop RPG",
                      "A setting for D&D 5e and Starfinder"), "5e Compatible");
    check('OSR is detected from the blurb too',
        bk.bkClassify("The Overlords of Steel", "a mega-adventure for Old-School Essentials"), "OSR");
    check('and neither still means neither',
        bk.bkClassify("A Board Game", "with cards and tokens"), "RPG (Other)");
    check('the source signature takes both arguments',
        /function bkClassify\(title, blurb\)/.test(BK_SRC), true);
}

console.log('\nBackerKit — the summary is captured, so genre needs no detail pass:');
{
    const b = bk.bkBuildRow(card());
    check('the blurb is stored', b.row.blurb, "An epic campaign expansion, also for 5E");
    check('the category is stored', b.row.category, "Role-Playing Games");
    check('the rank is a number, not its label', b.row.trending_rank, 1);
    check('a rank label change does not break it',
        bk.bkRank("#7 Trending this week"), 7);
    check('no rank abstains', bk.bkRank("Role-Playing Games"), null);
    const noBlurb = bk.bkBuildRow(card({ blurb: "" }));
    check('a missing blurb is null and counted', noBlurb.row.blurb, null);
    check('...in defaultedFields', noBlurb.defaulted.indexOf("blurb") !== -1, true);
}

console.log('\nBackerKit — required fields are never sent short:');
{
    // project_id and title are REQUIRED in BigQuery. A row missing either is
    // dropped SILENTLY by skip_invalid_rows, so it must not be built at all.
    check('no title -> no row', bk.bkBuildRow(card({ title: "" })), null);
    check('no usable url -> no row',
        bk.bkBuildRow(card({ url: "https://www.backerkit.com/c/" })), null);
    check('a creator falls back to the slug rather than blocking the row',
        bk.bkBuildRow(card({ creator: "" })).row.creator, "Free League Publishing");
    check('...and that fallback is counted',
        bk.bkBuildRow(card({ creator: "" })).defaulted.indexOf("creator_from_slug") !== -1, true);
}

console.log('\nBackerKit — unmeasured values are null, never zero:');
{
    const b = bk.bkBuildRow(card({ text: "Ended The One Ring RPG: Bestiary" }));
    check('no money read -> null, not 0', b.row.funding_amount, null);
    check('no backers read -> null, not 0', b.row.backers_count, null);
    check('ended with no date -> null days', b.row.days_remaining, null);
    check('but the ended status IS recorded', b.row.status, "ended");
    check('and all of it is counted',
        ["funding_amount", "backers_count", "days_remaining"]
            .every(f => b.defaulted.indexOf(f) !== -1), true);
}

console.log('\nBackerKit — the defaulted summary:');
check('counts repeat across rows',
    bk.bkSummarise([{ defaulted: ["blurb"] }, { defaulted: ["blurb", "goal_amount"] }]),
    'blurb 2, goal_amount 1');
check('a clean run says so', bk.bkSummarise([{ defaulted: [] }]), 'no defaulted fields');

// ════════════════════════ SOURCE GUARDS ════════════════════════

console.log('\nSource guards:');
check('Kickstarter posts ONE request, never chunked (the dedup guard truncates)',
    /ONE request, not chunked/.test(KS_SRC) && !/CHUNK/.test(KS_SRC), true);
check('Kickstarter refuses to run off kickstarter.com',
    /hostname\.endsWith\("kickstarter\.com"\)/.test(KS_SRC), true);
check('BackerKit refuses to run off backerkit.com',
    /hostname\.endsWith\("backerkit\.com"\)/.test(BK_SRC), true);
// BackerKit no longer FETCHES BackerKit at all. V1 and V2 called the Inertia
// endpoint with X-Inertia and credentials; V3 reads the DOM of a real signed-in
// tab, because the categories listing has no JSON representation (406 to
// Accept: application/json). Its only fetch is the POST to the bouncer, so
// asserting an Inertia header here would pin a mechanism that is gone.
check('Kickstarter still sends credentials, so its session rides along',
    /credentials: "include"/.test(KS_SRC), true);
check('BackerKit makes no request to backerkit.com at all',
    /fetch\(\s*["'`]https:\/\/www\.backerkit\.com/.test(BK_SRC) ||
    /X-Inertia/.test(BK_SRC.replace(/\/\/[^\n]*/g, '')), false);
// Checking for the string "about:blank" would match the comments that explain
// why the relay was removed. The mechanism is what matters: no popup is opened,
// and nothing is assembled as HTML to be written into one.
check('neither opens a popup any more',
    /window\.open\(/.test(KS_SRC) || /window\.open\(/.test(BK_SRC), false);
check('neither writes a document into one',
    /document\.write\(/.test(KS_SRC) || /document\.write\(/.test(BK_SRC), false);
check('both report the defaulted-field counts',
    /defaultedFields:/.test(KS_SRC) && /defaultedFields:/.test(BK_SRC), true);
check('a failed ingest throws rather than counting as sent',
    /throw new Error\("ingest HTTP/.test(KS_SRC) && /throw new Error\("ingest HTTP/.test(BK_SRC), true);
check('endpoints are derived from ENDPOINT, not retyped',
    /ENDPOINT\.replace\(/.test(KS_SRC) && /ENDPOINT\.replace\(/.test(BK_SRC), true);

// ════════════════ THE PAGE THIS READS ════════════════
//
// The measurements that chose it, recorded where the next reader will look.
const BK_PROSE = BK_SRC.replace(/^\s*\/\/ ?/gm, '').replace(/\s+/g, ' ');

console.log('\nthe source records why the collections endpoint was abandoned:');
check('it names both urls',
    /\/c\/collections\/<slug>/.test(BK_PROSE) && /\/c\/categories\/<slug>\/projects/.test(BK_PROSE), true);
check('and that page/offset/per_page/sort_by were all ignored',
    /SILENTLY\s*IGNORED/.test(BK_PROSE), true);
check('and that counting results would have looked like success',
    /would have read as fourteen successes/.test(BK_PROSE), true);
check('the harvester targets the categories listing',
    /c\/categories\/" \+ BK_CATEGORY \+ "\/projects/.test(BK_SRC), true);

console.log('\nit does not hard-code the 700 it happened to measure:');
// 700 is exactly 25 x 28, which is the shape of a server-side cap rather than
// a natural end. The loop stops on a plateau so a raised cap is picked up free.
check('the scroll loop stops on a plateau', /if \(now === previous\) break;/.test(BK_SRC), true);
check('no literal 700 anywhere in the logic', /\b700\b/.test(BK_SRC.replace(/\/\/[^\n]*/g, '')), false);
check('the cap suspicion is written down', /25 x 28/.test(BK_PROSE), true);

console.log('\nit finds cards by structure, not by Tailwind build artifacts:');
check('cards are found by walking up from a project link',
    /distinctProjects\(el\) === 1/.test(BK_SRC), true);
check('...counting DISTINCT urls, not links',
    /new Set\(Array\.from\(el\.querySelectorAll\(PROJECT_LINK\)\)\.map\(urlOf\)\)\.size/.test(BK_SRC), true);
// Stripped of comments first. The header EXPLAINS why these classes are
// avoided, quoting one, so testing the raw source matched the explanation and
// reported the opposite of the truth -- the same shape as the earlier check
// that matched the _enrich_batch definition instead of its call site.
const BK_CODE = BK_SRC.replace(/^\s*\/\/[^\n]*$/gm, '');
check('no arbitrary-value class is used as a selector in the CODE',
    /shadow-\[|line-clamp-1|font-walsheim/.test(BK_CODE), false);
check('...though the header still explains why, for the next reader',
    /shadow-\[/.test(BK_PROSE), true);
check('the reason that mattered is recorded',
    /~3 links per card/.test(BK_PROSE), true);

console.log('\nclipped text is read from the title attribute:');
check('fullText prefers the attribute', /getAttribute\("title"\)/.test(BK_SRC), true);
check('and the reason is recorded', /CSS line-clamps/.test(BK_PROSE), true);

console.log('\nstill ONE request, so the date guard cannot truncate it:');
check('exactly one POST', (BK_SRC.match(/method: "POST"/g) || []).length, 1);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
