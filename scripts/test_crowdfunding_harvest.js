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
    ['bkAmount', 'bkInt', 'bkDaysRemaining', 'bkClassify', 'bkBuildRow', 'bkSummarise',
     'bkExtractProjects'],
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

function bkProject(over) {
    return Object.assign({
        id: 'proj_8842',
        title: 'Tales of the Valiant: Monster Vault',
        creator_name: 'Kobold Press',
        raised_amount: '$412,880',
        backers: '6,214',
        ended_at: 'April 30, 2099 at 10:00 AM PDT',
        formatted_permalink: 'https://www.backerkit.com/c/projects/kobold-press/x'
    }, over || {});
}

const NOW = Date.parse('2026-09-23T12:00:00Z');

console.log('\n\nBackerKit — money and backers:');
check('"$412,880" parses', bk.bkAmount('$412,880'), 412880);
check('absent is null, NOT 0.0', bk.bkAmount(undefined), null);
check('unparseable is null, NOT 0.0', bk.bkAmount('TBD'), null);
check('"6,214" backers parses', bk.bkInt('6,214'), 6214);
check('absent backers is null, NOT 0', bk.bkInt(null), null);
check('a genuine zero survives as zero', bk.bkAmount('0'), 0);

const bkNoMoney = bk.bkBuildRow(bkProject({ raised_amount: null, backers: null }), NOW);
check('missing money is recorded as defaulted',
    bkNoMoney.defaulted.indexOf('funding_usd') !== -1 &&
    bkNoMoney.defaulted.indexOf('backers_count') !== -1, true);
check('...while the wire value stays 0 (the bouncer re-zeroes nulls anyway)',
    [bkNoMoney.row.funding_usd, bkNoMoney.row.backers_count], [0, 0]);

console.log('\nBackerKit — days_remaining meant three different things:');
check('a future date gives a positive count',
    bk.bkDaysRemaining('April 30, 2099 at 10:00 AM PDT', NOW) > 0, true);
check('an unparseable date is null, not 0', bk.bkDaysRemaining('sometime soon', NOW), null);
check('an absent date is null, not 0', bk.bkDaysRemaining('', NOW), null);
// The bookmarklet clamped with Math.max(0, …), so a campaign that ended months
// ago and one ending today both reported 0.
check('an ENDED campaign gives a negative count internally',
    bk.bkDaysRemaining('January 1, 2020 at 10:00 AM PDT', NOW) < 0, true);
const ended = bk.bkBuildRow(bkProject({ ended_at: 'January 1, 2020 at 10:00 AM PDT' }), NOW);
check('...which is flagged rather than silently clamped',
    ended.defaulted.indexOf('ended_clamped') !== -1, true);
check('...though the wire value is still clamped to 0', ended.row.days_remaining, 0);
const unparseable = bk.bkBuildRow(bkProject({ ended_at: 'soon' }), NOW);
check('an unparseable date is flagged separately',
    unparseable.defaulted.indexOf('days_remaining') !== -1, true);

console.log('\nBackerKit — the classifier no longer reads the project id:');
check('a 5e title classifies', bk.bkClassify('Tales of the Valiant: Monster Vault'), '5e Compatible');
check('an OSR title classifies', bk.bkClassify('Old School Essentials Reprint'), 'OSR');
check('anything else is RPG (Other)', bk.bkClassify('Lancer: Field Guide'), 'RPG (Other)');
// The bookmarklet passed `title + ' ' + project_id` to the matcher, so an id
// containing "5e" would have classified the project.
check('an id containing "5e" cannot classify the row',
    bk.bkBuildRow(bkProject({ id: 'abc5e999', title: 'Lancer: Field Guide' }), NOW).row.system_tag,
    'RPG (Other)');

console.log('\nBackerKit — required fields:');
check('no id means no row', bk.bkBuildRow(bkProject({ id: '' }), NOW), null);
check('no title means no row', bk.bkBuildRow(bkProject({ title: '  ' }), NOW), null);
check('a valid row survives', bk.bkBuildRow(bkProject(), NOW).row.project_id, 'proj_8842');

console.log('\nBackerKit — the Inertia response shape:');
check('top-level key is read',
    bk.bkExtractProjects({ 'crowdfunding/projects': [1, 2] }).length, 2);
check('nested under props is also read',
    bk.bkExtractProjects({ props: { 'crowdfunding/projects': [1] } }).length, 1);
check('an unknown shape yields an empty list, not a throw', bk.bkExtractProjects({ foo: 1 }), []);
check('null yields an empty list', bk.bkExtractProjects(null), []);

// ════════════════════════ SOURCE GUARDS ════════════════════════

console.log('\nSource guards:');
check('Kickstarter posts ONE request, never chunked (the dedup guard truncates)',
    /ONE request, not chunked/.test(KS_SRC) && !/CHUNK/.test(KS_SRC), true);
check('Kickstarter refuses to run off kickstarter.com',
    /hostname\.endsWith\("kickstarter\.com"\)/.test(KS_SRC), true);
check('BackerKit refuses to run off backerkit.com',
    /hostname\.endsWith\("backerkit\.com"\)/.test(BK_SRC), true);
check('BackerKit sends the Inertia header, without which it gets HTML',
    /"X-Inertia": "true"/.test(BK_SRC), true);
check('both send credentials so the session cookie rides along',
    /credentials: "include"/.test(KS_SRC) && /credentials: "include"/.test(BK_SRC), true);
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

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
