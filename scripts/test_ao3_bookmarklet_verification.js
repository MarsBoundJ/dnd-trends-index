// Tests for the AO3 bookmarklet's read-back filter verification.
//
//   node scripts/test_ao3_bookmarklet_verification.js
//
// The functions under test are read straight out of scripts/ao3_bookmarklet.js
// rather than copied here. A copy would drift, and a verification routine whose
// test no longer matches the shipped code is worse than no test — it reports
// green while the bookmarklet does something else.
//
// The DOM stub answers only the two selector shapes verifyFilter() uses. It is
// deliberately dumb: the point is to pin the VERDICT LOGIC, not to reimplement
// AO3. Whether those selectors match AO3's real markup can only be settled on a
// live page, which is why an unreadable page must report 'unverified' and never
// 'verified' — see the test named for it below.

const fs = require('fs');
const path = require('path');

const SRC = path.join(__dirname, 'ao3_bookmarklet.js');
const START = '  // ── Read-back filter verification';
const END = '  const params = new URLSearchParams';

function loadVerification() {
  const src = fs.readFileSync(SRC, 'utf8');
  const a = src.indexOf(START);
  const b = src.indexOf(END);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error(
      'Could not find the verification block in ao3_bookmarklet.js. If it was ' +
      'renamed or moved, update START/END here — do not delete this test.');
  }
  // eslint-disable-next-line no-new-func
  return new Function(
    'document',
    src.slice(a, b) + '\nreturn { normTag, tagAlts, tagsOverlap, verifyFilter, isUmbrella };');
}

const build = loadVerification();

// flagsFor() decides what the review table warns about, and is sliced the same
// way and for the same reason: a copy would drift.
const F_START = '  function flagsFor(r, all) {';
const F_END = '  // The review table shows the CANONICAL TAG';

function loadFlagsFor() {
  const src = fs.readFileSync(SRC, 'utf8');
  const a = src.indexOf(F_START);
  const b = src.indexOf(F_END);
  if (a < 0 || b < 0 || b <= a) {
    throw new Error('Could not find flagsFor() in ao3_bookmarklet.js.');
  }
  // eslint-disable-next-line no-new-func
  return new Function(src.slice(a, b) + ';return flagsFor;')();
}

const flagsFor = loadFlagsFor();
const flagText = (row) => flagsFor(row, [row]).map((x) => x[1]).join(' | ');

function makeDoc({ boxValue, works }) {
  const blurbs = (works || []).map((fandoms) => ({
    querySelectorAll: (sel) =>
      /fandoms/.test(sel) ? fandoms.map((t) => ({ textContent: t })) : [],
  }));
  return {
    querySelector: (sel) =>
      /other_tag_names/.test(sel) && boxValue !== undefined
        ? { value: boxValue } : null,
    querySelectorAll: (sel) => (/blurb/.test(sel) ? blurbs : []),
  };
}

const DND = 'Dungeons & Dragons (Roleplaying Game)';
let passed = 0;
let failed = 0;

function check(name, got, want) {
  if (got === want) {
    passed++;
    console.log(`ok    ${name}`);
  } else {
    failed++;
    console.log(`FAIL  ${name}\n        got=${got}  want=${want}`);
  }
}

const M = build(makeDoc({}));
const overlap = M.tagsOverlap;

// ── Tag matching ─────────────────────────────────────────────────────────
// AO3 names a fandom at several levels and work blurbs show the CHILD, so the
// umbrella we filter on is rarely the string that comes back.
check('umbrella matches its child',
  overlap('Avatar: The Last Airbender & Related Fandoms',
          'Avatar: The Last Airbender (TV 2005)'), true);
check('Witcher umbrella matches the Polish-titled child',
  overlap('Wiedzmin | The Witcher - All Media Types',
          'Wiedzmin: Dziki Gon | The Witcher 3: Wild Hunt'), true);
check('One Piece umbrella matches the anime child',
  overlap('One Piece - All Media Types', 'One Piece (Anime & Manga)'), true);
check('SPY x FAMILY umbrella matches the anime child',
  overlap('SPY x FAMILY - All Media Types', 'SPY x FAMILY (Anime)'), true);
check('Doctor Who related-fandoms matches the 2005 child',
  overlap('Doctor Who & Related Fandoms', 'Doctor Who (2005)'), true);

// The regression that motivated one-directional containment. Matching both
// ways let any shorter prefix satisfy the filter, which is precisely the
// wrong-but-plausible class of match this function exists to reject.
check('Avatar (2009) does NOT satisfy a filter for Airbender',
  overlap('Avatar: The Last Airbender & Related Fandoms', 'Avatar (2009)'), false);
check('an unrelated fandom does not match',
  overlap('Hollow Knight', 'Stranger Things (TV 2016)'), false);
check('a sibling under the same umbrella does not match',
  overlap('The Lord of the Rings - All Media Types',
          'The Hobbit - All Media Types'), false);

// ── Verdicts ─────────────────────────────────────────────────────────────
const verdict = (doc, tag) => build(doc).verifyFilter(tag);

check('a good capture verifies',
  verdict(makeDoc({
    boxValue: 'Avatar: The Last Airbender & Related Fandoms',
    works: Array(10).fill([DND, 'Avatar: The Last Airbender (TV 2005)']),
  }), 'Avatar: The Last Airbender & Related Fandoms').verdict, 'verified');

// Sep 1: a filter-less page stored 10,886 — every D&D crossover on AO3 — as one
// IP's count, because an ignored filter renders identically to an applied one.
check('a silently ignored filter is refused',
  verdict(makeDoc({ boxValue: '', works: Array(10).fill([DND]) }),
    'Avatar: The Last Airbender & Related Fandoms').verdict, 'failed');

check('AO3 applying a different tag is refused',
  verdict(makeDoc({
    boxValue: 'Stranger Things (TV 2016)',
    works: Array(10).fill([DND, 'Stranger Things (TV 2016)']),
  }), 'Avatar: The Last Airbender & Related Fandoms').verdict, 'failed');

// The load-bearing one. If the page cannot be read, the honest answer is "I do
// not know", never "checked". A verifier that certifies on silence reproduces
// the bug it was written to remove.
check('an unreadable page reports unverified, never verified',
  verdict(makeDoc({}), 'Hollow Knight').verdict, 'unverified');

check('works alone can verify when the filter box is absent',
  verdict(makeDoc({ works: Array(5).fill(['Hollow Knight (Video Game)']) }),
    'Hollow Knight').verdict, 'verified');
check('works alone can refuse when the filter box is absent',
  verdict(makeDoc({ works: Array(5).fill(['Elden Ring (Video Game)']) }),
    'Hollow Knight').verdict, 'failed');

// A real umbrella lists works tagged only with a sibling: the LotR umbrella
// covers The Hobbit. With AO3 itself confirming the filter, that is far more
// likely to be name-matching missing an odd child than a dropped filter, so it
// warns instead of throwing away a real capture.
// The mirror case, and the reason the grading is symmetric. These selectors have
// never been run against live AO3 markup. If AO3 does not repopulate its filter
// box on results pages, refusing on an empty box would block EVERY capture — a
// worse failure than the one being fixed. Works uniformly carrying the requested
// fandom cannot happen without the filter, since an unfiltered page is the
// site-wide D&D set, which is a mix.
const boxEmpty = verdict(makeDoc({
  boxValue: '',
  works: Array(8).fill([DND, 'Hollow Knight (Video Game)']),
}), 'Hollow Knight');
check('empty box but the works carry it: verified, not refused', boxEmpty.verdict, 'verified');
check('  ...and it still raises a warning', boxEmpty.warn, true);

// Both signals failing is still decisive. This is the Sep 1 unfiltered page:
// no filter applied, and the works are the site-wide D&D mix.
check('both signals failing is still refused',
  verdict(makeDoc({ boxValue: '', works: Array(8).fill([DND]) }), 'Hollow Knight').verdict,
  'failed');

const mixed = verdict(makeDoc({
  boxValue: 'The Lord of the Rings - All Media Types',
  works: [['The Hobbit - All Media Types'], ['The Hobbit - All Media Types'],
          ['The Silmarillion']],
}), 'The Lord of the Rings - All Media Types');
check('box agrees but children differ: verified, not refused', mixed.verdict, 'verified');
check('  ...and it still raises a warning', mixed.warn, true);

// A genuine zero has no works to corroborate with; the filter box is the only
// evidence, and it is enough.
check('a zero-result page still verifies from the filter box',
  verdict(makeDoc({ boxValue: 'Mistborn - All Media Types', works: [] }),
    'Mistborn - All Media Types').verdict, 'verified');

// ── Umbrella threshold ───────────────────────────────────────────────────
// From the first live run, Sep 14. The Avatar umbrella capture was CORRECT —
// AO3 confirmed the filter and the count was the expected 60 — but the works
// read 15/20, because the other five are Legend of Korra: genuinely inside the
// "& Related Fandoms" umbrella, and sharing no words with its name. At a flat
// 0.8 threshold a correct capture warned, which trains the warning to be
// ignored. Under an umbrella a partial match is the normal reading.
check('umbrella tags are recognised',
  M.isUmbrella('Avatar: The Last Airbender & Related Fandoms'), true);
check('  ...and so is the other suffix',
  M.isUmbrella('One Piece - All Media Types'), true);
check('  ...while a plain tag is not',
  M.isUmbrella('Stranger Things (TV 2016)'), false);

const korra = verdict(makeDoc({
  boxValue: 'Avatar: The Last Airbender & Related Fandoms',
  works: Array(15).fill([DND, 'Avatar: The Last Airbender (TV 2005)'])
    .concat(Array(5).fill([DND, 'The Legend of Korra'])),
}), 'Avatar: The Last Airbender & Related Fandoms');
check('the real 15/20 umbrella capture verifies', korra.verdict, 'verified');
check('  ...and does NOT warn', korra.warn, false);

// The same ratio on a NON-umbrella tag still warns: there is no sibling-naming
// explanation available, so 75% is genuinely odd.
const flat = verdict(makeDoc({
  boxValue: 'Stranger Things (TV 2016)',
  works: Array(15).fill([DND, 'Stranger Things (TV 2016)'])
    .concat(Array(5).fill([DND, 'Elden Ring (Video Game)'])),
}), 'Stranger Things (TV 2016)');
check('the same ratio on a flat tag still warns', flat.warn, true);

// ── Pass detail and warn detail stay apart ───────────────────────────────
// Joining them put the warning inside the GREEN line and printed it again in
// the amber one: the panel said the same thing twice and rendered a caveat as
// a tick. Seen in the first live screenshot.
const split = verdict(makeDoc({
  boxValue: 'The Lord of the Rings - All Media Types',
  works: [['The Hobbit - All Media Types']],
}), 'The Lord of the Rings - All Media Types');
check('the confirmation line carries only what confirmed',
  /confirms the filter/.test(split.detail) && !/none of the/.test(split.detail), true);
check('the warning line carries only what disagreed',
  /none of the/.test(split.warnDetail) && !/confirms the filter/.test(split.warnDetail), true);


// ── Review-table flags ───────────────────────────────────────────────────
// What a CURRENT build writes.
check('a verified row raises no verification flag',
  /UNVERIFIED/.test(flagText({ work_count: 60, verification_verdict: 'verified' })), false);
check('an unverified row is flagged',
  /UNVERIFIED/.test(flagText({ work_count: 60, verification_verdict: 'unverified' })), true);

// What an OLDER build left in localStorage: no verdict field at all. Testing
// for the ABSENCE of 'verified' rather than the presence of 'unverified' is
// what stops the one never-checked row from being the one that looks clean.
check('a row from an older build, carrying no verdict, is flagged',
  /UNVERIFIED/.test(flagText({ work_count: 60 })), true);

// ── Critical flags must block the send ───────────────────────────────────
// Sep 14, the fourth time this row has landed. A stale saved bookmark captured
// Baldur's Gate at 49,245 — the whole fandom, not an intersection — inside an
// otherwise clean 23-IP round. Every layer behaved: the URL generator refused to
// emit the link, read-back verification correctly PASSED (AO3 did apply the tag
// we asked for; we asked the wrong thing), the panel raised a red flag, and the
// guard raised two CRITICALs. The row landed anyway, because the red flag was
// advice and Send stayed enabled. One bad row sets the log-normalisation
// denominator and compresses every other AO3 score ~2.4x.
const batch = [
  { ip_name: "Baldur's Gate 3", work_count: 49245, verification_verdict: 'verified' },
  { ip_name: 'The Lord of the Rings', work_count: 84, verification_verdict: 'verified' },
  { ip_name: 'Avatar', work_count: 60, verification_verdict: 'verified' },
  { ip_name: 'Jujutsu Kaisen', work_count: 54, verification_verdict: 'verified' },
];
const isCritical = (r) => flagsFor(r, batch).some((y) => y[0] === '#ff8888');
check('metatag inflation is flagged critical', isCritical(batch[0]), true);
check('an ordinary row in the same batch is not', isCritical(batch[1]), false);
check('a zero is flagged critical',
  isCritical({ ip_name: 'X', work_count: 0, verification_verdict: 'verified' }), true);


console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
