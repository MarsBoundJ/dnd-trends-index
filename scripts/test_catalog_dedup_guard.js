// The catalog duplicate guard — and why it could not be the guard the other
// three ingest routes use.
//
//     node scripts/test_catalog_dedup_guard.js
//
// On 2026-09-24 a double-clicked Run Now inserted DMs Guild and DriveThruRPG
// twice: ~2,180 and ~2,726 rows for one day. Kickstarter, BackerKit and the
// Amazon rank route all refused their second run correctly. ingest-catalog had
// no guard at all.
//
// The obvious fix — copy the other routes' "does today already have rows?"
// check — is a DATA LOSS BUG here, for two reasons that do not apply there:
//
//   1. THIS ROUTE IS CHUNKED. The extension posts 1,000 rows per request and
//      the three bookmarklet copies post 500. A per-request date check makes
//      chunk 2 find the rows chunk 1 just inserted and skip. A 1,090-product
//      capture silently becomes 1,000, and the client reports success.
//
//   2. THREE SOURCES SHARE THE ENDPOINT. DMs Guild, DriveThruRPG and Amazon
//      all post here. A whole-table date check lets whichever runs first block
//      the other two for the rest of the day.
//
// So the guard fires only on chunk 0, and scopes to one source. This models the
// client and server together and runs the scenarios, then pins the real source
// to the modelled rule — the same mirror-and-assert approach
// scripts/test_catalog_selectors.js uses for the probe.

const fs = require('fs');
const path = require('path');

const BOUNCER = fs.readFileSync(path.join(__dirname, '..', 'bouncer', 'main.py'), 'utf8');
const BG = fs.readFileSync(
    path.join(__dirname, '..', 'catalog_harvester_extension', 'background.js'), 'utf8');

let pass = 0, fail = 0;
function check(name, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) pass++; else fail++;
    console.log((ok ? '  ok   ' : '  FAIL ') + name +
        (ok ? '' : '\n         expected ' + JSON.stringify(expected) +
                   ', got ' + JSON.stringify(actual)));
}

// ---------- The model ----------

// The server, mirroring the guard in bouncer/main.py. `guardEveryChunk` exists
// only so the naive version can be run and shown to lose data.
function makeServer(opts) {
    opts = opts || {};
    const table = [];
    return {
        table: table,
        rowsFor: (src, day) =>
            table.filter(r => r.source === src && r.collected_date === day).length,
        post(rows, query) {
            if (!rows.length) return { status: 400, body: { error: 'No data' } };
            const chunkIndex = query.chunk;
            const asks = opts.guardEveryChunk ? chunkIndex !== undefined : chunkIndex === '0';
            if (asks) {
                const source = String(rows[0].source || '');
                const collected = String(rows[0].collected_date || '');
                if (source && collected) {
                    const n = opts.guardWholeTable
                        ? table.filter(r => r.collected_date === collected).length
                        : table.filter(r => r.source === source &&
                                            r.collected_date === collected).length;
                    if (n > 0) {
                        return { status: 200, body: { skipped: true,
                            reason: source + ' catalog already ingested for ' + collected } };
                    }
                }
            }
            rows.forEach(r => table.push(r));
            return { status: 200, body: { inserted: rows.length } };
        }
    };
}

// The client loop, mirroring runExtractionInPage in background.js.
function runClient(server, products, chunkSize, opts) {
    opts = opts || {};
    let successCount = 0, skipped = false;
    for (let i = 0; i < products.length; i += chunkSize) {
        const chunk = products.slice(i, i + chunkSize);
        const query = {};
        if (!opts.noHeader) query.chunk = String(i / chunkSize);
        const res = server.post(chunk, query);
        if (res.status !== 200) return { error: res.status, count: successCount };
        if (opts.ignoreSkipped) {            // the loop as it was before this change
            successCount += chunk.length;
            continue;
        }
        if (res.body.skipped) { skipped = true; break; }
        successCount += res.body.inserted || chunk.length;
    }
    return { count: skipped ? 0 : successCount, skipped: skipped };
}

const products = (src, day, n) =>
    Array.from({ length: n }, (_, i) => ({ source: src, collected_date: day,
                                           product_url: src + '/p/' + i }));

const DAY = '2026-09-24';
const DMG = products('DMs Guild', DAY, 1090);
const DTR = products('DriveThruRPG', DAY, 1363);

// ---------- Scenarios ----------

console.log('\na normal run stores every chunk (the bug the naive guard would cause):');
{
    const s = makeServer();
    const r = runClient(s, DMG, 1000);
    check('all 1,090 rows stored, not just the first chunk', s.table.length, 1090);
    check('the client reports 1,090', r.count, 1090);
    check('nothing was skipped', r.skipped, false);
}

console.log('\nthe naive per-request guard silently truncates to one chunk:');
{
    const s = makeServer({ guardEveryChunk: true });
    const r = runClient(s, DMG, 1000);
    check('only the first chunk lands', s.table.length, 1000);
    check('...and 90 products are lost', 1090 - s.table.length, 90);
    check('the client is told it skipped, so at least it does not lie', r.count, 0);
}

console.log('\na second run of the same source on the same day stores nothing:');
{
    const s = makeServer();
    runClient(s, DMG, 1000);
    const second = runClient(s, DMG, 1000);
    check('the table still holds exactly one capture', s.table.length, 1090);
    check('the second run reports skipped', second.skipped, true);
    check('...and reports 0, not a phantom 1,090', second.count, 0);
}

console.log('\nthe other two sources are not blocked by the first:');
{
    const s = makeServer();
    runClient(s, DMG, 1000);
    runClient(s, DTR, 1000);
    check('DMs Guild stored in full', s.rowsFor('DMs Guild', DAY), 1090);
    check('DriveThruRPG stored in full', s.rowsFor('DriveThruRPG', DAY), 1363);
}
{
    const s = makeServer({ guardWholeTable: true });
    runClient(s, DMG, 1000);
    const dtr = runClient(s, DTR, 1000);
    check('a whole-table date check would lock DriveThruRPG out', dtr.skipped, true);
    check('...losing all 1,363 of its rows', s.rowsFor('DriveThruRPG', DAY), 0);
}

console.log('\nignoring `skipped` in the client leaks a partial duplicate:');
{
    // This is why the client had to change too. Chunk 0 is refused, but a loop
    // that does not look at the answer carries on to chunk 1 — which is not
    // chunk 0, so the guard never asks — and inserts the remaining 90.
    const s = makeServer();
    runClient(s, DMG, 1000);
    const bad = runClient(s, DMG, 1000, { ignoreSkipped: true });
    check('90 duplicate rows slip through', s.table.length, 1180);
    check('...and the run claims a full 1,090 capture', bad.count, 1090);
}

console.log('\na client that sends no header is left unguarded, deliberately:');
{
    // The three bookmarklet copies chunk without the header. Treating a missing
    // header as chunk 0 would truncate them, so they keep the old behaviour:
    // duplicates possible, no silent data loss.
    const s = makeServer();
    runClient(s, DMG, 500, { noHeader: true });
    check('a legacy run stores everything', s.table.length, 1090);
    const again = runClient(s, DMG, 500, { noHeader: true });
    check('a legacy re-run still duplicates (known, and the safer failure)',
        s.table.length, 2180);
    check('...and it is not silently short', again.count, 1090);
}

// ---------- Pin the real source to the model ----------

function guardBlock() {
    const a = BOUNCER.indexOf("chunk_index = request.args.get('chunk')");
    const b = BOUNCER.indexOf('# Enrich rows with Gemini', a);
    if (a < 0 || b < 0) throw new Error('guard block not found in bouncer/main.py');
    return BOUNCER.slice(a, b);
}

console.log('\nbouncer/main.py implements exactly that rule:');
const block = guardBlock();
check('it reads the query parameter with NO default',
    BOUNCER.indexOf("request.args.get('chunk')") !== -1 &&
    BOUNCER.indexOf("request.args.get('chunk',") === -1, true);
check("it guards only on chunk '0'", /if chunk_index == '0':/.test(block), true);
check('it scopes the count to one source', /WHERE source = @source/.test(block), true);
check('...and to one collected_date', /CAST\(collected_date AS STRING\) = @collected/.test(block), true);
check('it answers a duplicate with skipped:True', /"skipped": True/.test(block), true);

console.log('\nthe guard runs BEFORE the Gemini enrichment:');
// Enrichment is the expensive part. A duplicate run that enriched first would
// pay the full Vertex bill to produce rows it then throws away.
//
// Compared INSIDE the route. The first draft of this check compared against
// BOUNCER.indexOf('_enrich_batch(rows, model)'), which matches the function
// DEFINITION near the top of the file — so it measured "is the guard before
// line 24?", answered no, and would have kept answering no however the route
// was ordered.
function catalogRoute() {
    const a = BOUNCER.indexOf("elif path == 'system/library/ingest-catalog':");
    const b = BOUNCER.indexOf("elif path == 'system/library/ingest-catalog-detail':", a);
    if (a < 0 || b < 0) throw new Error('ingest-catalog route not found');
    return BOUNCER.slice(a, b);
}
const route = catalogRoute();
const guardAt = route.indexOf("chunk_index = request.args.get('chunk')");
const enrichAt = route.indexOf('_enrich_batch(rows, model)');
check('both appear inside the ingest-catalog route', guardAt >= 0 && enrichAt >= 0, true);
check('guard precedes the _enrich_batch call', guardAt < enrichAt, true);

console.log('\nclient values reach the query as parameters, never as text:');
check('the guard binds both values',
    /ScalarQueryParameter\('source', 'STRING', source\)/.test(block) &&
    /ScalarQueryParameter\('collected', 'STRING', collected\)/.test(block), true);
check('no f-string interpolation anywhere in the SQL it builds',
    /f"[^"]*\{/.test(block.split('job_config')[0]), false);

console.log('\nno new request header was introduced, so the deploys stay order-independent:');
// An earlier draft sent X-Chunk-Index as a header. That needs a matching entry
// in Access-Control-Allow-Headers, and until the bouncer carrying it was
// deployed every catalog POST from an updated client would fail its preflight
// -- breaking all three storefronts if the two deploys went out backwards.
check('CORS headers are untouched',
    (BOUNCER.match(/'Access-Control-Allow-Headers': 'Content-Type, X-Ritual-Key'/g) || []).length, 2);
check('nothing sends an X-Chunk-Index header any more',
    BOUNCER.indexOf('X-Chunk-Index') === -1 && BG.indexOf('X-Chunk-Index') === -1, true);
check('the router reads request.path, which a query string cannot affect',
    /full_path = request\.path/.test(BOUNCER), true);

console.log('\nbackground.js is the chunk-aware client the guard needs:');
check('it sends the chunk index on the URL',
    /"chunk=" \+ \(i \/ chunkSize\)/.test(BG), true);
check('...appended with the right separator',
    /const sep = endpoint\.indexOf\("\?"\) === -1 \? "\?" : "&";/.test(BG), true);
check('it stops on skipped', /if \(data\.skipped\) \{ skipped = true; break; \}/.test(BG), true);
check('it counts what the server says it inserted',
    /successCount \+= data\.inserted \|\| chunk\.length;/.test(BG), true);
check('the unconditional count is gone',
    /\n\s+successCount \+= chunk\.length;\n/.test(BG), false);
check('a skipped run reports 0, not a phantom count',
    /count: skipped \? 0 : successCount/.test(BG), true);

console.log('\namazon.js posts to the same endpoint and needs the same parameter:');
// Amazon's postChunks already honoured `skipped` -- it was the ONE client that
// did -- but it sent no chunk index, so the new guard would have waved it
// through. It is not a bystander: on 2026-09-24 it stored 707 rows where one
// capture is ~350.
const AMZ = fs.readFileSync(
    path.join(__dirname, '..', 'catalog_harvester_extension', 'amazon.js'), 'utf8');
check('amazon.js sends the chunk index too',
    /"chunk=" \+ \(i \/ chunkSize\)/.test(AMZ), true);
check('...with the same separator logic as background.js',
    /const sep = endpoint\.indexOf\("\?"\) === -1 \? "\?" : "&";/.test(AMZ), true);
check('it already stopped on skipped, and still does',
    /if \(data\.skipped\) \{ skipped = true; break; \}/.test(AMZ), true);
check('no catalog client posts to a bare endpoint any more',
    /await fetch\(endpoint, \{/.test(AMZ) === false &&
    /await fetch\(endpoint, \{/.test(BG) === false, true);

console.log(`\n${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
