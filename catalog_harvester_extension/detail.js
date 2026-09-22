// Arcane Incursion — detail pass
//
// The shelf harvest answers "what is selling". This answers "what IS it":
// page count, publisher, the storefront's own facet tags, the full star
// distribution, release dates, AI disclosure.
//
// It talks to the storefront's REST API rather than rendering pages. One JSON
// call per product against roughly 36 requests for a rendered Angular route, so
// the faster path is also the lighter one on someone else's servers. No tabs are
// opened: api.<store>.com matches the extension's existing host permissions, so
// the service worker fetches directly.
//
//   GET https://api.<store>.com/api/vBeta/products/{id}?groupId=G&siteId=S
//
// ALWAYS send groupId and siteId. The host does not decide the vocabulary — a
// bare call to api.dmsguild.com returns DriveThruRPG's taxonomy with HTTP 200
// and no warning.

const DETAIL_STORES = {
    "DMs Guild":    { api: "https://api.dmsguild.com",    groupId: 29, siteId: 76 },
    "DriveThruRPG": { api: "https://api.drivethrurpg.com", groupId: 1,  siteId: 10 }
};

// ---------- Axis roots (sliced by scripts/test_catalog_detail.js) ----------
//
// A filter's axis is the root it descends from. The API gives each filter an
// `ancestors` array, so the axis is whichever ancestor is a root here. Thirteen
// numbers, taken from taxonomy/*_facets_v2.json, which are generated from the
// storefronts' own /api/vBeta/filters.
//
// An unrecognised filter gets axis null and is stored anyway. It is a real tag
// we cannot place, not a tag that does not exist — the same abstain the gold
// views use. Guessing an axis would file a product under a heading no one chose.

const AXIS_ROOTS = {
    "DMs Guild": {
        45341: "product_type", 45342: "edition",   45343: "setting",
        45423: "theme",        45468: "content",   45472: "campaign_expansion",
        45477: "language",     45544: "format"
    },
    "DriveThruRPG": {
        10: "genre", 20: "product_type", 30: "rule_system",
        40: "language", 44498: "format"
    }
};

// Promotional rails — Staff Picks, seasonal sales — hang off a phantom parent
// and are merchandising, not taxonomy. A title is not "a Roll20Con product".
const PROMO_PARENT = 999999;

function resolveAxis(store, filterAttrs) {
    const roots = AXIS_ROOTS[store] || {};
    const ancestors = Array.isArray(filterAttrs.ancestors) ? filterAttrs.ancestors : [];
    for (const a of ancestors) {
        if (roots[a]) return roots[a];
    }
    // A root itself has no ancestors but is its own axis.
    if (roots[filterAttrs.filterId]) return roots[filterAttrs.filterId];
    return null;
}

function isPromoFilter(filterAttrs) {
    const ancestors = Array.isArray(filterAttrs.ancestors) ? filterAttrs.ancestors : [];
    return filterAttrs.parentId === PROMO_PARENT || ancestors.indexOf(PROMO_PARENT) !== -1;
}

// ---------- Normalisation (sliced by scripts/test_catalog_detail.js) ----------
//
// Every coercion below was earned by a measured inconsistency, not anticipated:
//
//   pagecount is "104" (string) on DriveThruRPG and 27 (number) on DMs Guild.
//   The top-level filesize read 0 while files[0].size carried the real value.
//   reviewCount is written REVIEWS (28) while the star buckets are RATINGS (94)
//     — using reviewCount as the denominator of an average is wrong by 3x.
//   sku and isbn come back as "" rather than null when absent.
//
// The rule throughout: an absent value is null, never a zero and never an empty
// string. A zero is a measurement; null is the absence of one, and the gold
// views already abstain on nulls rather than scoring them.

function toInt(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = parseInt(String(v).replace(/[^\d-]/g, ""), 10);
    return Number.isFinite(n) ? n : null;
}

function toFloat(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = parseFloat(v);
    return Number.isFinite(n) ? n : null;
}

function toText(v) {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    return s === "" ? null : s;
}

function toList(v) {
    return Array.isArray(v) ? v.map(toText).filter(Boolean) : [];
}

function normalizeDetail(payload, store, collectedDate) {
    const root = (payload && payload.data) || payload || {};
    const a = root.attributes || {};
    const included = (payload && payload.included) || [];

    const publisher = included.find(x => x.type === "Publisher");
    const pub = (publisher && publisher.attributes) || {};

    // Star buckets are the rating denominator; reviewCount is not.
    const rr = a.reviewRatings || {};
    const buckets = [rr.countOne, rr.countTwo, rr.countThree, rr.countFour, rr.countFive]
        .map(toInt);
    const ratingTotal = buckets.every(b => b === null)
        ? null
        : buckets.reduce((s, b) => s + (b || 0), 0);

    const files = Array.isArray(a.files) ? a.files : [];
    const fileSize = files.length ? toInt(files[0].size) : null;

    const filters = included
        .filter(x => x.type === "Filter" && x.attributes)
        .map(x => x.attributes)
        .filter(f => !isPromoFilter(f))
        .map(f => ({
            filter_id: toInt(f.filterId),
            parent_id: toInt(f.parentId),
            axis: resolveAxis(store, f),
            label: toText((f.descriptions && f.descriptions[0] && f.descriptions[0].name)),
            depth: Array.isArray(f.ancestors) ? f.ancestors.length : null
        }))
        .filter(f => f.filter_id !== null);

    return {
        collected_date: collectedDate,
        source: store,
        product_id: toInt(a.productId),
        title: toText(a.description && a.description.name),

        publisher_id: toInt(a.publisherId),
        publisher_name: toText(pub.name),
        is_community_content: a.isCommunityContent === true,
        community_author_id: toInt(a.communityContentAuthorId),
        community_author_alias: toText(a.communityContentAuthorAlias),
        creator_level_id: toInt(a.creatorLevelId),

        page_count: toInt(a.pagecount),
        file_name: files.length ? toText(files[0].filename) : null,
        file_size_bytes: fileSize,
        scanned_pdf: a.scannedPDF === true,
        watermarked: a.watermarked === true,
        is_bundle: a.isBundle === true,

        price: toFloat(a.price),
        special_price: toFloat(a.specialPrice),
        lowest_digital_price: toFloat(a.lowestDigitalPrice),
        lowest_print_price: toFloat(a.lowestPrintPrice),
        on_sale: a.onSale === true,
        // A PWYW price is what the creator ASKS, not what buyers paid. Any
        // revenue proxy of price x units is wrong for these in an unknown
        // direction, so the flag has to travel with the price.
        is_pwyw: a.isPwyw === true,

        rating: toFloat(a.rating),
        rating_count: ratingTotal,
        review_count: toInt(a.reviewCount),
        rating_1: buckets[0], rating_2: buckets[1], rating_3: buckets[2],
        rating_4: buckets[3], rating_5: buckets[4],

        // Authoritative, per product, and present even off a bestseller shelf —
        // unlike the shelf-heading reading the weekly harvest still uses.
        ranking_tier: toText(a.ranking && a.ranking.humanName),

        date_created: toText(a.dateCreated),
        date_available: toText(a.dateAvailable),
        date_modified: toText(a.dateModified),
        file_last_modified: toText(a.fileLastModified),

        authors: toList(a.authors),
        artists: toList(a.artists),
        editors: toList(a.editors),
        contributors: toList(a.contributors),

        // Seller-declared, so this measures DISCLOSURE, not incidence.
        ai_disclosed: a.ai === true,
        handmade: a.handmade === true,

        sku: toText(a.sku),
        isbn: toText(a.isbn),

        filters: filters,
        taxonomy_version: store === "DMs Guild" ? "dmsguild-v2" : "drivethrurpg-v2",
        harvester_version: "detail-v1"
    };
}

// ---------- end normalisation ----------

// ---------- The pass ----------
//
// 2,453 products cannot be fetched in one service-worker lifetime — Chrome
// evicts after ~30s idle, which is exactly what stranded the first shelf
// harvest. So the pass is a RESUMABLE CURSOR: a queue in chrome.storage, a
// small batch per alarm tick, progress persisted after every batch. An eviction
// costs the current batch, never the run.

const DETAIL_ALARM = "detail-pass";
const DETAIL_BATCH = 20;        // products per tick
const DETAIL_SPACING_MS = 800;  // between requests; slow is fine, rude is not
const DETAIL_ENDPOINT = ENDPOINT.replace("ingest-catalog", "ingest-catalog-detail");

async function detailStart(queue) {
    await chrome.storage.local.set({
        detailQueue: queue,
        detailCursor: 0,
        detailStartedAt: Date.now(),
        detailErrors: [],
        detailDone: 0
    });
    chrome.alarms.create(DETAIL_ALARM, { periodInMinutes: 1, when: Date.now() + 1000 });
    console.log(`[Incursion] Detail pass queued: ${queue.length} products.`);
}

async function detailStop(reason) {
    await chrome.alarms.clear(DETAIL_ALARM);
    await chrome.storage.local.set({ detailQueue: [], detailCursor: 0 });
    console.log("[Incursion] Detail pass stopped: " + reason);
}

async function detailFetchOne(store, productId) {
    const cfg = DETAIL_STORES[store];
    if (!cfg) throw new Error("Unknown store: " + store);
    const url = `${cfg.api}/api/vBeta/products/${productId}` +
                `?groupId=${cfg.groupId}&siteId=${cfg.siteId}`;
    const res = await fetch(url, { credentials: "include" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return res.json();
}

async function detailTick(ritualKey) {
    const st = await chrome.storage.local.get(
        ["detailQueue", "detailCursor", "detailErrors", "detailDone"]);
    const queue = st.detailQueue || [];
    let cursor = st.detailCursor || 0;

    if (cursor >= queue.length) {
        if (queue.length) await detailStop("queue complete");
        return;
    }

    const today = new Date().toISOString().split("T")[0];
    const batch = queue.slice(cursor, cursor + DETAIL_BATCH);
    const rows = [];
    const errors = st.detailErrors || [];

    for (const item of batch) {
        try {
            const payload = await detailFetchOne(item.store, item.productId);
            rows.push(normalizeDetail(payload, item.store, today));
        } catch (e) {
            // One bad product must not stop the run, but it must not vanish
            // either — a silent skip is how a gap becomes a wrong denominator.
            errors.push({ productId: item.productId, store: item.store, error: e.message });
        }
        await new Promise(r => setTimeout(r, DETAIL_SPACING_MS));
    }

    if (rows.length) {
        const res = await fetch(DETAIL_ENDPOINT, {
            method: "POST",
            headers: { "Content-Type": "application/json", "X-Ritual-Key": ritualKey },
            body: JSON.stringify(rows)
        });
        if (!res.ok) {
            // Do NOT advance the cursor: the batch is unsent, so retry it next
            // tick rather than leaving a hole nothing will ever come back for.
            errors.push({ batchAt: cursor, error: `ingest HTTP ${res.status}` });
            await chrome.storage.local.set({ detailErrors: errors.slice(-200) });
            return;
        }
    }

    cursor += batch.length;
    await chrome.storage.local.set({
        detailCursor: cursor,
        detailErrors: errors.slice(-200),
        detailDone: (st.detailDone || 0) + rows.length
    });
    console.log(`[Incursion] Detail ${cursor}/${queue.length} (+${rows.length})`);

    if (cursor >= queue.length) await detailStop("queue complete");
}
