# What a product page actually gives us

Probed 22 Sep 2026 against DMs Guild product 457996 (*Chains of Asmodeus*) and
DriveThruRPG product 535790 (*Single Player Mode*), live DOM via the DevTools
console.

**Both stores are the same application.** Same `phoenix-frontend` Angular build,
same JSON-LD shape, same `Page Count` markup, same `/en/product/{id}/{slug}`
URLs, same CDN image path, and product pages on both use the modern named-param
facet links. One extractor serves both; only the axis names differ.

## The headline: facets are per-product

A product page carries its own facet links, as real hrefs in the site's modern
named form:

```
setting (1)             45356-forgotten-realms
productType (3)         45381-core-rules · 45393-adventures · 45397-character-options
theme (3)               45532-planar · 45744-horror · 45747-dungeon
content (1)             45680-guild-adept
campaignExpansions (2)  1000055-previous-storylines · 45832-descent-into-avernus
edition (2)             1000261-5th-edition · 45462-5e
languages (1)           45483-english
```

So **one visit per product yields every facet plus page count plus the
structured data below.** No facet-walk over 169 browse URLs, no set-membership
reconstruction. This is the single most useful thing the probe settled.

### The probe over-collects — scope the extractor

The probe above scans **every anchor on the page**, so its output mixes this
product's tags with site navigation. On the DriveThruRPG product it reported
`productType=2810 :: Gift Certificates` for a Cyberpunk solo supplement, which
is plainly a nav link, not a tag. Note also that href carried a bare id with no
slug, so a URL parser must tolerate both `2810` and `2810-gift-certificates`.

For taxonomy work this does not matter: a real id with a real label is a real
facet value wherever it was linked from. For the **harvester** it matters a
great deal — a real extractor must scope to the product's own tag container,
not `document`. Treat per-product facet sets from this probe as an upper bound.

### It corrected assumptions on both stores

Recorded in `dmsguild_facets_v1.json` and `drivethrurpg_facets_v1.json`:

- **productType and edition are multi-valued.** This one product is
  simultaneously Core Rules, Adventures and Character Options. A one-per-product
  model would have discarded two thirds of that.
- **Two more values exist that no browse surface lists**: `45832-descent-into-avernus`
  and `45462-5e`. Three axes lost their `complete` flag as a result.

On DriveThruRPG the same probe found **three product types and one genre that
neither browse surface lists**: `2110-campaigns-adventures-modules`,
`1000756-solo`, `44823-gm-less`, and `510-cyberpunk`.

`2110` is the important one. This file previously stated, and built a cross-store
mapping hazard on, the claim that *DriveThruRPG has no Adventures product type*.
That was false — it exists, and is simply absent from both browse lists. The
lesson is the curated-surface rule again, but sharper: absence from a list is
not merely weak evidence, it actively produced a wrong conclusion that was
written down as a finding. Map value to value from observed product tags only.

`1000756-solo` is a nice illustration of how differently the two stores cut
things: DriveThruRPG files solo play under `productType`, DMs Guild under
`theme` (`45752-solo-single-player`). Same concept, different axis — the third
instance of that pattern after Bundles.

`45462-5e` deserves its own warning: it co-occurs with `1000261-5th-edition` on
the same product, so the two are duplicate handles for one concept, not
alternatives. **Any 5e count must union both ids.**

## Page count

A label/value pair of sibling `<p>` elements:

```html
<p class="">Page Count</p>
<p class="u-text-bold">286</p>
```

Read it by finding the element whose text is exactly `Page Count` and taking the
next sibling's text — never by position, and never by the `u-text-bold` class,
which is a utility class used all over the page. Expect the same pattern to
carry other spec rows (file size, format, publish date); worth dumping the whole
label/value block rather than just this one pair.

## JSON-LD

One `application/ld+json` block, `@type=Product`:

```
name · image · description · mpn · sku · brand · offers · aggregateRating
```

That is publisher (`brand`), price and availability (`offers`), and rating plus
rating count (`aggregateRating`) as clean structured data rather than scraped
text. Prefer it over DOM reads wherever the two overlap — it is the site's own
contract, and less likely to move than a class name.

## The product id is the real key

```
https://www.dmsguild.com/en/product/457996/chains-of-asmodeus
                                     ^^^^^^
```

Numeric, stable, and it also derives the cover image:
`https://d1vzi28wh99zvq.cloudfront.net/images/44/457996.webp`.

`catalog_supply` currently keys on `product_url`, which carries a slug that can
change when a title is renamed. **Extract `product_id` and store it alongside.**
It is the better join key and it is free.

## The constraint that changes the harvester design

**These storefronts are Angular single-page applications.** The console shows
`phoenix-frontend`, chunked bundles, and a router emitting `NavigationEnd`.

Two consequences:

1. **A plain `fetch()` of a product URL returns an app shell, not the product.**
   The BackerKit bookmarklet's same-origin-fetch-and-parse approach does not
   transfer here. Anything that reads the DOM must run after the app has
   rendered that route.
2. **There is a JSON API underneath.** The probe caught
   `api.dmsguild.com/api/vBeta/daily_deal/current?siteId=76&groupId=29`
   returning 404. So `api.<store>.com/api/vBeta/...` exists and the SPA is
   already talking to it.

If a product endpoint on that API returns the facets, page count and offer data
directly, the detail harvest becomes a paced sequence of JSON calls with no DOM
parsing and no tab-opening — far faster, far less brittle, and much gentler on
the storefront than rendering 2,453 pages. **Confirming that endpoint is the
next step, and it should happen before any harvester is written.**

If no such endpoint is usable, the fallback is rendering each product route in a
tab and extracting from the live DOM — correct but slow, and 2,453 of them needs
real pacing. Slow is fine; the weekly cadence means a first pass can take hours.

## Still unknown

- Whether file size, format and publish date sit in the same label/value block.
- How to scope the facet extractor to the product's own tag container.
- Whether the API exposes a product endpoint, and what it returns.
