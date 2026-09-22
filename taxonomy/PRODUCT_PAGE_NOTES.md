# What a product page actually gives us

Probed 22 Sep 2026 against DMs Guild product 457996 (*Chains of Asmodeus*), live
DOM via the DevTools console. DriveThruRPG not yet probed; it is the same
OneBookShelf platform, so expect the same shape and confirm rather than assume.

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

It also immediately corrected two assumptions, both recorded in
`dmsguild_facets_v1.json`:

- **productType and edition are multi-valued.** This one product is
  simultaneously Core Rules, Adventures and Character Options. A one-per-product
  model would have discarded two thirds of that.
- **Two more values exist that no browse surface lists**: `45832-descent-into-avernus`
  and `45462-5e`. Three axes lost their `complete` flag as a result.

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

- DriveThruRPG's product page — assumed similar, unverified.
- Whether file size, format and publish date sit in the same label/value block.
- Whether the API exposes a product endpoint, and what it returns.
