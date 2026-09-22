# Taxonomy — use the storefront's, don't invent our own

## The decision

Product classification for `catalog_supply` uses **each storefront's own browse
facets** as the canonical vocabulary. We do not design a taxonomy and ask an LLM
to apply it.

`dmsguild_facets_v1.json` is the DMs Guild facet tree, captured from the live
filter sidebar. DriveThruRPG needs its own file; its facets differ, because it
sells the whole TTRPG market rather than D&D alone.

## Why

Two taxonomies already existed here, both invented, and neither was read by
anything downstream:

| | Written by | Categories |
|---|---|---|
| `primary_category` | `bouncer/main.py` `_enrich_batch`, on every ingest | Adventure, Setting Book, Sourcebook, Rulebook, Supplement, Dice & Accessories, Map & Terrain, Miniatures, Non-TTRPG, Other |
| `product_type` | `cloud_functions/catalog_enricher/` — no deploy script, so never deployed | Adventure, Setting Guide, Rules Supplement, Player Options, Maps & Assets, Monster/NPC Compendium, Solo Adventure, Accessory, Fiction, Other |

Three problems, and the third is the one that matters:

1. **They disagreed.** "Sourcebook" versus "Rules Supplement" for the same
   product, depending on which ran.
2. **Both had gaps.** The live one had no Player Options and no monster
   category — two of the largest categories on DMs Guild.
3. **Their categories overlapped.** "Sourcebook" / "Supplement" / "Setting Book"
   are not mutually exclusive, so the same product classifies differently run to
   run. That drift looks exactly like a market shift on a trend chart. It is the
   same failure shape as the V9 tier bug fixed in #144 — a number that moves for
   a reason that has nothing to do with the world — and it is harder to spot,
   because no arithmetic fails.

The storefront's own facets avoid all three:

- **Stable ids.** `45393-adventures` is a key, not a string to be matched.
- **Mutually exclusive where it counts**, because the site enforces it at
  tagging time.
- **Observed, not inferred.** A facet is read off the page. It cannot drift
  between runs, cannot hallucinate, and costs no tokens.
- **It is how the market describes itself.** Sellers choose these tags and
  buyers browse by them. When the question is "what is selling", the
  seller's own vocabulary is the right unit — our invented one would measure
  our categories, not the market's.

## What the facets do NOT cover

Use the LLM here, and only here:

- **5e 2014 vs 2024.** DMs Guild has one `5th Edition` value covering both.
  The edition transition is a live market question, so this needs another
  signal — description text, or the product's own edition wording.
- **Audience (player-facing vs GM-facing).** Partly implied — `Character
  Options` is player, `Resources for DMG Creators` is GM — but not stated for
  most product types.
- **Cross-storefront mapping.** DTRPG's vocabulary differs; something has to
  reconcile the two for combined analysis.
- **Products the seller tagged poorly or not at all.**

## Known limits

**Facets are seller-declared.** Creators choose their own tags, so there is
mis-tagging and under-tagging. This is still better than our inference — it is
the data the storefront's own discovery runs on, so it is what buyers actually
see — but a facet is evidence of how a product is *presented*, not ground truth
about what it contains.

**A missing facet is not a negative.** A product with no `setting` tag may be
setting-agnostic or may be untagged. Those are different facts. Record absence
as unknown; never as "Nonspecific/Any Setting".

**A browse list is a curated surface, not an enumeration.** This is the one that
cost the most to learn, and it holds on both storefronts:

- DMs Guild's *unfiltered* browse popup lists ten product types and omits
  **Monsters & NPCs** — yet `45395-monsters-npcs` is real, with a working browse
  URL, because the first capture was taken while browsing inside it.
- DriveThruRPG's browse popup carries a `ruleSystem` value, **Other systems**,
  that its metal.php sidebar does not.

**Why**, discovered later via the API (see `API_NOTES.md`): the facets are a
**tree**. Every value we found "missing" turned out to be a child — Cyberpunk
under Science Fiction, Campaigns/Adventures/Modules under Supplements &
Expansions. Browse lists show top-level nodes only. The rule below was the right
conclusion from the evidence; the reason is depth, not curation, and these flat
JSON files are a snapshot of a hierarchy. Record `parentId`/`ancestors` when
harvesting.

Two rules still follow:

1. **A value proven by a working URL outranks its absence from any list.** Keep
   it, and record how it was observed.
2. **Never read absence as a negative.** "Not in the list" means the list did
   not show it — nothing more.

Completeness is therefore tracked **per axis**, not per file. An axis is
`complete: true` only when it matched across two independent surfaces and shows
no search affordance. A searchable axis (DriveThruRPG's `ruleSystem` and
publishers) is never complete: the visible list is a shortcut.

**A UI control is not a facet value.** DriveThruRPG's `ruleSystem` list ends in
*Other systems*, which reads exactly like a catch-all bucket. It is not — it is a
disclosure control. Clicking **Refine** expands it to 30 further systems
(Shadowrun, Deadlands, GUMSHOE, Cortex, Palladium…), and that expanded list
*still* ends in "…and More!". Recorded as a value, it would have collapsed a
whole tier of the market into one bucket labelled "other", and every one of
those systems would have counted as unclassified. Expand anything that looks
like a catch-all before recording it; `scripts/test_taxonomy_facets.py` now
fails on labels of that shape.

The same axis is why counts here are floors, never totals: 48 values are known
and the storefront itself declines to say how many exist.

**A value may be known without its id.** If a surface shows a value but carries
no href — as the DriveThruRPG popup did for *Other systems* — record it with
`"id": null` and `"id_pending": true`, plus a note. Never invent an id: a wrong
one filters the wrong products and nothing complains. Never drop the value
either; that loses the knowledge that it exists. `scripts/test_taxonomy_facets.py`
enforces both, and fails any axis claiming completeness while holding a pending
id.

## Versioning

Every classified row carries `taxonomy_version` (`dmsguild-v1` and so on).
Storefronts add and retire facets — Arcavios and Feywild are recent, and more
will come. Without the stamp, rows classified under different vocabularies blend
silently and a category's apparent growth may just be the day it was introduced.
This is the `V9` / `V11` tag lesson applied to classification: when the rules
change, the data must say which rules it was written under.

## Adding a storefront

1. Capture the facets from **two** independent surfaces — the browse popup and a
   category sidebar, say. One is not enough; on both stores so far they
   disagreed.
2. Save as `taxonomy/<store>_facets_v1.json`, same shape.
3. Record ids, slugs and labels — ids are the join key, labels change. Mark any
   value seen without an href as `id_pending`.
4. Set `complete` per axis, `true` only where the two surfaces agreed and no
   search box is offered.
5. Note catch-all buckets ("More Settings", "Previous Storylines",
   "Miscellaneous", "Other") so analysis never treats them as peers of real
   values.
6. Run `python scripts/test_taxonomy_facets.py`.

## Also captured, deliberately not taxonomy

Price is a **range** filter on both stores, so it stays a numeric column.
`free` and `pay_what_you_want` are booleans worth keeping: a PWYW title lists at
$0.00 without being free, and price alone cannot separate the two — untracked,
every PWYW product joins the free pile and drags down any average-price or
price-per-page figure.

Named sales ("September Planescape Sale", "Roll20Con Sale") are weekly
merchandising, recorded under `promos_seen` only to be explicit that they are
not product attributes. A title is not "a Roll20Con product".
