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

**Capture the tree unfiltered.** `dmsguild_facets_v1.json` was taken from a
sidebar already filtered to one product type, so each list shows only the facets
available *within* that type. Values in it are real; absence proves nothing. Its
`capture_complete` flag is `false` until it is retaken from a bare
`/en/browse`.

## Versioning

Every classified row carries `taxonomy_version` (`dmsguild-v1` and so on).
Storefronts add and retire facets — Arcavios and Feywild are recent, and more
will come. Without the stamp, rows classified under different vocabularies blend
silently and a category's apparent growth may just be the day it was introduced.
This is the `V9` / `V11` tag lesson applied to classification: when the rules
change, the data must say which rules it was written under.

## Adding a storefront

1. Capture the facet sidebar from an unfiltered browse page.
2. Save as `taxonomy/<store>_facets_v1.json`, same shape, `capture_complete`
   set honestly.
3. Record ids, slugs and labels — ids are the join key; labels change.
4. Note catch-all buckets ("More Settings", "Previous Storylines", "Other") so
   analysis never treats them as peers of real values.
