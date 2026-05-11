# DDB Fit Methodology Reference

Methodology documentation for the Trusight breakdowns' DDB Fit layer —
the brand-integrity-cost-ranked options-framing for IP content elements.
The per-IP applications now live inside the breakdowns doc itself
(see `trusight_breakdowns_scratch.md`). This file documents the
methodology, the cross-IP observations from the pilot, and the
methodology notes that inform extension to additional IPs.

For project-memory codification, see
`memory/project_ddb_fit_methodology.md`.

---

## The methodology — four principles

1. **Brand integrity is the dominant slot-budget constraint** — not
   engineering cost. WoTC's curatorial restraint over D&D's official
   corpus is what limits typical sourcebook content to ~15-25 monsters /
   1-3 subclasses / 5-15 magic items / 5-20 spells / 1-2 player options.
   The MTG Universes Beyond backlash is the live precedent WoTC is
   watching.

2. **Options, not prescriptions.** Each content element gets 2-4 ranked
   options for which existing 5e slot it fits into, with brand-integrity
   cost ranking. WoTC's design team makes the final choice. Trusight
   surfaces the menu and the trade-offs.

3. **Existing-slot-first hierarchy.** For every IP-derived element, the
   methodology asks four questions in order: (a) Does an existing 5e
   slot already hold this fantasy? (b) If yes, list 2-4 options for
   which existing slot, ranked by brand-integrity cost. (c) If no — does
   the addition genuinely need a new slot, or is that an over-reach?
   (d) Surface contextual factors that bear on the choice but aren't
   Trusight's call to make.

4. **HIGH-cost options are surfaced, not hidden.** A complete option set
   includes the high-brand-integrity-cost path so WoTC sees the breadth
   of the analysis and the trade-off space. Surfacing a few HIGH-cost
   options per IP demonstrates depth of signal without recommending them.

## Brand-integrity cost ranking — concrete examples

| Option pattern | Brand-integrity cost | Why |
|---|---|---|
| Reflavor existing magic item / spell / monster | **LOWEST** | Zero new content surface; pure IP-recognition layer |
| Refresh / update existing official subclass | **LOWEST** | Builds on existing official content; no new slot |
| Add to existing slot (one new subclass for existing class) | **LOW** | Fills a known slot; preserves class identity |
| New magic-item category (e.g., Charms-as-Potion-variant) | **LOW-MID** | New flavor, existing rules infrastructure |
| Multi-slot expansion at typical envelope (~15-25 monsters / 1-3 subclasses) | **LOW-MID** | Sits within historical crossover-sourcebook precedent |
| New rules variant (optional, doesn't replace core rules) | **MID** | Tasha's-style optional rules; opt-in |
| New base class (not subclass) | **HIGH** | Permanent commitment; rare in WoTC catalog (Artificer 2019 is the last) |
| New gameplay subsystem (Carve-Craft, etc.) | **HIGH** | Permanent commitment; conflicts with every future book |
| Multi-system overhaul (Souls-difficulty + crafting + ecology + ...) | **HIGHEST** | Effectively a new game-mode; brand-shifting |

---

## Where the per-IP analysis lives

The DDB Fit options-framing is integrated into each IP's *Translation
possibilities (DDB product surface)* section in the breakdowns doc.
Pilot coverage so far:

| IP | Status | Location in breakdowns doc |
|---|---|---|
| Hollow Knight | ✓ integrated | Section 13, *Slot options* subsection |
| Monster Hunter pair | ✓ integrated | Section 5, *Slot options* subsection |
| Berserk | ✓ integrated | Section 18, *Slot options* subsection |
| Mistborn | pending | Section 1 |
| Solo Leveling | pending | Section 2 |
| Omniscient Reader's Viewpoint | pending | Section 11 |
| Elden Ring | pending | Section 16 |
| Pillars of Eternity + Deadfire | pending | Section 8 |
| *(remaining 11 IPs)* | pending | Various sections |

---

## Cross-IP observations from the pilot

### 1. Every IP's signature mechanic has a brand-integrity-friendly reframe

The clearest pattern across all three pilot IPs:

| IP | Original "new subsystem" framing | Brand-integrity-friendly reframe |
|---|---|---|
| Hollow Knight | Charm-Notch attunement subsystem | Charms as consumable magic items |
| Monster Hunter | Carve-and-Craft full subsystem | Monster-Part Magic Items + Hunter Background "Carve" feature |
| Berserk | Cost-of-Power rules subsystem | Berserker Armor as single cursed legendary item |

**The lesson:** every "this IP needs a new subsystem" pitch should be
challenged. The flavor delivers through existing slots more often than
not.

### 2. Refresh existing content beats expanding the subclass shelf

Two of the three pilot IPs have a brand-integrity-friendlier option than
"ship a new subclass": **refresh existing official content**.
Hollow Knight can refresh an existing Monk subclass with Hornet-flavored
options; Monster Hunter can refresh the existing Monster Slayer Ranger
subclass from Xanathar's. Both options satisfy the recurring community
sentiment that under-loved older subclasses deserve attention — at zero
new-subclass-slot cost. **The refresh-existing pattern is a generalizable
lowest-cost path** to surface in the broader breakdowns doc.

### 3. Community-signal-strongest option ≠ brand-integrity-friendliest option

In all three pilot IPs, the community-engagement-leading option (Way of
the Needle Monk, weapon-class subclasses, Path of the Possessed
Barbarian) is *usually* the LOW-cost option — but not always the LOWEST.
The methodology's value is making this trade-off explicit so WoTC
chooses intentionally.

### 4. Slot-footprint discipline produces a cleaner pitch

The original pilot's deliverable answered "what should WoTC ship?" The
options-framed pilot answers "what's the lowest-cost option that
delivers the IP fantasy?" The second framing matches WoTC's actual
decision-rights better — they're the design experts; we surface the
menu and the brand-integrity profile, they pick.

---

## Methodology notes for extension

**What worked:**
- The options-framing per element (2-4 ranked options) is a cleaner
  deliverable shape than prescriptive recommendations.
- Brand-integrity-cost ranking gives WoTC's design team an intuitive
  ordering they can reason about quickly.
- Surfacing HIGH-cost options explicitly demonstrates breadth-of-analysis
  without recommending them.
- The "refresh existing content" path consistently lands as the LOWEST
  cost option when one exists — and it often does.

**What's hard / requires judgment:**
- Identifying *which* existing 5e content is the right refresh target
  requires both IP knowledge and current 5.5e architecture awareness
  (e.g., recognizing that Monster Slayer Ranger from Xanathar's is
  underused and a natural Monster-Hunter refresh anchor).
- The "mechanical-flavor combination" taxonomy doesn't fully automate;
  per-IP application is judgment-heavy work.
- Mature-content scope-definition (relevant for Berserk and any other
  adult-tone IP) is genuinely a separate analytical dimension that
  Trusight surfaces but doesn't decide.

**Honest scope estimate for full extension:**
- ~2 hours per IP at the integrated depth (per-element options tables +
  content surface preservation).
- 16 remaining IPs × 2 hours = ~32 hours of focused analytical work.
- Could be batched into 5-6 sessions of 5-6 hours each.

---

## Recommendation

The methodology produces a sharper deliverable than the original
breakdowns-only approach. Phased rollout:

1. **Selective extension to 5 high-leverage IPs** (Mistborn, Solo
   Leveling, ORV, Elden Ring, Pillars of Eternity pair) — the IPs most
   likely to enter live licensing conversation near-term. ~10 hours.
2. **Full extension across remaining 11 IPs** — only if the selective
   pass demonstrates value beyond what the breakdowns already capture.
3. **Park methodology as documented reference** — the methodology is
   captured in memory + this doc, and can be applied on-demand when an
   IP enters live conversation.
