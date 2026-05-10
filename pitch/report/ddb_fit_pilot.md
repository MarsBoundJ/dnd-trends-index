# DDB Fit Pilot — Demand Discovery Methodology

Pilot test of a new analytical layer Phil articulated May 10 2026:
**"What does the D&D community latently desire that current DDB content doesn't satisfy, and which IPs supply it?"**

The existing breakdowns (`trusight_breakdowns_scratch.md`) measure community
appetite for IPs. They don't measure whether the licensed product would *fit
into the existing D&D Beyond inventory* by filling demand gaps the audience
has explicitly articulated.

This pilot tests a 3-source methodology on three IPs (Hollow Knight, Monster
Hunter, Berserk) to validate whether the framework produces genuinely new
findings before extending to all 19.

## Methodology — three sources, one cross-reference

**Source A — Reddit wish-list harvest.** Pattern-search the 5 D&D subs
(r/DnD, r/dndnext, r/UnearthedArcana, r/onednd, r/3d6) for posts that
explicitly articulate latent want — *"wish there was"*, *"missing from"*,
*"WoTC should add"*, *"home-brewed because"*, *"what would you want"*. 65
queries × 13 patterns × 5 subs ran May 10 2026. Filter to ≥30 ups.

**Source B — Homebrew texture analysis.** For each IP, pull the full body
text of top-engagement homebrew posts (≥15 ups) from existing harvests.
Apply texture-analysis lens: *what specifically about it caught fire?*
(mechanical uniqueness, action-economy fit, fantasy texture, power-level
controversy, name + aesthetic).

**Source C — Per-IP supply mapping.** Catalog each IP's content NOT at the
DDB-category level (creature type, spell school) but at the
*mechanical-flavor combination* level (e.g., "small-insectoid + grounded +
ranged-acid-attacker" rather than "Beast type"). Judgment-heavy.

**Cross-reference.** Build a matrix: rows = demand clusters from Source A;
columns = IPs from Source C; cells = strength of supply-demand match.

---

## Source A findings — 8 demand clusters

The wish-list harvest produced 16 strict-pattern hits at ≥30 ups. Modest
volume, but the surfaced clusters are concrete and recurring.

| Demand cluster | Strongest evidence | Engagement |
|---|---|---|
| **Missing class: Warlord** | r/3d6 *"What do people want in a Gish?"* — *"Every time the topic of 'what classes are still missing from the game?' comes up, the answer always tied with Warlord is a Gish."* | 121 ups, 259 comments — top answer in a cross-subreddit question |
| **Missing class: Gish (arcane-martial hybrid)** | Same thread, plus *"Make me your best barbarian caster multiclass?"* (r/3d6, 46u, 152c) | Persistent demand across multiple frames |
| **Higher-power-level characters (beyond Standard Array)** | r/dndnext *"Unpopular Opinion - I believe the Standard Array is too low for what most people want."* — **explicitly cites Guts from Berserk** as exemplar of "what people want to play but standard array can't represent" | **1,489 ups, 636 comments** — corpus-leading demand thread |
| **Active / dynamic martials** | r/dndnext *"If you're pining for more active martials, give this a try."* — *"complaining that martials should be more active is one of this subreddit's favorite things"* | 70 ups, 48 comments — the OP names the demand pattern explicitly |
| **Drop-in low-prep content** | r/dndnext *"What kind of third-party D&D content do you wish existed?"* — *"high-quality, drop-in content that I could just run with minimal prep"* | 37 ups, 27 comments |
| **Better Enchanter / charm-magic mechanics** | r/onednd *"Enchanter Feedback: Look How They Massacred My Boy"* — extensive critique of the 2024 Enchanter subclass rework | 130 ups, 44 comments |
| **Stronger Artificer** | r/onednd *"Artificer updates and how they're not far enough to keep the class competitive"* | 31 ups, 91 comments |
| **Player-facing magic-item wishlist as DM tool** | r/DnD *"Magic Item Wish List? Does it work?"* — references the *practice* of asking players to fill a magic-item wishlist | 66 ups, 117 comments |

**Honest caveat on Source A.** Reddit's full-text search isn't optimized
for demand-articulation patterns. The 16-hit yield from 65 queries is real
signal but undersamples the actual volume — there are many more wish-list
posts the strict pattern doesn't catch (paraphrased complaints, in-thread
comments rather than top-level posts, wish-lists embedded in feedback
threads). Treat this as a starting cluster set, not exhaustive.

---

## Source B findings — homebrew texture analysis

### Hollow Knight (top 10 D&D-context posts deep-read)

| # | Engagement | Title | Texture analysis |
|---|---|---|---|
| 1 | **1,161u, 66c** | *Monk: Way of the Needle — Bind, slash, and dance around the battlefield* | *Kinetic-melee-mobility fantasy*. Silksong-launch-momentum-driven. The promise of "bind, slash, dance" is **active-martial** in exactly the way Source A's *"more active martials"* cluster demands. Mechanical text not in body (image-link post) but title alone identifies the slot. |
| 2 | **749u, 4c** | *Hollow Knight inspired Races for dnd — Mothkin* | *"Mothkin... naturally possessing powers of light, and insights into the world around them"* + *"a dying race"* + *"Greatmoths"* lore. Texture: **moth-folk lineage with light-magic + divination affinity + tragic-extinction narrative**. The "dying race" tragic frame is unusually emotionally hooked. |
| 3 | **500u, 2c** | *Hollow Knight inspired Subclass for dnd (11/12) — Underground Domain Cleric + Songrisen race* | **Two specific mechanical innovations:** (a) *"Underground domain Cleric, powered by the Elder Kin of the Below... burrowing speed... turning tunnels into an enemy shredding cheese-grater... bless sections of a cavern at higher levels"* — **terrain-manipulation Domain that 5e does not have**. (b) *Songrisen race* — *"cicada-like kin, brought from ancient slumber by the sound of the Veinsong... awoken hundreds, if not thousands of years from the lives they remember in fractured dreams"* — **awakened-from-millennia-sleep narrative**. |
| 4 | 412u, 59c | *The Jaeger: A Momentum-Based Martial Class That Runs On Critical Hits* | (False positive — German "Jäger"=hunter, not HK-specific.) But notable: **crit-trigger resource model** is itself a demand pattern. |
| 5 | 382u, 13c | *Hollow Knight inspired Subclass — The Great Moth Sorcerer* | *"based off [the] Radiance boss and centred around using all sorts of light magics"* — **light-domain Sorcerer with phase-transition boss inspiration**. |
| 6 | 355u, 6c | *Hollow Knight X DnD Fusion - Mantis Lord + Treant = Briar Lord (CR 5 Medium Plant)* | **Plant-monstrosity hybrid at boss-tier**. The HK community is making boss-mashups by combining HK monsters with D&D archetypes. |

**HK pattern:** community converters are NOT just porting HK monsters into D&D — they're identifying *novel mechanical-flavor combinations* (terrain-manipulation Cleric, light-Domain Sorcerer phase-transition, kinetic-melee Monk) that 5e doesn't currently have. The demand they're filling isn't "more bug content" — it's specific mechanical gaps.

### Monster Hunter (top D&D-context posts deep-read)

| # | Engagement | Title | Texture analysis |
|---|---|---|---|
| 1 | **199u + 150u, 21c+9c** | *D&D 5e2014 Monster Hunter Monster Manual Update — Now 622 Pages — Every Monster From Every Mainline Game* | **Body text is the killer finding**: *"converting Monster Hunter to the 5th edition D&D. In it you can hunt monsters and **carve them to obtain materials with effect**. Then you can use them to effectively **make your own magical armor and weapons** similar to how it works in the video game."* Plus *"A guardian template you can apply to any of your monsters to make them Guardians like the ones you see in Monster Hunter Wilds"* and *"New conditions from the subspecies manual and Wilds: Frozen & Stench"*. **The community fan project is porting the entire MH gameplay loop — carve, craft, armor — not just monster stat blocks.** That's a whole new D&D gameplay layer. |
| 2 | **2,817u, 129c** | *I have a DnD one shot coming up...* | Photo of a printed-and-painted Nergigante used in place of "young red dragon" for a D&D session. **The IP's bestiary is already aesthetically integrated into D&D play** — players use MH miniatures as proxies for D&D monsters. |
| 3 | 268u, 50c | *The College of Sephirous Love (joke bard subclass)* | *"the idea for this subclass came from monster hunter"* — even joke subclasses cite MH as inspirational source. |
| 4 | 195u, 3c | *Vincent Vale, Monster Hunter [oc] [art] [comm]* | A D&D character whose **archetype is "Monster Hunter as profession"** — "*a way to do research a little bit easier"* + *"in setting guild that has been set up to connect monster hunters with communities"*. **Independent of the Capcom IP, "Monster Hunter as a D&D class fantasy" is a thing players want.** |
| 5 | 137u, 12c | *Monster Hunter Subclass compendium part 2! 10 subclasses based on monster hunter monsters!* | **Each iconic monster becomes a subclass** — the Hunter-becomes-the-Hunted-becomes-the-Hunter loop. Series at part 2 already; clear iteration. |

**MH pattern:** unlike Hollow Knight (mechanical-gap-filling), MH's
homebrew is **system-porting** — the community is bringing MH's *gameplay
loop* to D&D as a whole subsystem. The 622-page Monster Manual is not a
bestiary; it's a *crafting-and-progression rules variant*. That's a much
larger licensing scope than "monster art for a D&D book."

### Berserk (top D&D-context posts deep-read)

| # | Engagement | Title | Texture analysis |
|---|---|---|---|
| 1 | **1,489u, 636c** | *Unpopular Opinion - I believe the Standard Array is too low* | **Body explicitly cites Guts from Berserk.** *"Guts from Berserk? Man we know he's capped at Strength but he's fast, clever, and I'd even argue [...]"* — **direct demand-supply match**: the most-engaged power-level-fantasy thread in the corpus uses Berserk's protagonist as the example of "what 5e doesn't let me build." This is the gold-standard cross-source signal. |
| 2 | 88u, 19c | *Primal Path: Path of the Possessed* | *"Inspired by Kentaro Miura's 'Berserk,' here's my 1st draft of a Barbarian subclass that exchanges massive damage for the possibility of endangering your allies."* + *"FRIEND and FOE rolls"*. **Risk-everyone-for-power mechanic** — the Berserker Armor cursed-fantasy directly translated. |
| 3 | 22u, 1c | *My dnd character inspired by guts* | Direct character-conversion. Modest engagement but persistent across multiple posts. |
| 4 | 11u, 1c | *Magic Items: The Struggler's Slab (Legendary, A*)* | **Guts's Dragonslayer sword as 5e legendary item**. The IP-supply maps directly to the *power-level-fantasy* demand cluster from Source A. |
| 5 | 11u, 4c | *Created a "Grunbeld" stat block* | One of Berserk's iconic Apostles as a 5e monster. **Apostle-tier monsters as encounter-design innovation**. |

**Berserk pattern:** the community engagement is *concentrated on the
power-fantasy axis* — players want the Guts-tier character that 5e's
balance gates against, and they're independently building it via subclass
+ legendary item + stat block. The 1,489-up thread explicitly proves the
demand exists; the homebrew artifacts demonstrate fans are filling it
themselves at modest scale.

---

## Source C — per-IP supply at the mechanical-flavor combination level

### Hollow Knight — supply catalog

| Mechanical-flavor combo | DDB-corpus uniqueness | Source-B evidence |
|---|---|---|
| Small-insectoid + grounded + ranged-acid/spore-projectile attacker | **Rare**. 5e's ranged-attack monsters are mostly humanoids, dragons, or large monstrosities. Aspids, Mosskin, etc. fill a documented gap. | Phil's worked example |
| Charm-economy magic items (modular slot-based attunement) | **Novel paradigm**. 5e's attunement system is binary (attuned or not); a Charm-Notch system is structurally new. | The 269-add Vessel species canonical artifact has Charm slots |
| Soul-as-resource (combat-driven generation, distinct from spell slots) | **Novel resource model**. | HK Way of the Needle Monk (1,161u) presumably uses Soul-style resource |
| Vessel-class blank-slate PC (silent-protagonist meta-narrative) | **Absent from 5e** — no canonical "blank vessel" species/subclass. | 269-add Vessel species (the corpus headline single artifact for HK) |
| Terrain-manipulation Domain Cleric | **5e does not have a Domain that does this.** Closest analogs (Grave, Order, Forge) don't manipulate terrain. | 500u Underground Domain Cleric homebrew |
| Bug-people lineage diversity (Mantis, Mothkin, Mosskin, Hivebee, Grimmkin, Songrisen, Pale Beings) | **5e has 1-2 insectoid PC species at most.** Body-plan diversity is unusually thin. | 749u Mothkin race homebrew + 500u Songrisen race |
| Multi-form / phase-transition bosses (Radiance, Nightmare King Grimm) | **Underrepresented**. 5e bosses occasionally have phase mechanics (Vecna, Strahd) but not as IP signature. | 382u Great Moth Sorcerer references Radiance |
| Plant-monstrosity hybrid bestiary | **Rare** — most plant creatures in 5e are large (Treants) or low-CR (Awakened Tree). | 355u Mantis Lord + Treant fusion |

### Monster Hunter — supply catalog

| Mechanical-flavor combo | DDB-corpus uniqueness | Source-B evidence |
|---|---|---|
| 14 distinct weapon-class subclasses (each with unique combat pattern) | **5e has subclass variety within classes but no IP-driven 14-subclass set keyed to weapon-mastery.** | 137u Subclass Compendium pt 2 |
| Carve-craft-armor full gameplay loop | **Absent from 5e core.** Crafting rules exist (XGtE) but as DM-tool, not gameplay loop. **A licensed MH product would import a whole new D&D gameplay subsystem.** | 199u Monster Manual body text confirms full system |
| Hunting-commission episodic campaign frame | **Underrepresented** — 5e doesn't have a "Guild commission of the week" canonical structure. | 195u Vincent Vale "monster hunter as profession" character |
| Felyne / Palico companion species (cat-folk hunter-companion) | **Tabaxi exists but as humanoid PC, not companion-class.** Cat-folk-as-Familiar-variant is novel. | Implied in MH bestiary |
| Boss-monster ecology (100+ named iconic monsters with relationships) | **5e doesn't have IP-anchored boss-rosters at this scale.** | 622-page Monster Manual literally is this |
| Wyverian Sage (long-lived sage-race lineage) | **Absent from 5e.** | Implied |
| Guardian-template / monster-augmentation system | **Novel.** 5e templates exist (Demilich, Lich) but not as modular augmentation. | 199u Monster Manual body text mentions Guardian template |

### Berserk — supply catalog

| Mechanical-flavor combo | DDB-corpus uniqueness | Source-B evidence |
|---|---|---|
| Cursed-power-armor (Berserker Armor) — power-with-cost mechanic | **5e has nothing in this slot.** Cursed items exist but as DM-trap, not player-build option. | 88u Path of the Possessed Barbarian + 11u Struggler's Slab |
| Brand-of-Sacrifice cursed-summons (debuff that periodically attracts demons) | **Novel.** 5e curses are usually static; periodic-encounter-attracting curse is encounter-design innovation. | Implied in Berserk lore |
| Apostle: humanoid-transforms-into-CR12+-monster mid-encounter | **Novel encounter-design.** 5e shapeshifters (Werewolves) don't transform mid-fight at boss-tier. | 11u Grunbeld stat block |
| Oversized-2H-melee with on-kill resource | **5e Barbarian doesn't quite deliver this.** Brute-Force-with-momentum-resource is the missing slot. | **1,489u Standard Array thread cites Guts as the example** |
| Schierke witchcraft (Wiccan-flavored elemental magic) | Some overlap with Druid; but witch-flavored magic is underrepresented vs canonical Wizard/Sorcerer/Warlock. | Lore reference |
| Skull Knight (ally-or-rival NPC archetype, intermittent powerful aid) | **Absent from 5e canonical NPC archetypes.** Most 5e NPCs are static-allies or static-enemies. | Lore reference |

---

## Cross-reference matrix

Demand clusters from Source A × IP supply from Source C. Cells: empty = no
match; `✓` = partial match; `✓✓` = strong match; `✓✓✓` = direct empirical
demand-supply confirmation.

| Demand cluster (Source A) | Hollow Knight | Monster Hunter | Berserk |
|---|---|---|---|
| Missing class: Warlord | — | — | — |
| Missing class: Gish (arcane-martial) | ✓ (Great Moth Sorcerer is sorcerer-martial-adjacent) | ✓ (Charge-Blade-Hunter is martial-with-spell-elements) | ✓ (Schierke as Wizard-with-melee-cantrip flavor) |
| **Higher-power-level (Guts-tier) characters** | — | — | **✓✓✓** *— 1,489u thread cites Guts by name; Berserker Armor + Dragonslayer is the supply* |
| **Active / dynamic martials** | **✓✓✓** *— 1,161u Way of the Needle Monk is the empirical hit; "bind, slash, dance" is the demand verbatim* | ✓✓ (14 weapon-class subclasses are mechanically active by design) | ✓✓ (Path of the Possessed is high-risk active) |
| Drop-in low-prep content | ✓ (Charm magic-items are drop-in flavor) | **✓✓** (Episodic Hunting Commissions = drop-in adventure structure) | ✓ (Per-Apostle one-shot encounters) |
| Better Enchanter / charm-magic mechanics | ✓ (HK Charm-Notch system is adjacent to Enchanter rework concerns) | — | ✓ (Schierke Wiccan-charm flavor) |
| Stronger Artificer | — | ✓✓ (MH crafting-loop is Artificer-adjacent gameplay) | — |
| Magic-item wishlist as DM tool | ✓ (HK Charms ARE a player-curated wishlist by design) | ✓✓ (MH crafting-loop is literally player-driven equipment-wishing) | — |
| **Bug-monster mechanical variety (insectoid + grounded + ranged)** | **✓✓✓** *— Phil's example confirmed by HK bestiary depth* | — | — |
| **Crafting-from-monster-parts loop** | — | **✓✓✓** *— 622-page Monster Manual already ports the system* | — |
| **Cursed-power-fantasy (cost-of-power)** | — | — | **✓✓✓** *— Berserker Armor is the canonical mechanic* |
| **Terrain-manipulation magic** | **✓✓✓** *— 500u Underground Domain Cleric* | — | — |
| Multi-form / phase-transition bosses | ✓✓ (Radiance, Nightmare King Grimm) | ✓ (Elder Dragons have multi-form variants) | ✓✓ (Apostle transformation = phase trigger) |
| Bug-people / unusual lineage diversity | ✓✓✓ (Mantis, Mothkin, Mosskin, Songrisen — community-built at 749u + 500u) | ✓ (Wyverian Sage, Felyne) | — |
| Boss-monster ecology at IP-scale (100+ named) | — | ✓✓✓ (the 622-page Monster Manual IS this) | — |

### What the matrix surfaces

**Each IP wins on demand clusters the others can't fill:**

- **Hollow Knight** uniquely fills: bug-monster mechanical variety, terrain-manipulation magic, bug-people lineage diversity, and (via Way of the Needle) the active-martials demand. The HK pitch becomes: *"WoTC's Hollow Knight book ships the active-martial Monk subclass the community has been asking for, plus a terrain-manipulation Domain Cleric, plus six bug-people species variations — none of which 5e currently has at this density."*

- **Monster Hunter** uniquely fills: crafting-from-monster-parts loop, hunting-commission campaign frame, IP-scale boss-monster ecology. The MH pitch becomes: *"WoTC's Monster Hunter book ships an entire new gameplay subsystem (crafting from monster parts) that 5e has wanted since Tasha's didn't quite deliver. The 622-page community Monster Manual is the proof-of-concept."*

- **Berserk** uniquely fills: cursed-power-fantasy, higher-power-level characters (Guts-tier — directly demand-cited), and via Path of the Possessed contributes to active-martials. The Berserk pitch becomes: *"WoTC's Berserk book ships the cursed-power-armor and Guts-tier character options that the 1,489-up Standard Array thread explicitly demanded."*

**Two demand clusters are unmet by all three IPs in the pilot:**
- *Missing class: Warlord* — none of HK/MH/Berserk supply this directly. Dwarf Fortress's succession-game-as-DM-rotation is closest but different.
- *Stronger Artificer* — MH partially fills via crafting-loop adjacency.

The **Standard Array power-level demand** + Berserk's direct citation in
the source thread is the cleanest demand-supply match in the pilot. That
finding alone validates the methodology — it's the kind of insight the
category-counting approach would have entirely missed.

---

## What this means for the breakdowns

If the methodology extends to all 19 IPs, each entry would gain a
**"DDB Fit"** section with three sub-parts:

1. **Demand clusters this IP fills** — bullet list referencing specific
   Source-A clusters with cross-source evidence.
2. **Mechanical-flavor combinations the IP uniquely supplies** —
   Source-C catalog filtered to combos that aren't well-served by the
   existing DDB corpus.
3. **Recommendation revision** — does this DDB-fit analysis change the
   licensing recommendation from the existing breakdown?

For HK, MH, and Berserk specifically, the DDB-fit analysis **strengthens
the existing recommendations** rather than reversing them:

- **HK:** Greenlight reinforced — the active-martial + terrain-manipulation
  + bug-people supply maps to documented demand the existing breakdown didn't
  surface.
- **MH:** Greenlight reinforced — the crafting-loop framing makes the
  licensing scope *bigger* than just monster-and-weapon-subclasses; it's
  a new gameplay subsystem.
- **Berserk:** Strengthens the existing greenlight with a now-empirical
  demand citation (the 1,489-up thread cites Guts by name).

---

## Methodology notes for extension

**What worked:**
- Source A wish-list pattern harvest is real signal even at 16 hits — the
  clusters cleanly cross-reference to homebrew evidence.
- Source B texture analysis on top-engagement homebrew bodies surfaced
  specific mechanical innovations (Underground Domain Cleric, Carve-Craft
  loop, Path of the Possessed) that title-only analysis would miss.
- Source C mechanical-flavor mapping required IP knowledge but produced
  cleaner DDB-fit signal than DDB-category counting would have.
- The cross-reference matrix is the deliverable that ties it all together
  — without it, the three sources are interesting but disconnected.

**What's hard / requires judgment:**
- Source A is undersampled by Reddit search; full coverage would need a
  longer-running scrape or pushshift-archive access.
- Source B requires reading homebrew bodies (titles aren't enough); when
  posts are image-link-only, we work from title + comment context only.
- Source C is judgment-heavy and IP-knowledge-dependent. For IPs where
  knowledge is shallower (e.g., FFXIV's Stormblood/Endwalker arcs), the
  mapping should be sanity-checked against fan wikis.
- "Mechanical-flavor combination" taxonomy isn't formally codified — it's
  pattern-recognition over IP content.

**Honest scope estimate for full extension:**
- ~1.5 hours per IP for full Source-B + Source-C work (Source A is shared
  across IPs).
- 16 remaining IPs × 1.5 hours = ~24 hours of focused analytical work.
- Could be batched into 4-5 sessions of 5-6 hours each.
- Output: each existing breakdown gains a ~30-40 line "DDB Fit" section
  + the cross-reference matrix grows to 19 columns.

---

## Recommendation

**The methodology produces genuinely new findings.** The Source-A wish-list
clusters + Source-B texture analysis + Source-C mechanical-flavor mapping
+ cross-reference matrix together surface insights the existing
breakdowns can't see.

**The strongest single finding from the pilot:** the 1,489-up Standard
Array thread explicitly citing Guts from Berserk as the example of "the
character D&D doesn't let me build." That's an empirical demand-supply
match with the kind of citation strength that lands in a licensing
diligence conversation.

**Phil's call:**
- (a) Extend to the remaining 16 IPs in batches across multiple sessions
- (b) Selectively extend to the 4-5 highest-leverage IPs (Mistborn, Solo
  Leveling, Elden Ring, Hollow Knight, Monster Hunter, Berserk) and leave
  the rest as-is
- (c) Park as documented methodology and revisit when the breakdowns are
  shipping into licensing conversations
