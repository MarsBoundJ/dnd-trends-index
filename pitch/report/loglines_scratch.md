# A1 LIVE Audit — Logline Drafts (Claude)

Scratch document for choosing between Claude / Perplexity / Gemini loglines before
committing them to the build script. Format per entry:

- **Header / Lead-in** (matches the bold-ember lead-in in the doc)
- *Proposed logline* (italicized, would render under the lead-in, above the prose)
- The existing dense paragraph (for context — what the logline is summarizing)

A `←` flag at the end of a logline means this is one I'd actively want a second
opinion on.

---

## Section 1 — What we ran

### The setup.

*A fresh UA packet landed Tuesday morning. We ran the pipeline against it the same day, no cherry-picking. Here's what came out.*

> Phil supplied the source PDF (UA2026-VillainousOptions02.pdf, 5 pages, 3 subclasses + 2 invocations) on May 4, 2026. We ran the A1 pipeline against it: harvest community discussion across Reddit, three top TTRPG forums, and the major D&D press/blogosphere; apply the UA-content classifier to every captured post; aggregate the per-subclass tags into a structured fingerprint.

### Coverage of the five channels.

*Reddit's our 80% front-row chorus; three top TTRPG forums and the press are the balcony critics adding nuance. Total catch: 442.*

> All five channels harvested. Reddit returned the strongest single signal — 9 dedicated discussion threads with 1,015 aggregate comments; we extracted the 359 highest-scoring top-level and first-reply comments for classification. Three top TTRPG forums each contributed independent signal: 7 substantive replies from Top Forum #3, 22 VO2-window posts from Top Forum #2, 48 posts from Top Forum #1. Press/blog coverage spans six outlets. Total dataset: 442 classified items.

---

## Section 3 — Path of Lament · Barbarian

### Top endorsements (flavor_endorsement tag).

*The Banshee/grief concept is doing the work. Readers across every channel want it on their character sheet.*

> The flavor lands. The structured posts overwhelmingly tag the Banshee/grief concept as a creative win:

### Top design critiques (per UA tag).

*Three specific friction points, each a different kind of fix. The tag tells you which lever to pull.*

> Three design concerns surfaced consistently across sources, each tagged into the UA-content schema:

### Minority dismissal — the only flavor_critique signal.

*One reader dropped a snarky "emo phase" comment. That's the entire flavor critique signal — caught but doesn't move the needle.*

> One comment (+6 ups) read "Emo phase." That's the entire flavor_critique tag volume for this subclass — a single low-engagement dismissal. The classifier captures it but the volume is too small to flag a backlash pattern.

### Actionable design read.

*Two polish passes and this one's ready to ship. The community already built the case for you.*

> Ship as-is with two specific polish passes: (1) make Otherworldly Anguish L10 instakill threshold scale to 4× Barb level instead of 2×; (2) make Sorrow Form's Undead-creature-type opt-in rather than automatic. Banshee's Wail mechanic is already net-positive — the "clunky" complaints are outweighed 4-to-1 by endorsements of the same feature.

---

## Section 3 — Warrior of Venom · Monk

### Top endorsements.

*Poison-themed monk is something the community has been waiting for. They're not subtle about it.*

> The poison-monk concept resonates, especially as a long-requested archetype:

### Top design critiques (per UA tag).

*Four design issues — each with a different tag, each with a different fix. The classifier separates them so you can address them separately.*

> Four design issues recurring consistently across all five channels:

### Press confirmation.

*When ScreenRant titles a piece "Free Game-Breaking Update," that's the same signal we just classified — externally validated.* ←

> ScreenRant published a standalone piece titled "New D&D Subclass Has A Free Game-Breaking Update" arguing the Slowing Toxin no-save mechanic is broken, urging players to exploit it before the inevitable nerf. That's an external classifier-grade signal independently confirming the community's overpowered tag on Slowing Toxin.

### Actionable design read.

*Three fixes before publication. The press piece means one of them is already on the design team's radar.*

> Ship with three required fixes before publication: (1) make Hallucinogenic Breath a cone, (2) extend Envenom Weapon to unarmed strikes, (3) errata Toxin Refiner to close the Bag-of-Rats loophole. Slowing Toxin balance is already on WoTC's radar via the press piece — design team likely has a draft response.

---

## Section 3 — Primordial Patron · Warlock

### Top endorsements (limited).

*The vibe lands but the love is thin. Appreciated, not adored.*

> The vibe lands but reception is thin — "hyped" and "raid boss energy" are praise, but not enough to outweigh the critique mass:

### Top design critiques (per UA tag).

*Five issues with cross-source support. The biggest one (friendly fire on the Node) is the highest-upvoted comment of the entire discussion.*

> Five issues with high-volume support across sources. The top critique (Elemental Node friendly fire) is the highest-upvoted comment in the entire primordial discussion thread:

### theme_frame_mismatch — the distinguishing tag of the packet.

*The community isn't rejecting the subclass. They're rejecting the "villainous" sticker on it. Different signal entirely.* ←

> Unlike the other two subclasses, Primordial Patron triggered a sustained "this isn't actually villainous" thread. Unlike a Goblin-Slayer-tier flavor_critique (where the tone is genuinely off-putting), this is a theme_frame_mismatch — the community thinks the subclass IS interesting, just not under the "Villainous Options" framing.

### Actionable design read.

*This one needs more than polish. Five specific design fixes, plus a marketing-team call about the framing.*

> Significant rework required before publication: (1) Elemental Node MUST exclude allies / chosen creatures, (2) increase node uses or make moving free, (3) rebalance against existing warlock subclasses, (4) reconcile Elemental Transmutation invocation with Sorcerer's Transmuted Spell metamagic, (5) Earth spell list needs additions. Plus a brand-marketing call: this subclass is fine, but framing it under "Villainous Options" creates an avoidable theme_frame_mismatch.

---

## Section 3.5 — Cross-channel divergence

### The structural insight.

*Different channels hear different things on the same content. That disagreement is what tells the design team who's reacting to what.*

> The five channels do not produce identical readings. Reddit, the three top TTRPG forums, and the press each have distinct cultures — and each culture produces a meaningfully different fingerprint on the same content. Single-source listening would miss this. Triangulation is the diagnostic.

### Per-subclass cross-channel stance.

*Each row is one channel's verdict. Where rows disagree, the disagreement is the actionable bit.*

> Each row below is one channel's verdict on each subclass. Where the row diverges, that divergence is itself the actionable signal — it tells the design team WHICH audience is reacting WHICH way.

### Three divergence patterns worth naming.

*The optimization crowd reads math, the worldbuilding crowd reads vibe, the press reads news. Different audiences, different verdicts — each pulling on a different lever.* ←

> (1) Path of Lament reads GREEN on every channel except Top Forum #2, whose mechanically-rigorous forum culture produced a long-form critique flagging the L3 feature as substantially underpowered. Reading: the worldbuilding-oriented audience loves the Banshee/grief flavor; the optimization-oriented audience reads the math and finds it underweight. Both signals are real and need different responses. (2) Warrior of Venom reads consistently GREEN-YELLOW across all four community channels — high concept-love, recurring critique on three specific design issues. Cross-channel agreement at this level is rare and high-conviction. The Sedative-toxin overpowered tag alone is triangulated across 5 independent voices and 4 channels — that's a publishable finding. (3) Primordial Patron diverges most sharply: Reddit reads YELLOW-RED, while all three top forums and press read warmer. Reading: Reddit's conversation is gated by the +47-upvote Elemental-Node-friendly-fire complaint that dominates the megathread. The other channels weight the elemental-evil flavor higher and the friendly-fire issue lower. Action: fix the node, the elemental-evil framing pulls reception up across all channels.

### The theme_frame_mismatch signal — strongest from Top Forum #1.

*One forum subthread asked the same question eight different ways: are these actually villainous? The label landed differently than the content.*

> Top Forum #1 produced the most concentrated "these aren't actually villainous" subthread of any source. Eight forum posts across five distinct readers questioned whether the "Villainous Options" framing fits the content. Aggregate sentiment of the subthread, paraphrased for storage:

### Why this matters.

*Disputing the label isn't rejecting the content. That's a marketing problem, not a design problem — different team, different fix.*

> Critically — this is NOT flavor_critique in the Goblin-Slayer sense (where the content is rejected as off-tone). The community engages WITH the content; they dispute the LABEL. That's a theme_frame_mismatch — actionable for marketing, not for design.

### Why this matters for the pitch.

*WoTC's survey gives one number. Trusight gives five — one per channel — so the design team can see who's reacting and how.*

> WoTC's official survey gives the design team one number per question, summed across the entire respondent pool. The cross-channel divergence is invisible to that survey because it never asks "which channel culture do you come from?" Trusight surfaces the divergence by triangulating across channels with distinct response signatures. That structural visibility is impossible from a single horizontal-listening tool, impossible from the official survey, and impossible from any one channel read in isolation. Five channels, five signatures, one structured fingerprint — that's the product.

---

## Section 4 — How the classifier is built

### The principle.

*One classifier can't do every job. UA reception, IP licensing, and brand-trust events all have different failure modes — so each gets its own tag set under the same architecture.*

> A horizontal social-listening tool would tell you "the community talked about this UA — average sentiment was mildly positive." That doesn't help. Trusight's classifier produces a structured fingerprint, and the fingerprint is the actionable artifact. But the right tag set depends on the event being measured. UA-content reception, IP-licensing reception, and brand-trust events have structurally different failure modes; each gets its own classifier schema, tuned to the decision the audit informs.

### Same architecture, different schemas.

*The pipeline stays the same. The tags swap depending on what kind of event the audit is reading.*

> The classifier accepts a thread, applies its current tag set, and emits a multi-label fingerprint per post. The architecture is uniform; the tag content differs by event class. Two of the schemas are shown below; a third (brand-trust events) sits in the production roadmap and is referenced in Section 7.

### Why the schemas can't be merged.

*Forcing one universal tag set creates noise on one side and missing alarms on the other. Specialization wins.*

> A single universal tag set would have to either (a) include cash_grab as a tag for UA, where it's structurally inapplicable and produces noise, or (b) drop cash_grab from IP work, where it's the canonical signal of an IP-licensing failure. Either choice degrades classifier accuracy on one event class to fit the other. The architectural answer is to keep the schemas separate, share the same uniform fingerprint shape, and select the schema based on what the audit measures.

### What this means in practice.

*Three different alarms for three different problems, each routing to the right team. A single sentiment score collapses them all into one beep.* ←

> When the UA-content classifier fires "overpowered HIGH" on a Sedative toxin, that's a distinct alarm from when the IP-reception classifier fires "tone_mismatch HIGH" on a Goblin Slayer crossover, which is again distinct from when a brand-trust classifier fires "cash_grab + not_dnd HIGH" on a policy announcement. Three separate decision actions, three separate audiences inside WoTC. A single-scalar sentiment score collapses all three into the same number; event-class-specific schemas keep them separate and actionable.

---

## Section 5 — Speed

### WoTC's current decision timeline.

*WoTC's clock starts late by design — they want playtesting before reaction. The structured-feedback wait stretches weeks to months. Long, variable, painful.*

> WoTC intentionally delays opening the official UA feedback survey for 14-21 days after a packet drops, so the community can actually playtest the material rather than react to a first read. The survey then stays open another two to three weeks. After it closes, WoTC's internal data team must process tens of thousands of long-form responses before the design team gets a structured report. Public results videos for past One D&D playtest cycles have landed several weeks to a couple of months after a survey closes — Playtest 8's published results came roughly six to ten-plus weeks after release. WoTC has no formal SLA on results publication; the structured-feedback wait is variable but consistently long.

### Trusight's decision timeline.

*Imagine getting an answer from the D&D community before you even send the UA survey out.*

> This audit ran on T+11. The structured per-subclass verdicts, top critique themes per UA tag, and supporting evidence are available today. WoTC's official survey for this packet does not even open until on or after May 7 (T+14) and closes May 14 (T+21). The classifier signal is also re-runnable: same query, same sources, every Tuesday morning. The picture sharpens as the discussion volume grows; by T+21 (survey close) the signal is based on a larger, behavior-driven sample than the official opt-in survey alone.

### The decision-window claim, calibrated.

*Lead time is variable, not contractual. The honest pitch claim: a structured verdict in hand a month-plus before the official one lands.*

> The exact lead time depends on when WoTC publishes — which is variable, not contractual. What is structurally guaranteed: today's audit precedes the official survey's open, the survey's close, and the published results, in that order. The smallest defensible lead time is the gap between Trusight's T+11 read and WoTC's survey-open at T+14 (about three days). The largest, against the longest historical results-publication windows, is on the order of two months. The pitch claim we stand behind: a structured per-subclass verdict in hand a month-plus before the design team sees an official one.

### Calibration disclosure.

*We'll show our work when WoTC publishes. If we got it right, the speed claim is validated; if we missed, we say where and why.*

> The speed advantage above is a methodological claim, not yet an empirically validated one — WoTC's official survey hasn't published. We commit to a calibration paper when the official results land, comparing this audit's T+11 directional verdict against the published result. If the directional read holds, the speed claim is empirically validated. If it diverges, we publish where and why. Until the calibration paper, treat the verdicts above as directionally usable, not authoritative.

---

## Section 6 — Methodology, confidence & calibration

### This section reads against ourselves.

*We're a new tool. Nothing's bulletproof. Calibrated honesty here is the basis for the trust the buyer has to extend to use us at all.*

> Trusight is a new tool. Sophisticated buyers — a Snowflake/dbt user, an Insights team validator, a stats-trained analyst — discount any pitch that reads bulletproof. They know nothing is. The audit's findings are real, but the document above presents them more firmly than the underlying data warrants in places. This section names where, and why, and what would firm them up. Calibrated honesty here is not apology; it's the basis for the trust the buyer has to extend to use the tool at all.

### Sample composition (and the bias it creates).

*Reddit's our 80% front-row chorus; TTRPG forums and blogs are the quiet critics in the balcony, adding nuance, second opinions, and the "are we sure?" details.*

> 359 of 442 items (81%) are from Reddit. Reddit's culture is more reactive — early, loud, opinion-strong — than the smaller TTRPG forums. This means the dataset over-represents the early-and-loudest demographic, and any finding that appears ONLY on Reddit deserves lower confidence than one triangulated across 3 or more channels. The smaller forums (Top Forum #1 n=48, Top Forum #2 n=22, Top Forum #3 n=7) are the corrective signal — when they agree with Reddit, confidence rises sharply; when they diverge, the divergence is itself the finding. The cross-channel table makes the divergences visible.

### Selection bias on the rate threads.

*The Reddit rate threads use WoTC's own Green/Yellow/Red system — happy accident. Both surveys have self-selection bias, so we're comparing like to like.*

> The r/onednd "How are you going to rate" threads happen to mirror WoTC's own Green/Yellow/Red survey rating system — a methodological gift that lets us read the same vote structure their survey will eventually receive. But the people who write structured rating posts are self-selected for engagement and opinion strength. The silent majority's verdict could differ. The official WoTC survey has the same self-selection problem (only opt-in respondents), so the comparison is apples-to-apples — but neither is a random sample of the player base.

### Per-finding confidence grades.

*Some findings are concrete. Some are directional. We mark which is which, so a buyer can read every claim with the right amount of weight.*

> The strongest findings are the cross-source-triangulated ones (Venom Sedative-toxin overpowered tag, the structurally different fingerprints between event-class schemas, Lament's flavor_endorsement on 4 of 5 channels). The medium-confidence findings are the per-subclass stance percentages (treat as ±10 points, not exact) and the cross-channel stance divergences themselves (real signal, fragile magnitudes). The weakest-but-still-valuable findings are the channel-culture interpretations and the Top Forum #3 column (n=7 — directional only). The forward-not-yet-validated findings are the speed claim and any future brand-trust event work — both honest predictions awaiting the calibration paper.

### What we do not yet have.

*Three known gaps. Naming them here beats discovering them in a buyer meeting.* ←

> Ground-truth comparison against the official WoTC survey result is a forward promise — that survey hasn't published. The channel-culture inferences are derived from one thread per channel on this UA, not from longitudinal per-channel corpora. The brand-trust-event schema referenced in Section 4 sits in the production roadmap; it has not yet been calibrated against an archived corpus. None of these gaps invalidates the findings above; they bound them.

### Calibration plan.

*Three commitments with dates: weekly re-runs, a calibration paper after WoTC publishes, and a forum-culture corpus to firm up the soft inferences.*

> Three commitments. (1) Re-run weekly through 2026-05-14 (survey close) and publish the T+11 / T+18 / T+21 fingerprint trajectory — the trajectory itself is a stability check on the methodology. (2) Calibration paper when WoTC's official survey publishes, comparing our directional verdicts to the official result, naming agreements and divergences. (3) Build the 10-thread per-channel comparative corpus to formalize the channel-culture claims — straightforward backfill work, schedulable as Q3 2026.

---

## Notes on tone calibration (for review)

- **~half dry, half pithier**, per Phil's brief. The dry ones lean explanatory ("The pipeline stays the same. The tags swap..."). The pithier ones use a metaphor or one-shot hook ("front-row chorus / balcony critics," "one beep," "the label landed differently than the content").
- **Loglines flagged with `←`** are ones I'd most want a second opinion on. They land but feel like the place where Perplexity or Gemini might find a sharper turn of phrase.
- **No logline goes to a callout, caption, or table** — only the bold-lead-in beat paragraphs. Pattern but not slavish, per Phil.
- **Coverage:** 28 paragraphs across sections 1, 3, 3.5, 4, 5, 6. Section 7 (closing) deliberately gets none — the closing reads tighter without explainers.
