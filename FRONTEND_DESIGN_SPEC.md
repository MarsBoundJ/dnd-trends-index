# Frontend Design Specification — Arcane Analytics

**Project:** dnd-trends-index frontend (a.k.a. "Arcane Analytics")
**Status:** Design complete, build not yet started
**Author:** Yorri + Claude (collaborative design session, April 14, 2026)
**Audience for this doc:** Future Claude sessions, future collaborators, future-Yorri

This document is the source of truth for the frontend we're about to build. It captures *what* we're building, *why* we made each decision, and *how* the pieces fit together. If anything in this doc conflicts with a newer decision, update the doc rather than diverging silently.

---

## 1. Vision

Build a **"Fantasy Bloomberg Terminal"** for Hasbro / WotC stakeholders: a premium market-intelligence web app that turns the dnd-trends BigQuery pipeline (13 single-stream analytics views + 10 composite views) into a Swiss-army-knife decision-support tool.

The site must work for three audiences simultaneously without knowing which will arrive first:

- **Executives** — need one-glance decisions and shareable briefs
- **Game designers** — need to explore mechanics friction, hype vs play, edition adoption
- **Marketers** — need blue-ocean gaps, creator economy, mainstream breakout signals

It must feel **premium and modern** (to satisfy Hasbro's corporate instincts) *and* **arcane and warm** (to resonate with WotC's creative culture). We thread this needle with a principle called **Chrome Corporate, Soul Fantasy**: structural elements stay precise and modern; atmospheric elements carry the arcane warmth.

---

## 2. The Core Mental Model — Cards as Gaming Accessories

**Every unit of information on the site is a card.** This is not a UI convention borrowed from Bootstrap — it's a deliberate mapping onto the mental model every D&D / MTG player already has.

| D&D / MTG Card | Our Card |
|---|---|
| Name + type + rarity symbol | Concept/view name + category + **confidence tier** |
| Mana cost / stats | Composite scores + bucket breakdown |
| Card art | Chart or visualization |
| Rules text | Data / leaderboard content |
| Flavor text | AI summary / Sage commentary |
| Hand of cards you're browsing | **Current lens** |
| Stowed in a Bag of Holding | **Saved clippings (cards + Sage answers)** |
| Collection binder | **Atlas** (site map) |

The Lens is a hand of cards you're currently browsing. The Bag of Holding is where you stow the cards you want to keep — a persistent stash that travels with you across sessions. The Atlas is your collection binder: a map of everything available, not what you've kept. The site is no longer a data dashboard with a fantasy skin — it's a fantasy object that happens to contain data.

**This metaphor informs every subsequent decision.** Confidence tiers map to rarity glows. Aceternity's glowing borders are "rare card shine." Bronze card borders mimic MTG frames. The "Stow in Bag of Holding" action is "save it for the road." Copy leans into it subtly: "Stowed — Paladin is in your bag."

---

## 3. Information Architecture

### 3.1 Landing — "State of the D&D Multiverse"

The landing page is a daily pulse, not a navigation menu. Users should get value in under 10 seconds even if they never tap anything.

**Elements:**

1. **Daily Brief hero card** — AI-generated "State of the D&D Multiverse" summary. Pulls from the latest `composite_concept_index` refresh. Three voices available (Strategist / Scholar / Storyteller — see §6).
2. **The Sage entry point** — prominent button invoking the AI guide. Offers text input, voice input, checklist flow, or "Surprise me."
3. **Lens chip row** — six filters that reshape the card grid (not separate pages).
4. **Atlas icon** — opens the site map.
5. **Bag of Holding icon** — persistent, badge count, bottom nav.
6. **Hero card** — top card from the active lens.
7. **Scrollable card column** — the rest of the active lens's cards.

### 3.2 Six Lenses (Primary Navigation)

Lenses are *filters*, not pages. Tapping a lens rearranges the same underlying data without a page navigation. Users never lose context.

```
Mobile (2x3 grid):              Desktop (1x6 row):
+---------+---------+---------+
|Overview |Marketing|Game Dev |
+---------+---------+---------+
|Creator  |Digital  |Deep Dive|
+---------+---------+---------+
```

| Lens | Composites surfaced | Audience |
|---|---|---|
| **Overview** | trend_score, cross_pollination_v2, Daily Brief | Everyone |
| **Marketing** | blue_ocean, nostalgia_novelty, mainstream_breakout, creator_economy | Brand/marketing leads |
| **Game Dev** | mechanics_friction, hype_vs_play, digital_vs_tabletop | Designers |
| **Creator** | creator_economy, cross_pollination_v2 | Creator-focused strategists |
| **Digital / BG3** | digital_vs_tabletop + BG3-origin concepts | Digital strategy leads |
| **Deep Dive** | All 10 composites + single-stream "Labs" | Analysts |

Users can **save custom lenses** via the Sage's checklist flow, which appear alongside the defaults.

### 3.3 Card Expansion Pattern

Tap any card → it animates to fill the screen (mobile) or enlarges in place with AI chat at bottom (desktop). A back button returns to the same scroll position. This mimics iOS App Library / Apple Music album cards.

### 3.4 Concept Detail Drawer

Every concept name on the site is tappable. Tapping opens a drawer (bottom sheet on mobile, side panel on desktop) showing:

- Concept header, category, tags
- All five bucket scores (curiosity / community / creator / ownership / commerce)
- Per-stream sparklines (Google Trends, Reddit, YouTube, Fandom, BGG, Roll20, etc.)
- Related cards from every lens
- Sage chat pre-loaded with the concept as context
- "Stow Concept in Bag of Holding" button (saves the entire concept as a composite item)

This is the "evidence view" that lets a skeptical exec verify claims. It was the single most important addition from Perplexity's proposal — we had composite-level cards but no clean drill-down path, and this fills that gap.

### 3.5 Bag of Holding

A persistent, session-spanning collection tool. Every card, every Sage answer, every concept drawer can be **stowed** in the Bag of Holding via a button in the card's bottom-right. The name is a deliberate callout to the iconic D&D item that famously holds more than its apparent volume — which is the literal behavior of a digital clip-collection that grows without capacity. The verb "Stow" replaces "Clip" throughout the copy.

**URL slug:** Shareable link URLs use `/collection/{id}`, not `/bag/{id}` — the URL stays semantic and neutral because shareable links land in exec emails. "Bag of Holding" is the product-chrome name; `/collection/` is the URL name. (Similar to how Apple Music uses `/library/` in URLs while the UI says "Library.")

**Mechanics:**

- Bag of Holding icon in bottom nav shows a badge count
- Anonymous users stow to `localStorage`; signed-in users stow to Firestore
- Inside the bag: stowed items in order, drag to reorder (dnd-kit)
- "Ask AI to organize this" button → the Sage groups items logically, adds transitions/headers
- Export: PDF (@react-pdf/renderer), shareable link, email, copy-to-clipboard
- **Confidence scores travel with stowed items** — the final export shows "Confidence: 88%" next to every claim, making briefs auditable
- Saved bags as history (named, reusable artifacts, not one-shot)
- Exit guard: "You have 6 items in your Bag of Holding. Save before leaving?"

**Three levels of persistence:**

1. **Lenses** — saved ways of viewing the site
2. **Bags of Holding** — saved collections of cards + AI insights
3. **Reports** — finalized, shareable artifacts exported from a Bag of Holding

### 3.6 The Atlas (Navigation Card)

A tappable card that expands into a full-screen site map. Organized into six sections (borrowed from Perplexity's proposed structure, but demoted from top nav to navigational-aid):

- **Home** — State of the D&D Multiverse
- **Trends** — concepts and themes moving now
- **Products & Opportunities** — blue ocean, crowdfunding, commercial angles
- **Digital & BG3** — cross-medium footprint
- **Deep Dives** — single-stream "Labs" for analysts
- **Methodology** — confidence scoring, pipeline status, caveats

The Atlas is glassmorphic (à la Demiplane's sidebars) and doubles as onboarding for first-time users.

### 3.7 Admin Section (Gated)

Same site, gated by Google Cloud IAP on `/admin/*` paths. Inside:

- **Harvesting Cockpit** — co-located launchers for Amazon, DMs Guild, DTRPG, Kickstarter (bookmarklet flow) alongside **Harvest Console cards** for scripted scrapes (BackerKit and future additions)
- **Pipeline status dashboards**
- **Data quality monitors**
- **Sage Q&A logs** (user research data)

**Magic moment:** when an admin harvests data, public site cards update in near-real-time via Firestore listeners. Non-admin users watching a card see a subtle shimmer. This is the "see the data come alive" feeling.

---

## 4. Visual Design System

### 4.1 Palette — "Obsidian & Ember"

Dark theme as default. Light theme is a future toggle, not a launch requirement.

| Role | Color | Hex | Why |
|---|---|---|---|
| Background base | Deep obsidian | `#0B0D12` | Not pure black; hint of blue, easier on eyes |
| Surface (card) | Slate onyx | `#141821` | Lifted from base |
| Surface (elevated) | Iron | `#1C2230` | Hover states, modals |
| Border | Brushed bronze | `#3A2E1F` | Subtle, warm, not gold-tacky |
| Border (active) | Ember | `#B8692A` | Glowing, torchlit |
| Primary text | Parchment | `#E8E3D5` | Warm off-white, not stark |
| Secondary text | Ash | `#8A8578` | Muted, readable |
| Data hot | Ember orange | `#E87722` | Evocation fire |
| Data cold | Arcane cyan | `#5FC9E7` | Frost / divination |
| Positive | Druid green | `#6BAA75` | Momentum up |
| Negative | Void purple | `#8B5CF6` | Necromancy, momentum down |
| Rarity: copper | Dull tarnished copper | `#8C6239` | Exploratory (0–69%) — handle with care |
| Rarity: silver | Muted grey | `#6B6B70` | Solid but cautious (70–79%) |
| Rarity: gold | Warm gold | `#D4A94A` | Confident (80–89%) |
| Rarity: platinum | Pale near-white | `#D1D5DB` | High confidence (90–94%) |
| Rarity: mithral | Saturated arcane-blue | `#7AB8E0` | Exceptional (95–99%) |

Warmth comes from ember, bronze, parchment, and gold — not from flames or textures.

### 4.2 Typography

Three fonts, each doing exactly one job:

- **Headers & concept names:** **Spectral** (Google Fonts). Designed for on-screen reading, warm, strong structure. Feels like WotC book design at 14px on a phone.
- **Body & UI labels:** **Inter** (Google Fonts). Boring choice, correct choice. Used by Stripe, Linear, Figma.
- **Numbers & data:** **JetBrains Mono** (Google Fonts). Tabular, crisp, monospaced. Makes data feel terminal-like in a premium way.

**Font choice is fixed across all personas.** Persona differentiation uses color accents, not fonts.

### 4.3 The Glow Budget

**Rule: pick a glow budget and stick to it.** If everything glows, nothing glows.

- **Default state:** No glow. Bronze borders. Calm.
- **Hover state:** Subtle ember glow pulse. Inviting.
- **Active / expanded card:** Stronger amber torchlight ring.
- **"Hot" cards** (big momentum in last 24h): Very slow, subtle pulse — the card is "alive."
- **Confidence tier glow:** Use the card's metal-tier color (copper / silver / gold / platinum / mithral — see §5.2 and §9.16) in the border on hover, in place of the generic ember pulse.
- **Everything else:** Stays still.

Glow is signal, not decoration.

### 4.4 Visual Unity — Strict Chrome, Loose Content

The biggest design risk: cards contain wildly different content (bar charts, scatter plots, leaderboards, text blocks, sparklines). Without discipline this becomes a yard sale.

**Strict (site-wide, non-negotiable):**

- One card container: same corner radius, border, shadow, header bar, padding
- One color palette
- One type scale (4-5 sizes max)
- One accent color for interactive elements
- One confidence pip (always visible, top-left of header) and one hover-state border glow (desktop only) — both keyed off the same metal tier, see §5.2 and §9.17
- One Stow button, one Explain button — identical position on every card

**Loose (content within the container):**

- Chart type varies by data
- Chart color within the palette varies by category
- Text density varies

As long as every card wears the same uniform, the content inside can be wildly different and it still feels like one product. This is how Bloomberg, Stripe, and Linear stay coherent.

### 4.5 Card Icon Taxonomy

Every card has **exactly two icon slots** in the top-right of the header. Non-negotiable, same position everywhere.

**Slot 1 — Card Type:**

- Graph / chart card
- Leaderboard card
- Article card
- AI summary card
- Matrix / scatter card

**Slot 2 — Lens Tag(s):**

- Game Dev
- Marketing
- Creator
- Digital
- Deep Dive
- (Overview gets no icon — it's the null state)

If a card belongs to multiple lenses, Slot 2 holds up to two icons or a `+N` indicator. **Tapping a lens icon on any card jumps to that lens.** Icons double as navigation.

**Icon style:** Heraldic guild marks. Small (~16px), bronze at rest, ember on hover. Custom SVG, not emoji. Think MTG set symbols on the bottom-right of a card — present, tasteful, ignorable when you're focused.

**Not an icon slot — the confidence pip:** The top-*left* of the card header holds the confidence pip (see §5.2 and §9.17). It is **not** a third icon slot. The "exactly two icon slots" rule above refers only to the card-type and lens-tag slots in the top-right. The pip is a separate header element — a small colored dot, not an icon — and does not count toward the two-slot budget. It also doubles as the tap/click target for the methodology popover §5.2 describes. Future sessions must not add a third icon slot on the grounds that "the header already has three things in it."

### 4.6 Tone of Voice

Premium dark fantasy risks going full *Doom* and losing warmth. The "childlike fun" ingredient lives in:

- **Copywriting** — The Sage has personality. Loading states say "Consulting the scrolls…" not "Loading." Errors say "The divination failed" not "Error 500."
- **Micro-animations** — The D20 loader. Cards lift on hover. Bag of Holding badge bounces when you stow something.
- **Easter eggs** — `/roll` in the Sage actually rolls a die. A tiny d6 in the corner cycles faces.

**Triangle: Premium chrome. Arcane soul. Warm voice.**

### 4.7 Effects (Finite List)

All from Aceternity UI unless noted:

- **Glowing Border** on hover (every card) + rarity tier glow
- **Meteors** — subtle background on the Daily Brief hero card only
- **Background Beams** — possibly the landing page, muted
- **Spotlight** — "State of the D&D Multiverse" header glow
- **D20 spinning loader** — custom SVG, replaces all default spinners
- **Card expansion** — Framer Motion (under Aceternity)
- **Bag of Holding stow micro-animation** — Framer Motion

**Rule: no effect appears on more than 20% of screen surface area on any page.**

---

## 5. Confidence Scoring (First-Class Feature)

Stakeholders must always know how sure the system is of any statement. This is a **structural discipline**, not decoration.

### 5.1 Two Layers

1. **Data confidence** (objective, computed)
   - How many streams fired for this concept?
   - How fresh is the underlying data?
   - What's the average stream-level confidence (HIGH/MED/LOW)?
   - Deterministic. No AI guesswork.

2. **AI grounding confidence** (for generated text)
   - How much of the claim is backed by retrievable data vs inference?
   - Computed per generation.

### 5.2 Display

- **Confidence is displayed in two complementary places** on every card, both keyed off the same metal tier (copper 0–69% / silver 70–79% / gold 80–89% / platinum 90–94% / mithral 95–99%, see §9.16):
  - **(1) A confidence pip** — a small (~6–8px) colored dot in the top-left of the card header, **always visible on both mobile and desktop**. This is the primary confidence signal. It carries the tier color at all times, is the tap/click target for the methodology popover below, and is deliberately compact so it doesn't fight §4.3's "calm bronze at rest" aesthetic. Not an icon slot — see the note at the end of §4.5.
  - **(2) A hover-state border glow (desktop only)** — on desktop, moving the cursor over the card transitions the resting bronze border to the same tier color. Acts as a *reward* for user attention, not the primary signal. Mobile has no equivalent because (a) there is no hover on touch and (b) the always-visible pip already carries the signal — mobile cards' borders stay bronze at all times. See §9.17 for the full pip-vs-border reasoning.
- Exact percentage number is secondary — hidden behind a tap/click on the pip
- Tap the pip → methodology popover: "Data: 85% · AI Grounding: 92% · Combined: 88%"
- Dedicated **Methodology page** in the Atlas explains the whole system

### 5.3 Discipline

**Don't let confidence become theater.** If every card shows 85-95%, the metric is meaningless. Low-signal concepts should genuinely score low and users should *see* them score low. That builds trust in the high scores.

Confidence scores **travel with stowed items** into reports. When a WotC exec hands a brief to their boss, every claim has its confidence attached.

---

## 6. The Sage — AI Layer

### 6.1 Identity

The Sage is a single AI persona with three voices. Not three separate AIs. Name decided: **"The Sage."**

### 6.2 Three Voices (Shared With Articles)

- **The Strategist** (Brief/Exec) — "The Paladin Opportunity" — short, decisive, recommends action. 150 words, bullets.
- **The Scholar** (Deep/Analyst) — "On the Rise of Oath of Vengeance: A Methodological Note" — analytical, cites evidence, acknowledges caveats.
- **The Storyteller** (Designer) — "Why Holy Warriors Are Having a Moment" — narrative, player-fantasy oriented, cultural.

Voice is a prompt parameter, not a separate model.

### 6.3 Context-Awareness

Every Sage invocation receives structured page context:

```
{
  page_type: 'home' | 'lens' | 'concept' | 'collection' | 'admin',
  current_lens: LensId,
  selected_concept: ConceptId | null,
  time_range: { start, end },
  backing_view: 'gold_views.composite_blue_ocean',
  loaded_data: <page JSON state>
}
```

Because Gemini 1.5 has a 2M-token context window, we pass the entire loaded page JSON invisibly. No complex RAG pipeline needed — the model figures out what's relevant.

### 6.4 Tool Calling (Prevents Hallucination)

The Sage has ~10-12 TypeScript-defined tools for live BigQuery queries:

- `getConceptHistory(conceptId, days)`
- `getTopMovers(lens, days)`
- `getBlueOceanList(category, limit)`
- `getCompositeByLens(lens)`
- `getConfidenceExplanation(cardId)`
- `searchConceptsByKeyword(query)`
- *(etc)*

When a user asks "how has Astarion trended this month," the Sage **does not guess** — it calls `getConceptHistory('astarion', 30)`, receives real data, and summarizes. This is how you prevent numerical hallucination.

### 6.5 Three Ways to Invoke

1. **"Explain this"** button on every card — AI gets the card's JSON as context
2. **Free chat** — bottom-sheet Sage opens, knows current page
3. **"Quick Brief"** shortcut — user describes what they need, Sage assembles a Bag of Holding automatically

### 6.6 Q&A Logging

Every question + context + answer → `sageLogs/{logId}` in Firestore. Two benefits:

- Free user research — see what stakeholders actually ask
- Confidence calibration data over time

### 6.7 AI Articles (Third Pillar)

Alongside data cards and chat, the Sage produces **pre-written short articles** (300-500 words, half a printed page). Articles are a card type. They appear inline in lenses.

- Scheduled Cloud Function runs daily, picks top-moving concepts, generates articles in three voices
- Stored in `gold_articles` BigQuery table
- **Because articles are cards, stowing them in the Bag of Holding produces instant publishable report content**
- This is the feature that makes the Swiss Army Knife actually cut: login → stow → export shareable brief in 2 minutes

---

## 7. Technology Stack

### 7.1 The Short Version

**Next.js 16 (App Router) + TypeScript + Tailwind CSS v4 + shadcn/ui + Tremor + Aceternity + Vercel AI SDK (Vertex AI) + Firestore + NextAuth + Cloud Run behind IAP.**

Everything stays in GCP. Nothing exotic. Each piece plays with the others.

### 7.2 Decisions With Reasoning

| Layer | Choice | Why |
|---|---|---|
| Framework | **Next.js 16 App Router** | Tremor and Aceternity are React; Next is the React default; Server Components + streaming SSR are perfect for the Sage; built-in font optimization; `proxy` (formerly `middleware`) handles IAP. Turbopack is the default bundler in v16 — we use it. |
| Language | **TypeScript** | Data-heavy app with large type surface (concept IDs, lenses, card types, tool schemas). Non-negotiable. |
| Styling | **Tailwind CSS v4** | Required by Aceternity and shadcn; design tokens live in `@theme` blocks inside `src/app/globals.css` (v4 moved theme config from JS to CSS); this is the single source of truth that enforces palette discipline |
| UI primitives | **shadcn/ui** | Components copied into repo (not a library); built on Radix (accessible); fully themeable; owns the source |
| Charts | **Tremor** | Dark-mode native, Tailwind-themable, dashboard-specific; Recharts under the hood for escape hatches |
| Flourishes | **Aceternity UI** | Glowing borders, meteors, spotlight; used sparingly per glow budget |
| Animations | **Framer Motion** | Already in tree (Aceternity depends on it); card expansion, Bag of Holding bounce |
| Server state | **TanStack Query v5** | Caching, deduplication, stale-while-revalidate for all Bouncer + BigQuery reads |
| Client state | **Zustand** | Bag of Holding, active lens, UI state. ~1kb. Persists to localStorage. |
| AI integration | **Vercel AI SDK + `@ai-sdk/google-vertex`** | `useChat` hook, streaming primitives, tool calling with Zod schemas, framework-agnostic |
| App database | **Firestore** | GCP-native; real-time listeners (critical for admin→public shimmer); generous free tier; schema-flexible |
| Auth (public) | **NextAuth.js v5 (Auth.js)** | Google sign-in + magic link; anonymous browsing allowed |
| Auth (admin) | **Google Cloud IAP** | Hard gate on `/admin/*` at Cloud Run level; free for <100 users |
| Hosting | **Cloud Run** | Same GCP project; scales to zero; IAP native; internal networking to BigQuery/Firestore |
| CI/CD | **Cloud Build → Artifact Registry → Cloud Run** | GCP standard pattern |
| Drag-and-drop | **dnd-kit** | Bag of Holding reordering; accessible, touch-friendly |
| PDF export | **@react-pdf/renderer** | Client-side PDF from React components; full control over styling |
| Markdown | **react-markdown + remark-gfm** | Sage responses + articles |
| Voice input | **Web Speech API** | Built into browsers, no deps |
| Utility icons | **Lucide React** | Chevrons, close, back |
| Heraldic icons | **Custom SVG components** | Six lens + five card-type glyphs, inlined as React |
| Forms | **React Hook Form + Zod** | Type-safe validation |
| Dates | **date-fns** | Lighter than moment |
| SSE streaming | **Next.js route handlers** | Built-in `ReadableStream` — no lib |

### 7.3 Explicitly Rejected

- Redux (Zustand covers it)
- Axios (TanStack + fetch)
- Vercel hosting (keep data in GCP)
- GraphQL / tRPC (REST via Bouncer is simpler)
- Prisma (on Firestore)
- Emotion / styled-components (Tailwind only)
- Separate Express server (Next route handlers)
- CMS (articles live in Firestore/BigQuery)
- Storybook, Jest, Turborepo (premature for v1)

### 7.4 Cost Estimate

At low traffic: ~$10-30/month total new costs. At heavy demo volume: ~$50-100/month.

- Cloud Run: $0-5 (scales to zero)
- Firestore: $0 (free tier)
- Vertex AI: $5-20 depending on usage
- IAP: $0 (<100 users)
- Cloud Build + Artifact Registry: ~$0.10

---

## 8. Build Order

Each step is a visible, demo-able product increment. No "dead month" of invisible work.

1. **Skeleton** — Next.js + Tailwind + shadcn + Obsidian & Ember theme + three fonts loaded
2. **CardChrome** — the universal card container, before anything goes inside cards
3. **One lens end-to-end** — Overview lens pulling real Bouncer data, rendering as Tremor charts inside CardChrome. No Sage, no Bag of Holding, no animations.
4. **Sage MVP** — one chat interface, contextual to current page, streaming from Vertex, no tools yet
5. **Bag of Holding MVP** — localStorage only, stow-and-view
6. **Confidence scoring + rarity glows** — first time it feels like the real product
7. **Sage tool calling** — Sage can query live data
8. **Concept drawer** — drill-down pattern
9. **Articles** — generator + display
10. **Atlas navigation**
11. **Auth + Firestore persistence** — Bags of Holding survive sessions
12. **Admin + IAP + Harvest Console**
13. **Aceternity flourishes** — glow, meteors, spotlight
14. **D20 loader + micro-interactions**
15. **Report export (PDF)**
16. **Polish pass** — copy, mobile QA, accessibility audit

---

## 9. Decisions Log — The Whys

This section captures non-obvious decisions and the reasoning, so future collaborators can judge edge cases.

### 9.1 Why lenses, not pages

**Decision:** Top navigation is six lenses (filters), not separate pages.

**Why:** Mobile-first. Page-based nav fragments a session, loses context, and requires navigation discipline users won't sustain. Lenses rearrange the same data canvas in place. Perplexity proposed a page-based structure; we demoted it to the Atlas (navigational aid) rather than primary nav.

### 9.2 Why "Chrome Corporate, Soul Fantasy"

**Decision:** Structural elements stay modern and precise; atmospheric elements carry arcane warmth.

**Why:** The project serves both Hasbro (corporate, conservative, financial) and WotC (creative, fantasy-comfortable). Going full dark fantasy alienates Hasbro; going full Bloomberg alienates WotC. The chrome/soul split threads the needle. Bonus: the soul can be *dialed down* (reduce glow, mute ember) for pure-Hasbro demos and the site still works.

### 9.3 Why the card metaphor is sacred

**Decision:** Cards are the core mental model, not just a UI pattern.

**Why:** Yorri made the observation mid-session that D&D / MTG players already understand "cards" — they collect them, sort them, build decks, read rarity symbols. Every piece of UI maps cleanly onto this: lens = hand you're currently browsing, Bag of Holding = stowed keepsakes, atlas = collection binder, confidence = rarity. This is the kind of metaphor that unifies a design. Never dilute it.

### 9.4 Why confidence scores are structural, not decorative

**Decision:** Confidence is a first-class feature with its own methodology page.

**Why:** AI products routinely ship with confident-sounding outputs that hallucinate. WotC stakeholders making real decisions need to know what's solid. Confidence travels with stowed items into exported briefs, so the analyst handing a report to their boss doesn't have to worry about defending fabricated claims.

**Discipline:** Don't let confidence become theater. If everything shows 85-95%, the metric is meaningless. Low-signal concepts must genuinely score low.

### 9.5 Why one Sage with three voices, not three AI personas

**Decision:** The Sage is a single AI with voice toggles, not three separate characters.

**Why:** Three separate personas fragment the mental model and confuse users about which to ask. A single Sage with switchable voices is simpler to understand, simpler to engineer (one system prompt parameter), and lets users recognize continuity across interactions. Gemini originally proposed three personas for articles; we absorbed the instinct as three voices of one Sage.

### 9.6 Why tool calling instead of raw prompting

**Decision:** The Sage calls typed tools for numerical queries instead of inferring numbers from context.

**Why:** Hallucination prevention. When a user asks "how much did Paladin rise this month," the Sage must not estimate — it must call `getConceptHistory`. This integrates with the confidence discipline: numerical claims from tools get high data confidence; inferences get lower AI-grounding confidence.

### 9.7 Why Firestore, not Cloud SQL

**Decision:** App state (Bags of Holding, Q&A logs, saved lenses) lives in Firestore.

**Why:** Real-time listeners are critical for the "admin harvest → public shimmer" feature. Schema flexibility helps during build. Generous free tier. Zero auth setup in GCP. Migration to Cloud SQL later is possible via the thin data layer if relational queries ever become necessary.

### 9.8 Why Cloud Run, not Vercel

**Decision:** Next.js deploys to Cloud Run, not Vercel.

**Why:** The entire backend stack is GCP. Vercel creates cross-cloud latency to BigQuery, cross-cloud billing, and integration friction with IAP. Cloud Run stays in-project, integrates natively with IAP, and handles auto-scaling identically.

### 9.9 Why pnpm over npm

**Decision:** Package manager is pnpm.

**Why:** Faster installs, stricter dependency hoisting (catches bugs earlier), disk-efficient via content-addressable store. This is the modern default. Installed via the official standalone script to user directory (no admin needed on Windows).

### 9.10 Why the Harvest Console is not a real bash terminal

**Decision:** BackerKit scrape is a styled "terminal card" with a one-tap Run button, not an actual bash shell.

**Why:** Security (no arbitrary code execution even for admins), portability (Cloud Functions don't support stateful sessions), simplicity (a button + SSE stream is enough). If genuine ad-hoc shell access is ever needed, Google Cloud Shell is the right tool, opened in a new tab.

### 9.11 Why six lenses, not five

**Decision:** Digital / BG3 earned its own lens.

**Why:** Hasbro and WotC are moving strongly toward digital based on community patterns. Excluding BG3 from top-level nav would be analytically dishonest. Six lenses still fit the mobile 2x3 grid cleanly.

### 9.12 Why two icon slots, not more

**Decision:** Each card has exactly two icon slots (card type + lens tags).

**Why:** Legibility discipline. Two slots = glance-readable. Three slots = squint-readable. Four slots = ignored. Confidence doesn't need an icon (it's the border glow). Date doesn't need an icon (newest-first is default). Source doesn't need an icon (card name says it).

### 9.13 Why Next.js 16, not 15 (reconciled during Step 1)

**Decision:** Scaffold on Next.js 16 — the current latest — rather than downgrading to Next.js 15 as the original §7 draft specified.

**Why:** This spec was written on April 14, 2026. Next.js 16 landed within 24 hours and is now what `create-next-app@latest` installs. The *intent* of §7.2's framework row — "latest App Router with Server Components, built-in font optimization, and a middleware layer that can handle IAP" — is fully satisfied by v16. Downgrading to v15 would mean drifting from the ecosystem default on day 1 and paying an upgrade cost within months for zero design benefit.

Net v16 changes relevant to our build order:

- **Turbopack is the default** for both `next dev` and `next build`. We opt in by doing nothing. (If we ever need Webpack, `next build --webpack` is the escape hatch.)
- **`middleware` → `proxy`.** The file and the export are renamed. Step 12 (Admin + IAP) uses `proxy.ts`, not `middleware.ts`.
- **Async Request APIs are fully async** (`params`, `searchParams`, `headers()`, `cookies()`). Synchronous access is removed. Affects Steps 3+ when we fetch data.
- **`next lint` is removed.** ESLint runs directly (already configured via `eslint.config.mjs`).
- **Async `id`/`params` in `icon` and `open-graph` image generators** — affects Step 16 metadata polish at most.

None of these affect Step 1 (skeleton). All are documented in `arcane/node_modules/next/dist/docs/01-app/02-guides/upgrading/version-16.md` if we need to reread them later.

### 9.14 Why `@theme` in CSS, not `tailwind.config.ts` (reconciled during Step 1)

**Decision:** Obsidian & Ember palette tokens live in `src/app/globals.css` under a `@theme inline { ... }` block, not in a `tailwind.config.ts` JS file.

**Why:** Tailwind v4 deliberately removed `tailwind.config.ts` as the theme location and moved it into CSS via the `@theme` directive. Since the spec mandates Tailwind v4 (for Aceternity and shadcn compatibility), there is no `tailwind.config.ts` file in a v4 scaffold — the choice was already made upstream. The §7.2 row was edited to match.

The discipline the spec cares about — "one source of truth for tokens that enforces palette discipline" — is unchanged. It just lives in CSS now, which is arguably *better* suited to design tokens (closer to the rendered output, no JS round-trip).

### 9.15 Why "Bag of Holding," not "Briefcase" (renamed between Step 1 and Step 2)

**Decision:** The persistent clippings feature is named the **Bag of Holding**, the action verb is **Stow**, and the URL slug is `/collection/{id}`.

**Why:** The original name "Briefcase" was a holdover from the Bloomberg Terminal half of the chrome/soul split. It worked as corporate chrome but contradicted the fantasy-soul half and fought the card metaphor — a briefcase holds papers, not cards. "Bag of Holding" is an iconic D&D item that famously holds more than its apparent volume, which is the literal behavior of a digital clip-collection with no capacity. It makes the persistent stash feel native to the world the rest of the site lives in, and it makes Yorri smile — which is a decent proxy for whether it'll make exec users smile too.

**Why the URL stays `/collection/`:** Shareable link URLs land in executive emails and get pasted into Slack. A URL like `/bag/abc123` reads as cute-for-cute's-sake out of context; `/collection/abc123` reads as neutral and professional. "Bag of Holding" is the product-chrome name inside the app; `/collection/` is the URL name. (Apple Music does the same thing: UI says "Library," URL says `/library/`.) This also means the `page_type` enum value in Sage context is `'collection'`, not `'bag'` — the internal data model matches the URL, not the chrome.

**Why "Stow":** Matches the metaphor (you *stow* gear in a bag of holding; you don't *clip* things into it), one syllable, unambiguous verb, no namespace collision with other UI actions.

**Scope of the rename:** Every UI surface, copy string, component name, state slice, and spec reference. Internal enum values and URL slugs use `collection`, not `bag`. Done as a standalone PR stacked on top of Step 1's skeleton PR, before Step 2 (CardChrome) begins, so no Step 2 code is ever written under the old vocabulary.

### 9.16 Why metals, not MtG rarity (reconciled at start of Step 2)

**Decision:** The confidence tier ladder uses a **metal hierarchy** — **copper** (exploratory floor) / **silver** / **gold** / **platinum** / **mithral** — and **not** the MtG common / uncommon / rare / mythic labels the original §4.1 draft specified. Ranges: copper `0–69%`, silver `70–79%`, gold `80–89%`, platinum `90–94%`, mithral `95–99%`. The bottom four tiers (copper through platinum) are the canonical D&D currency ladder (cp < sp < gp < pp); mithral sits above platinum as the top rung, preserving the D&D-native vocabulary at both ends of the scale.

**Why not MtG rarity:** MtG rarity is a *scarcity* metaphor — it describes how often you pull a card from a booster pack. Our confidence score is a *trust* metaphor — it describes how much a stakeholder should rely on the claim. Forcing a scarcity ladder onto a trust axis breaks both sides: a "common" card reads as worthless both to the exec ("only 45% sure? no thanks") and to the D&D player ("oh, junk from the bulk box"). The metaphor ends up fighting itself — precisely the kind of metaphor cosplay §9.3 warns against.

**Why metals work:** Metals describe *material value*, which *is* a trust axis — everyone intuits gold > silver at a glance, no lore required. And the specific metal choices lean on two different canons at once: the top four tiers (silver / gold / platinum / mithral) preserve the arcane-soul half of the chrome/soul split the same way "Bag of Holding" does in §9.15 — "mithral chain shirt" is D&D vocabulary, not a Monopoly top rung — while the bottom four tiers (copper / silver / gold / platinum) *are* the standard D&D currency ladder, which is the kind of detail every player who has ever counted out loot after a dungeon run will recognize instantly. The ladder works at both ends: Hasbro execs see a gold / platinum hierarchy they already understand, D&D players see a copper / silver / gold / platinum coin purse they already understand, and mithral is the shared bonus that rewards attention without punishing the absence of it.

**Why the ranges start at 70, not 50:** The scoring formula (a blend of data-reliability and AI-grounding confidence — methodology TBD) is expected to cluster in the 80–89 band, with 70–79 as the soft floor for "published" cards. Anything below 70 is unusual enough to deserve a distinct visual treatment (copper), not a subdivision of "acceptable." Stakeholders at Hasbro / WotC are unlikely to act on anything below ~85% in practice, so the ladder is tuned to the top half of the range where the real decisions get made.

**Why a floor tier at all, not just four metals:** Occasionally a low-signal claim is still worth surfacing because the reward justifies the risk. Those cards need somewhere to go that isn't "round them up to silver" (that would be the exact kind of confidence theater §5.3 warns against) and isn't "hide them" (dishonest). Copper is the explicit exploratory floor: visually warmer and duller than silver, with tooltip copy that flags it as low-signal. Five tiers in the vocabulary, only four in the normal distribution.

**On the floor tier name — copper, not lead, not iron:** Three names got considered. *Iron* is ruled out permanently because `--color-iron` is already the elevated-surface token (`#1C2230` — hover states and modals). Reusing "iron" for a rarity tier would create a token-vocabulary collision — `border-iron` cannot mean two things. *Lead* was the first proposed alternative (D&D-adjacent via spell components, with a natural "handle with care" subtext from literal toxicity), but it leaned darker in tone than the site's voice calls for and broke the symmetry with the top rungs of the ladder. *Copper* won because it completes the D&D currency ladder at the bottom end — cp is the lowest coin, sp / gp / pp are the next three rungs, mithral is the one that goes above the standard ladder. That's a single coherent metaphor end-to-end instead of "D&D spell components on the bottom, D&D currency in the middle, D&D material lore at the top."

**The ladder is NOT architected around a predicted distribution.** Yorri's intuition is that the bell curve will sit in gold (80–89%). That's plausible, but it's a guess until the scoring formula exists. If the formula turns out harsher than expected and the bulk lands in silver instead of gold, the fix is to the formula, not the ladder. The ladder must survive whatever the formula produces.

**Palette implications** (applied to §4.1):

- **Copper** `#8C6239` — new. Dull tarnished copper penny, deliberately brown-leaning and mid-luminance. *Not* a bright copper-pipe orange (which would collide with `ember` `#B8692A` — the active-hover glow — and make the exploratory floor read like an active-state card, which is semantically backwards). *Not* a very dark brown (which would collide with `bronze` `#3A2E1F` — the resting border). The dull-tarnished read threads both needles. If it lands wrong in the `/test-card-chrome` harness, the fix is to the hex, not the name.
- **Silver** `#6B6B70` — inherits the old `rarity-common` hex. Name changed, hex unchanged.
- **Gold** `#D4A94A` — inherits the old `rarity-mythic` hex. Name changed, hex unchanged.
- **Platinum** `#D1D5DB` — new. Pale cool near-white.
- **Mithral** `#7AB8E0` — new. Saturated arcane-blue with a silver undertone, deliberately distinct from both platinum above it and the `arcane` data-accent (`#5FC9E7`) which keeps its data-hot/cold role unchanged. If mithral and silver read too similarly in the `/test-card-chrome` harness, mithral shifts bluer (not silver shifts — silver's muted grey is correct).

**Freed-up palette slots:** The old `rarity-uncommon` (druid green) and `rarity-rare` (arcane cyan) double-duty is retired. Druid green stays in the palette as the `druid` data-accent (positive momentum); arcane cyan stays as the `arcane` data-accent (cold data). The double-duty was cute but confusing — "is this card uncommon or is it showing positive momentum?" Decoupling them clarifies the palette.

**A note on feasibility as a third confidence axis:** During this decision Yorri flagged that AI recommendations carry a *feasibility* dimension ("can Hasbro actually act on this in the real world without blowing up their roadmap?") that is genuinely different from §5.1's two layers (data confidence + AI grounding). Feasibility is about the *recommendation*, not the *claim*. This is **not** in v1 — §5 stays at two layers for now — but is banked for a later conversation once the scoring formula is designed. Storage location TBD (either §11 open questions or `project_analytics_future_ideas.md`).

### 9.17 Why a confidence pip alongside the hover border glow (reconciled at start of Step 2)

**Decision:** Every card displays its confidence tier in two complementary places: an always-visible **confidence pip** in the top-left of the card header, and a **desktop-only hover-state border glow** that transitions the resting bronze border to the card's tier color. The pip is always on both platforms; the border glow is desktop-only. The pip also serves as the tap/click target for the methodology popover §5.2 describes. §4.5's "exactly two icon slots" rule is unchanged — the pip is a separate header element in the opposite corner, not a third icon slot.

**Why not a border glow alone:** Hover is a desktop-only concept. On mobile there is no cursor, so a hover-only confidence glow means mobile users never see the confidence signal at all. That breaks §5's core rule — "stakeholders must always know how sure the system is of any statement" — on the platform that will serve most casual viewers. Any confidence-display scheme that hides the signal on mobile is dead on arrival.

**Why not an always-on colored border on both platforms** *(Option Y in the Step-2 discussion)***:** Considered and rejected. It would fight §4.3's "calm bronze at rest" aesthetic on desktop — every card would be visibly colored at all times, turning the page into a loud color grid where nothing stands out. §4.3 explicitly says "if everything glows, nothing glows," and an always-on tier-colored border is effectively always-on glow. It also wastes the glow budget on basic legibility instead of reserving it for signal.

**Why not a platform-split border — mobile always-colored, desktop hover-revealed** *(Option X in the Step-2 discussion)***:** Also considered and rejected. It makes the site's behavior visibly different on different devices, which creates muscle-memory inconsistency for users who move between phone and laptop — the same card looks like a different thing depending on which device you picked it up on. The pip-based solution behaves the same way on both platforms, which matters more than cleverness about hover affordances.

**Why the pip wins** *(Option Z)***:** It is always visible on both platforms (mobile gets the confidence signal for free), small enough not to fight §4.3's calm aesthetic (it's a dot, not a border), doubles as the tap/click target that §5.2 already needed somewhere for the methodology popover, and leaves the desktop hover glow free to be a *reward* for attention rather than a *requirement* for basic legibility. Desktop users still get a hover effect — bronze-to-tier border transition — but it's an "I'm engaging with this card" moment, not the primary confidence signal. Mobile users never feel shortchanged because they already have the signal.

**Positioning:** Top-left of the card header, mirroring the two icon slots in the top-right. Creates a clean header symmetry: `[pip] [title / subtitle] [icon slot 1 · icon slot 2]`. Approximate size 6–8px; exact pixel locked during the CardChrome build in `/test-card-chrome`.

**Step-by-step implementation split:** Three build steps touch the confidence visuals, each with a distinct responsibility.

- **Step 2 (CardChrome) — visual contract.** The pip renders in its correct slot, colored by the card's metal tier, positioned consistently, wrapped in a shadcn `Tooltip` primitive that shows the raw confidence percentage on hover (desktop) or long-press (mobile). The hover-state border glow is a simple `border-color` transition from `bronze` to `rarity-{tier}` — no halo, no `box-shadow` spread, no animation beyond Tailwind's default transition. Dummy confidence values drive the tier calculation (no real scoring formula yet).
- **Step 6 (Confidence scoring + rarity glows) — semantic wiring.** Real confidence values flow from the backend, the full methodology popover replaces the tooltip, and `confidenceToTier()` becomes the single source of truth used everywhere. Border glow and pip still render as they did in Step 2 — this step is about *what's behind* the visuals, not the visuals themselves.
- **Step 13 (Aceternity flourishes) — visual flourish upgrade.** The simple border-color transition from Step 2 is replaced with a real Aceternity glowing-halo effect (colored `box-shadow` spread, possibly animated). The pip's visual stays as-is. This is the one place the glow budget §4.3 describes actually becomes visible.

Step 2 locks the contract. Step 6 delivers the interaction and real data. Step 13 delivers the flourish. None of the three steps can rip out work from the others.

---

## 10. What's Archived for Later (Not Dead)

These are documented in `project_analytics_future_ideas.md` and elsewhere:

- Clustering / themes-based browsing (requires new analytics work)
- Seasonality and lifecycle tags
- Automated weekly article generator (exists in some form; wire in once confirmed)
- Dedicated "Library Health" sub-view (blocked by concept-name matching gap)
- Correlation clusters, 18-month leading indicator (from Perplexity's analytics ideas)
- Light-theme toggle (v2)
- Collaborative Bags of Holding (multi-user editing)
- Mobile app (PWA is enough for v1)

---

## 11. Open Questions

None at time of writing. Every design decision has been made. Ready to begin Step 1 of the build order when Yorri is ready.

---

## 12. Source Conversations

This spec is the output of a long design session on April 14, 2026 between Yorri and Claude. Key inputs that shaped it:

- Gemini's suggestions (UI inspiration sites, Modern Dark Fantasy aesthetic, three-persona idea, Vertex AI integration plan, Tremor + Aceternity recommendation)
- Perplexity's suggestions (six-section IA, concept detail drawer, per-page AI context, Q&A logging, voice toggles)
- Inspiration sites: untapped.gg (liked), raider.io (rejected: too busy), steamdb.info (data density), app.demiplane.com (glassmorphic), foundryvtt.com/kb (liked most — tasteful fantasy)
- Yorri's card-as-gaming-accessory insight (session-defining reframe)
- Yorri's Digital/BG3 strategic emphasis
- Yorri's confidence-score requirement (hallucination defense)
- Yorri's Bag of Holding workflow vision (the Swiss Army Knife that actually cuts)

---

**End of spec.**
