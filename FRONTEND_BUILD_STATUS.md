# Frontend Build Status

**Last updated:** 2026-04-15
**Current phase:** Step 6 complete (data layer) — Confidence scoring live, Popover wired
**Next step:** Step 6.5 — AI grounding confidence (two-pass check at generation time)

---

## Where We Are

Design phase is **complete**. See `FRONTEND_DESIGN_SPEC.md` for the full spec.

Step 1 (Skeleton) is **complete** as of 2026-04-15. The Next.js 16 app lives at `arcane/` in the repo root. The Obsidian & Ember palette and the three fonts (Spectral / Inter / JetBrains Mono) are wired into `src/app/globals.css` via Tailwind v4's `@theme` directive and `next/font/google` respectively. The token-verification harness now lives permanently at `/swatch` (moved in Step 2).

Step 2 (CardChrome) is **complete** as of 2026-04-15. shadcn/ui initialized, Card + Button + Tooltip primitives added, `src/components/card-chrome.tsx` built, and `/test-card-chrome` verification harness verified in browser. The confidence tier system was redesigned from MtG rarity to a D&D metal ladder (see §9.16–9.17 in FRONTEND_DESIGN_SPEC.md). The palette token count grew from 15 to 16 — the new copper/silver/gold/platinum/mithral tiers no longer double-duty with druid/arcane, so all 16 tokens are now unique hex values.

Environment:

- Node.js v24.12.0
- pnpm 10.33.0 (project manager)
- npm 11.6.2 (used as fallback for `create-next-app` — pnpm dlx hit `ERR_PNPM_NO_IMPORTER_MANIFEST_FOUND` for that specific invocation)

Backend is **live** (no frontend changes needed):

- 13 single-stream analytics views deployed
- 10 composite analytics views deployed
- `composite_concept_index` materialized
- Bouncer API live at `https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api` (returns JSON categories/items as of 2026-04-15, HTTP 200)
- Vertex AI available (project `dnd-trends-index`)
- Firestore available (project `dnd-trends-index`)

---

## Build Order (16 Steps)

- [x] **1. Skeleton** — Next.js 16 App Router + TypeScript + Tailwind v4 + Obsidian & Ember palette as design tokens in `@theme` + Spectral/Inter/JetBrains Mono loaded via `next/font/google`. Token harness lives at `/swatch`. shadcn/ui deferred to Step 2 as planned.
- [x] **2. CardChrome** — Universal card container (`src/components/card-chrome.tsx`). shadcn Card + Button + Tooltip primitives. Bronze resting border → confidence-tier hover border. Always-on confidence pip with tooltip (`{confidence}% · {tier}`). Two empty icon-slot placeholders (Step 13 gets heraldic SVGs). Stow + Explain buttons. Confidence tier system redesigned to D&D metal ladder: copper (0–69%) / silver (70–79%) / gold (80–89%) / platinum (90–94%) / mithral (95–99%). Verification harness at `/test-card-chrome` — all 7 visual checklist items confirmed in browser.
- [x] **3. One lens end-to-end** — Overview lens at `/overview` with real Bouncer data. Three CardChrome cards: Top Classes leaderboard, Category Heat Recharts bar chart, Top Opportunities leaderboard. Server Component with 1-hr ISR revalidation. All confidence props stubbed at 75 (silver) with STUB comment — real formula is Step 6. Recharts used instead of Tremor (Tremor v3 conflicts with Tailwind v4; Tremor v4 is early beta). Verified in browser: data flowing, chart labels correct, responsive layout.
- [x] **4. Sage MVP** — Streaming Gemini chat panel with page context, `useChat` hook, contextual Explain button wiring on every CardChrome.
- [x] **5. Bag of Holding MVP** — localStorage-backed bag store (Zustand), stow/unstow from any card, `/collection` page, stable selector for SSR hydration.
- [x] **6. Confidence scoring + rarity glows** (data layer) — `concept_confidence` BigQuery view (v1.0.1 formula), Bouncer `/confidence` endpoint (Option B wiring), `fetchConfidence()` + `cardConfidence()` helpers, Overview cards wired with real scores, methodology Popover replaces Tooltip on pip. See `CONFIDENCE_METHODOLOGY.md` for full formula rationale. AI grounding layer deferred to Step 6.5.
- [ ] **6.5. AI grounding confidence** — Two-pass grounding check at article/Sage generation time. Produces `ai_grounding_confidence`. Displayed confidence for AI cards = `min(data_confidence, ai_grounding_confidence)`. Data cards unaffected.
- [ ] **7. Sage tool calling** — Define ~10 tools as TypeScript functions with Zod schemas. Sage can query live BigQuery data without hallucinating numbers.
- [ ] **8. Concept detail drawer** — Tap any concept name → drawer opens with per-stream sparklines, bucket scores, related cards, Sage pre-loaded.
- [ ] **9. Articles** — Scheduled Cloud Function generates articles in three voices, stored in `gold_articles`, displayed as card type.
- [ ] **10. Atlas navigation** — Full site map card, glassmorphic, expands to full screen on mobile / sidebar on desktop.
- [ ] **11. Auth + Firestore persistence** — NextAuth with Google + magic link. Bags of Holding migrate from localStorage to Firestore on sign-in. Saved lenses persist.
- [ ] **12. Admin + IAP + Harvest Console** — `/admin/*` gated by Google Cloud IAP via a Next 16 `proxy.ts` (formerly `middleware.ts`). Harvesting Cockpit with bookmarklet launchers + BackerKit Harvest Console (styled terminal card with Run button).
- [ ] **13. Aceternity flourishes** — Glowing borders on hover, Meteors on Daily Brief hero, Spotlight on main header.
- [ ] **14. D20 spinning loader** — Custom SVG, replaces all default spinners.
- [ ] **15. Report export (PDF)** — `@react-pdf/renderer` export of Bags of Holding with confidence scores baked in.
- [ ] **16. Polish pass** — Copy (Sage voice across loading/error/empty states), mobile QA, accessibility audit.

---

## Step 1 Deviations From The Original Plan (All Reconciled)

Two things changed between the spec being written on 2026-04-14 and the scaffold running on 2026-04-15. Both are documented in `FRONTEND_DESIGN_SPEC.md` §9.13 and §9.14 respectively:

1. **Next.js 16, not 15.** `create-next-app@latest` installed Next 16.2.3 — Next 16 landed within a day of the spec being written. Rather than downgrading, we stayed on latest and updated §7 to match. Turbopack is the default bundler in v16; `middleware` → `proxy`; async Request APIs are now fully async.
2. **`@theme` in CSS, not `tailwind.config.ts`.** Tailwind v4 moved theme config out of JS and into CSS. The spec's §7.2 line about `tailwind.config.ts` was aspirational given v4 was already mandated; the `@theme` approach is the only option and is now what §7.2 reflects.

Neither changes any design decision — only two version-reconciliation notes.

---

## Step 1 Verification Evidence

- `HTTP 200` on `http://localhost:3000/`
- `<title>Arcane Analytics</title>` in rendered HTML
- `<html class="...spectral_variable inter_variable jetbrains_mono_variable">` — all three `next/font/google` variables attached
- All 13 unique Obsidian & Ember hex values present in the compiled Tailwind CSS bundle at Step 1 verification (the original 15 tokens had 2 shared hex values — `rarity-uncommon` = `druid`, `rarity-rare` = `arcane`). Step 2 replaced those 4 rarity tokens with 5 distinct metal-tier tokens (copper/silver/gold/platinum/mithral), bringing the total to 16 tokens / 16 unique hex values.
- Turbopack dev server ready in 3.4s, cold page compile 6.9s

---

## Notes For Next Session (Starting Step 3)

Goal: Overview lens pulling real data from Bouncer, rendered as Recharts charts inside CardChrome. No Sage, no Bag of Holding, no animations. Data flowing Bouncer → screen.

**Chart library decision (Step 3):** Using **Recharts directly** instead of Tremor. Tremor v3 (`@tremor/react`) was built for Tailwind CSS v3 and conflicts with our v4 `@theme` setup. Tremor v4 exists but is early beta. Recharts is what Tremor wraps anyway; the spec explicitly calls it the escape hatch.

**Confidence stub decision (Step 3):** Bouncer API `score` field (Google Trends 0–100) is NOT our confidence score — they measure different things (popularity vs. trust). All Step 3 cards are hardcoded to `confidence={75}` (silver) with a `// STUB` comment. Real confidence formula (data reliability + AI grounding per §5.1) lands in Step 6.

1. **Wire `.env.local`** — Create `arcane/.env.local` with:
   ```
   NEXT_PUBLIC_BOUNCER_API_URL=https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api
   ```
   Verified `HTTP 200`. The Bouncer root endpoint returns 18 categories; all paths (`/`, `/categories`, `/composite`, `/trend-score`) return the same dataset. No separate composite endpoint exposed.

2. **Install Recharts** — `pnpm --prefix arcane add recharts`. Ships its own TypeScript types.

3. **Build the Overview lens** — A `src/app/overview/page.tsx` that:
   - Fetches from Bouncer with `{ next: { revalidate: 3600 } }` (Next 16 does NOT cache fetch by default — must opt in)
   - Renders 3 CardChrome cards: top classes leaderboard, category heat bar chart, top opportunities
   - All `confidence={75}` (silver stub)
   - A simple `loading.tsx` skeleton is fine

4. **Update `src/app/page.tsx`** — Add a link to `/overview` from the landing stub, or redirect there directly.

5. **Do not add** Sage, Bag of Holding, Concept Detail Drawer, animations, or auth. Step 3 is purely data → screen.

6. **Deployment option** — Cloud Run service `arcane-analytics` hasn't been created yet. Step 3 is the first meaningful page to deploy, but deployment can be deferred to Step 4+ if the lens isn't production-worthy yet. Yorri decides.

7. **shadcn/ui workaround note** — `pnpm dlx shadcn@latest` fails on Windows/pnpm 10.33 (`ERR_PNPM_NO_IMPORTER_MANIFEST_FOUND`). Also `npx shadcn@latest init` non-interactive mode is broken in shadcn 4.2.0 (Nova/Vega prompt system). Workaround: add components manually via `npx shadcn@latest add <name> --yes` — this works fine. `components.json` and `src/lib/utils.ts` already exist from Step 2.

---

## Deferred Questions (Resolved At Build-Start On 2026-04-15)

- **Repo decision:** ✓ New subdirectory `arcane/` inside `dnd-trends`. Legacy `frontend/` untouched.
- **Environment variables:** ✓ Vertex AI project = `dnd-trends-index`, Firestore project = `dnd-trends-index`, Bouncer API = `https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api` (verified 200 OK). Not yet wired into `.env.local` — Step 3 is the first time we need them.
- **Deployment target:** ✓ New Cloud Run service `arcane-analytics`. Not yet created — Step 1 is dev-only. Deployment happens first during Step 3 (or later if we want to defer until there's a meaningful page to deploy).

---

**Step 6 (data layer) complete. Step 6.5 (AI grounding) is next.**

---

## Step 6 Verification Evidence

- `concept_confidence` view deployed to `gold_data.concept_confidence` (v1.0.1)
- Bouncer `/confidence?names=Paladin,Wizard,Sorcerer` returns correct JSON with `data_confidence`, `tier`, `explanation` payload
- Overview page at `/overview` shows per-card confidence (78% Silver for Top Classes, 68% Copper for Category Heat/Top Opportunities)
- Clicking the confidence pip opens a methodology Popover showing: score/tier headline, weakest-link concept name, binding constraint explanation, factor breakdown (streams, families, agreement, velocity), aggregate disclosure, and algo version
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — zero errors)
- ESLint clean (`pnpm lint` — zero errors)
- Distribution: 48% copper, 52% silver, 10 gold, 0 platinum, 0 mithral (see `CONFIDENCE_METHODOLOGY.md` section 5 for calibration rationale)

## Step 6 Files Changed

| File | Change |
|---|---|
| `gold_views/concept_confidence.sql` | NEW — the confidence formula, tier assignment, explanation payload |
| `gold_views/composite_concept_index.sql` | EDITED — added `*_avg_confidence` columns to feed the formula |
| `bouncer/main.py` | EDITED — added `/confidence` endpoint + router branch |
| `deploy_bouncer.py` | EDITED — fixed stale entry point (`get_daily_trends` -> `bouncer_api`) |
| `arcane/src/lib/bouncer.ts` | EDITED — `ConfidenceEntry`, `ConfidenceMap`, `fetchConfidence()`, `cardConfidence()` |
| `arcane/src/components/card-chrome.tsx` | EDITED — Popover replaces Tooltip, new explanation props, `bindingCopy` map |
| `arcane/src/components/ui/popover.tsx` | NEW — shadcn Popover primitive |
| `arcane/src/app/overview/page.tsx` | EDITED — batch-fetch confidence, per-card min aggregation, removed STUB_CONFIDENCE |
| `CONFIDENCE_METHODOLOGY.md` | NEW — full formula design rationale (you are here) |
