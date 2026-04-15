# Frontend Build Status

**Last updated:** 2026-04-15
**Current phase:** Step 1 complete — skeleton scaffolded and verified
**Next step:** Step 2 of 16 — CardChrome (universal card container)

---

## Where We Are

Design phase is **complete**. See `FRONTEND_DESIGN_SPEC.md` for the full spec.

Step 1 (Skeleton) is **complete** as of 2026-04-15. The Next.js 16 app lives at `arcane/` in the repo root. The Obsidian & Ember palette and the three fonts (Spectral / Inter / JetBrains Mono) are wired into `src/app/globals.css` via Tailwind v4's `@theme` directive and `next/font/google` respectively. The default home page has been replaced by a token-verification harness at `/` that renders all 15 palette swatches, a sample paragraph in each font, and a preview of the card uniform that Step 2 will formalize.

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

- [x] **1. Skeleton** — Next.js 16 App Router + TypeScript + Tailwind v4 + Obsidian & Ember palette as design tokens in `@theme` + Spectral/Inter/JetBrains Mono loaded via `next/font/google`. Verified with a dev-server render of a swatch/font harness page. **shadcn/ui init was deferred to Step 2** — there are no shadcn components on the page yet, so initialising the library now would add config without consumers. We'll run `pnpm dlx shadcn@latest init` at the top of Step 2 when CardChrome needs its first primitives.
- [ ] **2. CardChrome** — Universal card container component. One file. Every card type will import this. Sets the visual contract: border, padding, corner radius, shadow, header bar, two icon slots, confidence glow ring, Clip button, Explain button. **Starts with** `pnpm dlx shadcn@latest init` and adding the first shadcn primitives CardChrome needs (likely Card, Button, Tooltip).
- [ ] **3. One lens end-to-end** — Overview lens pulling real data from Bouncer, rendering as Tremor charts inside CardChrome. No Sage, no briefcase, no animations yet. Data flowing from BigQuery → screen.
- [ ] **4. Sage MVP** — Single chat interface (Vercel AI SDK `useChat` hook), contextual to current page, streaming from Vertex AI Gemini 1.5, no tools yet.
- [ ] **5. Briefcase MVP** — localStorage only (no Firestore yet), clip-and-view, no export yet.
- [ ] **6. Confidence scoring + rarity glows** — First time it feels like the real product.
- [ ] **7. Sage tool calling** — Define ~10 tools as TypeScript functions with Zod schemas. Sage can query live BigQuery data without hallucinating numbers.
- [ ] **8. Concept detail drawer** — Tap any concept name → drawer opens with per-stream sparklines, bucket scores, related cards, Sage pre-loaded.
- [ ] **9. Articles** — Scheduled Cloud Function generates articles in three voices, stored in `gold_articles`, displayed as card type.
- [ ] **10. Atlas navigation** — Full site map card, glassmorphic, expands to full screen on mobile / sidebar on desktop.
- [ ] **11. Auth + Firestore persistence** — NextAuth with Google + magic link. Briefcases migrate from localStorage to Firestore on sign-in. Saved lenses persist.
- [ ] **12. Admin + IAP + Harvest Console** — `/admin/*` gated by Google Cloud IAP via a Next 16 `proxy.ts` (formerly `middleware.ts`). Harvesting Cockpit with bookmarklet launchers + BackerKit Harvest Console (styled terminal card with Run button).
- [ ] **13. Aceternity flourishes** — Glowing borders on hover, Meteors on Daily Brief hero, Spotlight on main header.
- [ ] **14. D20 spinning loader** — Custom SVG, replaces all default spinners.
- [ ] **15. Report export (PDF)** — `@react-pdf/renderer` export of briefcases with confidence scores baked in.
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
- All 13 unique Obsidian & Ember hex values present in the compiled Tailwind CSS bundle (13 uniques for 15 tokens because `rarity-uncommon` shares `#6baa75` with `druid`, and `rarity-rare` shares `#5fc9e7` with `arcane` — intentional per §4.1)
- Turbopack dev server ready in 3.4s, cold page compile 6.9s

---

## Notes For Next Session (Starting Step 2)

1. `cd arcane/` and run `pnpm dlx shadcn@latest init`. Default styling = "Default", base color = "Slate" (the component primitives will inherit our `@theme` tokens, so shadcn's base color only controls the initial CSS variables shadcn ships — we override them via globals.css).
2. Add the first shadcn primitives CardChrome needs: `pnpm dlx shadcn@latest add card button tooltip`.
3. Build `src/components/card-chrome.tsx` as the universal card container. Props: `children`, `title`, `subtitle`, `lens`, `cardType`, `confidence` (0-100 → rarity tier), `onClip`, `onExplain`. Render: bronze resting border, two icon slots top-right (empty placeholders for now — real icons wait for Step 13), confidence → `border-rarity-{tier}` on hover, Clip and Explain buttons at bottom.
4. Replace the current swatch harness at `/` with a `/test-card-chrome` page that shows CardChrome with ~5 different dummy content types to prove the container is truly universal.
5. Do not wire any real data (that's Step 3). Do not add Aceternity glowing-border effects (that's Step 13). CardChrome should look calm and correct in its default state.
6. When Step 2 is done, update this file and the memory index again.

---

## Deferred Questions (Resolved At Build-Start On 2026-04-15)

- **Repo decision:** ✓ New subdirectory `arcane/` inside `dnd-trends`. Legacy `frontend/` untouched.
- **Environment variables:** ✓ Vertex AI project = `dnd-trends-index`, Firestore project = `dnd-trends-index`, Bouncer API = `https://us-central1-dnd-trends-index.cloudfunctions.net/bouncer-api` (verified 200 OK). Not yet wired into `.env.local` — Step 3 is the first time we need them.
- **Deployment target:** ✓ New Cloud Run service `arcane-analytics`. Not yet created — Step 1 is dev-only. Deployment happens first during Step 3 (or later if we want to defer until there's a meaningful page to deploy).

---

**Step 1 complete. Waiting on confirmation to proceed to Step 2.**
