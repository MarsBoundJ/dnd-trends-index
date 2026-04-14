# Frontend Build Status

**Last updated:** 2026-04-14
**Current phase:** Design complete, build not yet started
**Next step:** Step 1 of 16 — Skeleton (Next.js + Tailwind + shadcn + Obsidian & Ember theme + fonts)

---

## Where We Are

Design phase is **complete**. See `FRONTEND_DESIGN_SPEC.md` for the full spec.

Environment is **ready**:

- Node.js v24.12.0
- npm 11.6.2
- pnpm 10.33.0
- Corepack 0.34.5
- Git configured

Backend is **live** (no frontend changes needed):

- 13 single-stream analytics views deployed
- 10 composite analytics views deployed
- `composite_concept_index` materialized
- Bouncer API live
- Vertex AI available

---

## Build Order (16 Steps)

- [ ] **1. Skeleton** — Next.js 15 App Router + TypeScript + Tailwind v4 + shadcn/ui init + Obsidian & Ember palette as design tokens + Spectral/Inter/JetBrains Mono loaded via `next/font/google`. Verify dev server renders a test page with the palette and fonts.
- [ ] **2. CardChrome** — Universal card container component. One file. Every card type will import this. Sets the visual contract: border, padding, corner radius, shadow, header bar, two icon slots, confidence glow ring, Clip button, Explain button.
- [ ] **3. One lens end-to-end** — Overview lens pulling real data from Bouncer, rendering as Tremor charts inside CardChrome. No Sage, no briefcase, no animations yet. Data flowing from BigQuery → screen.
- [ ] **4. Sage MVP** — Single chat interface (Vercel AI SDK `useChat` hook), contextual to current page, streaming from Vertex AI Gemini 1.5, no tools yet.
- [ ] **5. Briefcase MVP** — localStorage only (no Firestore yet), clip-and-view, no export yet.
- [ ] **6. Confidence scoring + rarity glows** — First time it feels like the real product.
- [ ] **7. Sage tool calling** — Define ~10 tools as TypeScript functions with Zod schemas. Sage can query live BigQuery data without hallucinating numbers.
- [ ] **8. Concept detail drawer** — Tap any concept name → drawer opens with per-stream sparklines, bucket scores, related cards, Sage pre-loaded.
- [ ] **9. Articles** — Scheduled Cloud Function generates articles in three voices, stored in `gold_articles`, displayed as card type.
- [ ] **10. Atlas navigation** — Full site map card, glassmorphic, expands to full screen on mobile / sidebar on desktop.
- [ ] **11. Auth + Firestore persistence** — NextAuth with Google + magic link. Briefcases migrate from localStorage to Firestore on sign-in. Saved lenses persist.
- [ ] **12. Admin + IAP + Harvest Console** — `/admin/*` gated by Google Cloud IAP. Harvesting Cockpit with bookmarklet launchers + BackerKit Harvest Console (styled terminal card with Run button).
- [ ] **13. Aceternity flourishes** — Glowing borders on hover, Meteors on Daily Brief hero, Spotlight on main header.
- [ ] **14. D20 spinning loader** — Custom SVG, replaces all default spinners.
- [ ] **15. Report export (PDF)** — `@react-pdf/renderer` export of briefcases with confidence scores baked in.
- [ ] **16. Polish pass** — Copy (Sage voice across loading/error/empty states), mobile QA, accessibility audit.

---

## Notes For Next Session

When Yorri comes back to start building:

1. Read `FRONTEND_DESIGN_SPEC.md` first. It's the source of truth.
2. This file tells you which step we're on.
3. Step 1 means: `cd` into a clean directory (NOT inside the existing repo — decide with Yorri whether frontend is a separate repo or a subdirectory of `dnd-trends`), run `pnpm create next-app@latest`, pick the TypeScript + Tailwind + App Router options, then install shadcn, configure the Obsidian & Ember palette from §4.1 of the spec, and load the three fonts.
4. **Do not skip ahead** — each step builds on the previous in a way that's demo-able. Step 3 should *look like the real product* even without AI or briefcase wired up.
5. If Yorri wants to change any design decision, update `FRONTEND_DESIGN_SPEC.md` first, then build.

---

## Deferred Questions (To Ask Yorri At Build-Start)

- **Repo decision:** Is the frontend a new subdirectory of `dnd-trends` (e.g., `/frontend-v2/`), a sibling directory, or a separate GitHub repo? The existing `frontend/` folder is the legacy HTML/CSS/JS version and should not be overwritten.
- **Environment variables:** Where are existing Bouncer API URLs, Vertex AI project ID, Firestore project ID documented?
- **Deployment target:** New Cloud Run service, or should this replace an existing one?

---

**When Yorri says "let's start building" — go to Step 1.**
