# Frontend Build Status

**Last updated:** 2026-04-19
**Current phase:** Step 9.5 complete — Frame abstraction plumbing (Firestore config, TS loader, admin panel, journalist prompt injection)
**Next step:** Step 9.6 (The Chronicler + Track A + Flash length), then 9.7 (Gamer Gary + Player's-Eye), 9.8 (Hasbro-2026 frame + Track D), 9.9 (Universes Beyond Matrix), 9.10 (Industry Fundamentals + Track C), 9.11 (Reports format). Step 12.5 (legacy admin port) and Step 13 (Aceternity) land after the 9.x series.

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
- [x] **6.5. AI grounding confidence** — Post-stream grounding check on Sage responses. Gemini fact-checks claims against pageContext, produces inline `(confidence%)[n]` footnote markers (max 5), footnotes section with per-claim source + explanation, and `ai_grounding_confidence` byline. Graceful fallback on error. See `CONFIDENCE_METHODOLOGY.md` §6.3.
- [x] **7. Sage tool calling** — 8 tools with Zod v4 schemas calling Bouncer API: getLeaderboard (10 sources), searchConcepts, getConceptConfidence, getMarketSummary, getTopMovers, getDmShortageIndex, getEditionMigration, getSystemHealth. Multi-step enabled (`stopWhen: stepCountIs(5)`). Compact ToolInvocationChip shows tool state in chat. Tool results fed to grounding check for accurate scoring.
- [x] **8. Concept detail drawer** — Tap any concept name → drawer slides in with cross-source rankings, confidence breakdown, Sage + Bag integration. Server-side aggregation via `/api/concept`. Responsive: bottom sheet on mobile, right panel on desktop. Three dismissal methods: backdrop click, X button, Escape key.
- [x] **9. Articles** — Scheduled Cloud Function generates articles from a 5-member Council of bylined writers (The Loremaster, The Bursar, The Quartermaster, The Weaver, The Architect), stored in `gold_data.daily_articles`, displayed as bylined card type. Split: **9a backend** (Council refactor + Freightos harvester + schema migration) and **9b frontend** (article cards + Sage Council-Chair framing). See `project_step_9_council.md` memory file and `docs/step-9-persona-study.md`.
- [x] **10. Atlas navigation** — Full site-map sheet, glassmorphic (`bg-iron/80 backdrop-blur-xl`), bottom-sheet on mobile / right sidebar on desktop. Site-wide `<SiteHeader />` lands as the host for the Atlas Compass trigger + wordmark. Eight tiles split into Available (Home, Trends, Articles, Bag of Holding) and Planned (Products & Opportunities, Digital & BG3, Deep Dives, Methodology); planned tiles render disabled with a tooltip. Bag of Holding uses a composite `BagOfHoldingSigil` (PackageOpen + Infinity charm). Onboarding auto-open deferred.
- [x] **11. Auth + Firestore persistence** — Auth.js v5 (NextAuth) with Google OAuth. Sign-in menu in SiteHeader (avatar + popover). JWT session strategy. Bag of Holding migrates localStorage → Firestore on sign-in via silent union-dedupe at `users/{uid}/bag/{itemId}`; subsequent stow/unstow mirror to Firestore through a module-scoped `BagSyncer`. Sign-out clears local cache to prevent cross-user leaks on shared browsers. **Step 11.5 (complete):** Resend magic-link email provider landed alongside Google — requires `@auth/firebase-adapter` (namespaced to `authjs_*` collections so it doesn't collide with `users/{uid}/bag/*`); `allowDangerousEmailAccountLinking: true` on Google so magic-link users can later sign in via Google with the same email. Edge-safe split between `auth.config.ts` (used by proxy) and `auth.ts` (full; has adapter + Resend). **Deferred:** saved-lenses persistence — revisit once the lens filter mechanic is formalized (tracked in `project_saved_lenses_backlog.md` memory).
- [x] **12. Admin + IAP + Harvest Console** — `/admin/*` gated by Next 16 `proxy.ts` (formerly `middleware.ts`) using Auth.js session email against an `ADMIN_EMAILS` allowlist; IAP deferred to the eventual Cloud Run deploy as a second layer in front of this gate. Harvesting Cockpit at `/admin/harvest` with three drag-install bookmarklets (Amazon, Kickstarter, shared DMsGuild/DTRPG). BackerKit Harvest Console at `/admin/backerkit` — fire-and-forget trigger, Firestore run log at `admin/runs/backerkit/{runId}`, terminal-styled card polls for status and surfaces the harvester's row count inline. Admin Atlas tile is conditional on `NEXT_PUBLIC_ADMIN_EMAILS`. **Deferred to Step 12.5:** legacy Library Clerk + Scrying Chamber feature port from the old HTML admin.
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

**Step 8 (Concept detail drawer) complete. Step 9a (Council backend) verified live on 2026-04-17. Step 9b (Article cards) complete on 2026-04-17. Step 10 (Atlas navigation) complete on 2026-04-18. Step 11 (Auth + Firestore persistence) complete on 2026-04-18. Step 12 (Admin + Harvesting Cockpit + BackerKit Console) complete on 2026-04-19. Step 11.5 (Resend magic-link) complete on 2026-04-19. Step 9.5 (Frame abstraction plumbing) complete on 2026-04-19 — see verification evidence below.**

---

## Step 9.5 Verification Evidence

- **Architecture locked.** Frames live as Firestore docs at `frames/{frameId}` rather than in code. Updating Hasbro's FY27 strategy (or pitching a different buyer — Paizo, Free League, an indie, a VC) is a one-doc edit instead of a release. Commercial rationale in `project_hasbro_pitch_problems_solutions.md` memory; technical roadmap for Steps 9.5→9.11 in `project_tracks_frames_roadmap.md` memory.
- **Active-frame pointer** uses a single `frames/_meta` doc (`{activeFrameId: "pure-data"}`) rather than a boolean on each frame. Single-writer semantics; atomic activation.
- **`arcane/src/lib/frames.ts`** — server-only Frame types + Zod schema + loaders (`getActiveFrameId`, `setActiveFrameId`, `getFrameById`, `getActiveFrame`, `listFrames`). TypeScript types inferred from the Zod schema so drift between runtime validation and compile-time types is impossible.
- **`setup_frames_collection.py`** — idempotent seed (`merge=True`) for the `pure-data` baseline frame. Only flips the `_meta` pointer if no active frame is already set. Windows cp1252 console can't render Unicode checkmarks, so output uses `[OK]` markers.
- **Firestore writes executed** (pause-and-confirm given): two docs at `frames/pure-data` + `frames/_meta`. Verified with a one-off Python script: `activeFrameId=pure-data`, `label='Pure Data (no corporate-strategy frame)'`, `tone={'deck_ready': 6, 'sharp': 1}`.
- **`gold_data.daily_articles` schema migrated** (pause-and-confirm given): `ALTER TABLE ... ADD COLUMN IF NOT EXISTS frame_id STRING`. Verified via `bq show --schema`. NULL for all historical rows; Step 9.8 populates on new Track D articles.
- **`cloud_functions/daily_journalist/council.py`** — Python mirror of the TS loader. `load_active_frame(firestore_client)` returns the active frame dict or `None` on any error (article generation must never fail on a frame hiccup). `build_prompt(member, context, frame=None)` signature extended with optional `frame` kwarg; existing callers get identical output.
- **Pure Data empty-worldview invariant verified.** Smoke-tested via `load_active_frame()` + `build_prompt(BURSAR, context, frame=pure_data_frame)`: no `INTERPRETIVE FRAME` section rendered. A synthetic Hasbro-shaped frame passed to the same call DOES render the `INTERPRETIVE FRAME` section with worldview summary, strategic priors, priority brands, and tariff risk-facts injected. Plumbing works end-to-end.
- **`/admin/frames` admin panel live.** Gated by existing proxy.ts (`/admin/:path*` + ADMIN_EMAILS allowlist). Lists frames, shows active-frame pointer panel, one-click Activate button per inactive tile (server action flips `frames/_meta`). Frame editing stays out of the UI by design — edit via setup scripts or Firestore console.
- **Admin landing** (`/admin`) adds a Frames tile alongside Harvesting Cockpit + BackerKit Console. Visible end-to-end on `http://localhost:3000/admin`.
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — exit 0).
- ESLint clean (pre-existing `concept-drawer.tsx` unused-import warning unchanged).
- **Behavior invariant (9.5's "plumbing only, no behavior change" guarantee):** with `pure-data` as the active frame, the daily_journalist Cloud Function generates the same prompts it generated yesterday. Deployment of the updated `council.py` is NOT done in 9.5 — the code sits in the repo and ships when Step 9.8 ingests the `hasbro-2026` frame and actually needs it.

## Step 9.5 Files Changed

| File | Change |
|---|---|
| `arcane/src/lib/frames.ts` | NEW — server-only Frame types, Zod schema, Firestore loaders, active-frame pointer helpers |
| `arcane/src/app/admin/frames/page.tsx` | NEW — read-only admin panel with one-click "Activate" server action |
| `arcane/src/app/admin/page.tsx` | EDITED — added Frames tile to the admin landing grid |
| `setup_frames_collection.py` | NEW — idempotent seed for the `pure-data` baseline frame + active pointer |
| `cloud_functions/daily_journalist/council.py` | EDITED — added `load_active_frame()` + extended `build_prompt()` with optional `frame` kwarg + `_render_frame_section()` helper |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 9.5 marked complete (this block) |

**GCP state changes (one-time):**
- Firestore: seeded `frames/pure-data` + `frames/_meta` via `setup_frames_collection.py`
- BigQuery: `ALTER TABLE gold_data.daily_articles ADD COLUMN IF NOT EXISTS frame_id STRING`

---

---

## Step 11.5 Verification Evidence

- **Resend account + API key.** Yorri signed up at resend.com, API key `re_...` dropped into `arcane/.env.local` as `AUTH_RESEND_KEY`. Sandbox `from: "onboarding@resend.dev"` delivers only to the account owner's own verified email (`halftonejones@gmail.com`) — fine for dev, lifted by verifying a custom domain later.
- **Split config.** `auth.config.ts` (new, edge-safe) holds only providers safe to instantiate without an adapter (Google) + session strategy + JWT/session callbacks. `auth.ts` (Node runtime) spreads `authConfig`, adds the Resend provider and `FirestoreAdapter`. `src/proxy.ts` re-runs `NextAuth(authConfig)` so the edge runtime doesn't pull in `firebase-admin` via the adapter. This pattern is the Auth.js v5 docs' recommended shape for "middleware + database adapter."
- **Firebase adapter** (`@auth/firebase-adapter`) wired via our existing firebase-admin singleton (no second Firebase App). Collection names namespaced to `authjs_users`, `authjs_accounts`, `authjs_sessions`, `authjs_verification_tokens` so they don't collide with Step 11's Bag-of-Holding docs at `users/{uid}/bag/...`.
- **`allowDangerousEmailAccountLinking: true`** on Google provider. Safe here: both Google and Resend independently verify email ownership (Google's OAuth profile + verified-email flag; Resend via the "click the link in your inbox" round-trip). Without it, a user who signed up via magic link couldn't later sign in with Google on the same email — Auth.js would treat them as distinct accounts.
- **JWT callback now prefers OAuth profile over the stored user record** for `token.name / email / picture`. Resend-created user records only carry `email` (no name, no avatar). When Google later linked to the same email, the adapter linked accounts but didn't update the user record's fields — so falling back to `user.name` / `user.image` produced nameless, avatar-less sessions. Prefer-profile logic populates the JWT directly on every OAuth sign-in.
- **Sign-in UI.** `SignInMenu` now calls `signIn()` without a provider argument, routing users to Auth.js's built-in `/api/auth/signin` page which renders both providers side-by-side (Google button + email input). Custom themed sign-in page is a Step 16 polish item.
- **Smoke tests (Yorri, Apr 19):**
  1. Signed out state → "Sign in" pill → Auth.js page → both providers visible. ✓
  2. Email path: entered `halftonejones@gmail.com`, clicked Sign in with Resend → "Check your email" page → email arrived from `onboarding@resend.dev` → clicking link signed in, avatar fallback icon + email visible. ✓
  3. Google path on the same account (post-11.5 `allowDangerousEmailAccountLinking: true` + prefer-profile callback): works, now with profile picture + first name visible in header. ✓
- **Firestore state after first use:** new collections `authjs_users`, `authjs_accounts`, `authjs_verification_tokens`. One `authjs_users/<id>` doc for `halftonejones@gmail.com`; two `authjs_accounts` docs pointing at it (`provider: "resend"`, `provider: "google"`). Bag of Holding data at `users/{google-sub}/bag/...` unchanged.
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — exit 0).
- **Known intentional behavior:**
  - Each re-sign-in via magic link generates a fresh email (one-time tokens — that's how passwordless works). A persistent session between sign-ins doesn't reduce the number of emails for new sign-ins.
  - Resend-only users show a fallback User icon in the header, no name — Resend doesn't supply profile data. Fixed by the user later adding Google, at which point the session fills in.
  - The stock Auth.js sign-in page isn't themed to Obsidian & Ember yet — Step 16 polish.

## Step 11.5 Files Changed

| File | Change |
|---|---|
| `arcane/auth.config.ts` | NEW — edge-safe base config (Google + session strategy + callbacks) |
| `arcane/auth.ts` | EDITED — spreads authConfig, adds Resend + FirestoreAdapter |
| `arcane/src/proxy.ts` | EDITED — re-runs `NextAuth(authConfig)` instead of importing `auth` from auth.ts (keeps adapter out of the edge runtime) |
| `arcane/src/components/sign-in-menu.tsx` | EDITED — `signIn()` with no provider argument; routes through Auth.js's built-in multi-provider sign-in page |
| `arcane/package.json` + `pnpm-lock.yaml` | EDITED — adds `@auth/firebase-adapter ^2.10.0`, `resend ^6.12.0` |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 11.5 marked complete (this block); Step 11 row updated to note 11.5 landing |

---

## Step 12 Verification Evidence

- **Admin gate.** `arcane/src/proxy.ts` (Next 16's renamed middleware, located at `src/` to sit next to `src/app/`) wraps Auth.js's `auth()` higher-order function. Matcher: `/admin/:path*`. Reads `ADMIN_EMAILS` env var (comma-separated, lowercased) and checks `req.auth.user.email` against the allowlist. Unauthenticated callers bounce to `/api/auth/signin?callbackUrl=<original>`; signed-in non-admins land on `/not-authorized` (friendly 403 page).
- **Smoke test.** `curl -I http://localhost:3000/admin` as unauthenticated returned `HTTP 307` with `location: /api/auth/signin?callbackUrl=%2Fadmin` — proxy firing correctly. Admin-email browser session lands on `/admin` directly.
- **Admin shell.** `arcane/src/components/admin/admin-shell.tsx` — bronze top band with `ADMIN · Arcane Analytics · <breadcrumbs>`, title, description, 6xl container. Used by `/admin`, `/admin/harvest`, `/admin/backerkit`.
- **`/admin` landing.** Tile grid: two active tiles (Harvesting Cockpit, BackerKit Console), two planned tiles (Library Clerk, Scrying Chamber) marked "Step 12.5" so the roadmap is visible to the operator.
- **Harvesting Cockpit (`/admin/harvest`).** Reads the bookmarklet registry from `scripts/*.txt` + `utils/dmsguild_incursion_mini.js` via a `"server-only"` lib. Three cards:
  - **Amazon Harvester** (`scripts/amazon_bookmarklet.txt`, 14 KB).
  - **Kickstarter Harvester** (`scripts/kickstarter_bookmarklet.txt`, 5 KB).
  - **DMsGuild / DTRPG Incursion** (`utils/dmsguild_incursion_mini.js`, wrapped at load via `wrapBookmarkletFromJs()` — URL-encode + `javascript:` prefix). Legacy `prompt("Enter Ritual Key:")` removed; now hardcodes `KEY = 'ArcaneLibrarian2026'` matching Amazon/Kickstarter. Source file `utils/dmsguild_incursion.js` updated to stay in sync.
  - React strips `href="javascript:..."` from JSX — `BookmarkletCard` sets the real href via a `useRef` + `useEffect` after mount. Click is intercepted (clicking in the admin page would run the script against the admin DOM).
- **BackerKit Console (`/admin/backerkit`).** Terminal-styled card with Run button, polls `/api/admin/backerkit/status` every 2s while any run is active, idles otherwise.
  - `POST /api/admin/backerkit/run` — `requireAdmin()` guard, writes `admin/runs/backerkit/{runId}` with `status: "running"`, fires `POST backerkit-harvester` with `body: "{}"` (Cloud Run rejects body-less POSTs with HTTP 411), updates the doc with `"completed"` / `"failed"` + summary when the fetch settles.
  - `GET /api/admin/backerkit/status?limit=N` — returns the latest N runs with ISO timestamps.
  - Completed rows show the harvester's `summary.message` inline (emoji stripped) — e.g. "Inserted 10 BackerKit projects." Failed rows show the error in red.
- **Cloud Run IAM** — `backerkit-harvester` had no IAM bindings and 403'd every caller. Granted `allUsers: roles/run.invoker` matching `bouncer-api`'s public pattern (Yorri pre-authorized this in the Step 12 scoping discussion).
- **Admin Atlas tile.** `atlas-sections.ts` gains `adminOnly?: boolean` and `visibleAtlasSections(email)` helper. The Atlas (`atlas.tsx`) now reads `useSession()` and filters admin-only sections against `NEXT_PUBLIC_ADMIN_EMAILS`. Signed-out + non-admin sessions never see the Admin (KeyRound) tile; admins see it as an active tile alongside Home/Trends/Articles/Bag.
- **`admin-guard.ts`** — shared `requireAdmin()` helper so every `/api/admin/*` route re-checks the allowlist. Proxy.ts only covers page routes (API is outside the matcher by design).
- **End-to-end user testing (Yorri, Apr 19):**
  1. Admin tile appears in Atlas when signed in as allowlisted email; disappears on sign-out. ✓
  2. Unauthenticated `/admin` redirects through the sign-in flow and lands back on `/admin` after success. ✓
  3. Harvesting Cockpit renders 3 cards; Amazon + Kickstarter pills drag-install; DMsGuild/DTRPG pill drag-installs and runs without the ritual-key prompt on both hosts. ✓
  4. BackerKit Run button fires the harvester; run row flips to "completed" with "Inserted 10 BackerKit projects." inline; Firestore `admin/runs/backerkit/<runId>` doc contains the full summary. ✓
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — exit 0).
- ESLint clean (pre-existing `concept-drawer.tsx` unused-import warning unchanged).
- **Deferred:**
  - **Step 12.5 — Library Clerk + Scrying Chamber port** from the legacy HTML admin (`frontend/old_admin_yorri.html`). Concept recategorize/approve/archive, CSV ingests for DMs Guild / DTRPG, and the data-stream health dashboard live there; placeholder tiles already render on `/admin`.
  - **IAP** — will be layered in front of the Cloud Run service at deploy time (not yet created). Proxy.ts stays as the belt; IAP becomes the suspenders.

## Step 12 Files Changed

| File | Change |
|---|---|
| `arcane/src/proxy.ts` | NEW — admin allowlist proxy (Auth.js wrapped, matcher `/admin/:path*`) |
| `arcane/src/app/not-authorized/page.tsx` | NEW — friendly 403 page |
| `arcane/src/lib/admin-guard.ts` | NEW — `requireAdmin()` helper for API routes |
| `arcane/src/lib/bookmarklets.ts` | NEW — server-only bookmarklet registry (`readBookmarklet`, `wrapBookmarkletFromJs`) |
| `arcane/src/components/admin/admin-shell.tsx` | NEW — back-office page frame with breadcrumbs |
| `arcane/src/components/admin/bookmarklet-card.tsx` | NEW — drag-install pill with `useRef` javascript: href |
| `arcane/src/components/admin/harvest-console.tsx` | NEW — terminal-styled BackerKit runner with inline count readout |
| `arcane/src/app/admin/page.tsx` | NEW — admin landing with active + planned tiles |
| `arcane/src/app/admin/harvest/page.tsx` | NEW — Harvesting Cockpit |
| `arcane/src/app/admin/backerkit/page.tsx` | NEW — BackerKit Console |
| `arcane/src/app/api/admin/backerkit/run/route.ts` | NEW — fire-and-forget trigger + Firestore run log |
| `arcane/src/app/api/admin/backerkit/status/route.ts` | NEW — list recent runs |
| `arcane/src/lib/atlas-sections.ts` | EDITED — added `adminOnly` flag + Admin tile + `visibleAtlasSections()` |
| `arcane/src/components/atlas.tsx` | EDITED — filters admin tile via `useSession()` |
| `utils/dmsguild_incursion.js` | EDITED — removed ritual-key prompt, hardcoded `KEY` |
| `utils/dmsguild_incursion_mini.js` | EDITED — same (the variant the Cockpit registry reads) |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 12 marked complete (this block) |

**GCP state change (one-time):** `gcloud run services add-iam-policy-binding backerkit-harvester --member=allUsers --role=roles/run.invoker` — matches the bouncer-api public pattern.

---

## Step 11 Verification Evidence

- **GCP setup (one-time, user-performed):** Enabled Firestore API in `dnd-trends-index`; created Firestore `(default)` database in Native mode, Standard edition, `us-central1`, restrictive security rules, real-time disabled. Created OAuth 2.0 Web application client with `http://localhost:3000` JS origin + `http://localhost:3000/api/auth/callback/google` redirect URI. Ran `gcloud auth application-default login` so the Firebase Admin SDK picks up user credentials via ADC.
- **Auth.js v5 wired.** `arcane/auth.ts` + `arcane/auth.d.ts` hold the NextAuth config (Google provider, JWT session, `session.user.id` set from the Google `sub` claim via `jwt`/`session` callbacks). `/api/auth/[...nextauth]/route.ts` re-exports the handlers. Smoke test: `curl http://localhost:3000/api/auth/providers` returned HTTP 200 with `{"google":{"id":"google","callbackUrl":"http://localhost:3000/api/auth/callback/google"}}`.
- **Sign-in UI.** `SignInMenu` client component lives in `SiteHeader`. Unauthenticated → "Sign in" pill that calls `signIn("google", { callbackUrl: location.href })`. Authenticated → Google avatar + first-name (hidden on narrow screens) opening a Popover with full name + email + Sign out button. `next.config.ts` allows `lh3.googleusercontent.com` under `images.remotePatterns` so `next/image` renders the Google avatar.
- **Firebase Admin client** (`src/lib/firebase-admin.ts`) — lazy singleton, `applicationDefault()` credential source, ADC-driven. `"server-only"` import guard prevents accidental client-bundle inclusion.
- **Bag of Holding API** (`src/app/api/bag/`):
  - `GET /api/bag` → lists the signed-in user's items.
  - `POST /api/bag` → upserts one item; uses its `id` as the Firestore doc id.
  - `DELETE /api/bag?id=...` → removes one item.
  - `POST /api/bag/merge` → silent union-dedupe of localStorage items vs. cloud items (cloud entry wins on id collision), batch-writes new ones, returns full merged set.
  - All handlers call `auth()` and return 401 if unauthenticated.
- **Client sync glue.** `BagSync` component (headless, mounted in `RootLayout` inside `AuthSessionProvider`) watches `useSession()` and the Zustand hydration gate. On sign-in: POSTs local items to `/api/bag/merge`, calls `setItems()` with the merged result, flips `syncStatus` to `"synced"`, and registers a module-scoped `BagSyncer` that mirrors subsequent stow/unstow to `POST /api/bag` and `DELETE /api/bag?id=`. On sign-out: clears local items, deregisters the syncer, resets `syncStatus` to `"anon"`.
- **bag-store.ts changes.** Added `syncStatus: "anon" | "syncing" | "synced"`, `setSyncStatus`, `setItems`, `registerBagSyncer`, and mirrored the three stow actions + `unstow` to the active syncer. `syncStatus` is omitted from persist's `partialize` so it re-derives from session state on every reload.
- **End-to-end verification (Yorri, in browser):**
  1. Signed in with Google while anonymous localStorage items were present → items mirrored to `users/{uid}/bag/*` in Firestore (console visible).
  2. Signed out → local bag cleared automatically.
  3. Signed back in → items restored via `/api/bag/merge` (cloudCount = N, localNewCount = 0).
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — exit 0).
- ESLint clean (pre-existing `concept-drawer.tsx` unused-import warning unchanged).
- **Deferred:**
  - **Step 11.5 — magic-link email provider.** Requires picking an email provider (Resend / SendGrid / SMTP) and wiring NextAuth's EmailProvider.
  - **Saved lenses persistence.** Tracked in `project_saved_lenses_backlog.md` — revisit once the lens filter mechanic is formalized, fallback to Step 16 polish pass.
  - **Production deploy.** Cloud Run service `arcane-analytics` still not created. When it is, the Google OAuth client will need a second redirect URI (`https://<cloud-run-host>/api/auth/callback/google`) and the app will need to be moved from Testing → In production (Google verification) for non-test-user sign-ins.

## Step 11 Files Changed

| File | Change |
|---|---|
| `arcane/auth.ts` | NEW — Auth.js v5 top-level config |
| `arcane/auth.d.ts` | NEW — module augmentation for session.user.id / JWT.uid |
| `arcane/src/app/api/auth/[...nextauth]/route.ts` | NEW — re-exports GET/POST from auth.ts |
| `arcane/src/components/session-provider.tsx` | NEW — "use client" wrapper around Auth.js SessionProvider |
| `arcane/src/components/sign-in-menu.tsx` | NEW — sign-in pill / authenticated account popover |
| `arcane/src/components/site-header.tsx` | EDITED — slots SignInMenu left of the Atlas trigger |
| `arcane/next.config.ts` | EDITED — allow lh3.googleusercontent.com remote images |
| `arcane/src/lib/firebase-admin.ts` | NEW — lazy Firestore admin singleton |
| `arcane/src/app/api/bag/route.ts` | NEW — GET/POST/DELETE for signed-in user's bag |
| `arcane/src/app/api/bag/merge/route.ts` | NEW — POST union-dedupe merge endpoint |
| `arcane/src/lib/bag-store.ts` | EDITED — syncStatus, setItems, BagSyncer, mirrored stow/unstow |
| `arcane/src/components/bag-sync.tsx` | NEW — headless effect component orchestrating the sync lifecycle |
| `arcane/src/app/layout.tsx` | EDITED — AuthSessionProvider + BagSync mounted above existing providers |
| `arcane/package.json` + `pnpm-lock.yaml` | EDITED — add next-auth@5.0.0-beta.31, firebase-admin |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 11 marked complete (this block) |

---

## Step 10 Verification Evidence

- Site-wide `<SiteHeader />` mounted in `arcane/src/app/layout.tsx` inside `AtlasProvider`. Renders on all four live pages — `/`, `/overview`, `/articles`, `/collection` — each returning HTTP 200 with the Arcane Analytics wordmark and `aria-label="Open the Atlas site map"` trigger present in the SSR HTML (verified via `curl`).
- `AtlasProvider` uses shadcn `Sheet` (same Radix-Dialog primitive as Step 8's concept drawer). Responsive via `window.innerWidth < 768` resize listener: `side="bottom"` on mobile (92vh, rounded top corners, drag-handle affordance) / `side="right"` on desktop (max-w-lg). Glassmorphic surface: `bg-iron/80 backdrop-blur-xl border-bronze/60`.
- Eight tiles in `arcane/src/lib/atlas-sections.ts` split into two sections:
  - **Available (4):** Home → `/`, Trends → `/overview`, Articles → `/articles`, Bag of Holding → `/collection`. Each tile is a `<Link>` that closes the Atlas on navigate.
  - **Planned (4):** Products & Opportunities, Digital & BG3, Deep Dives, Methodology. Rendered as disabled `role="button"` with a shadcn Tooltip ("Not built yet — the Council hasn&rsquo;t filed this beat.") on focus/hover.
- Landing page (`/`) pruned: the old 4-button nav row (`Overview lens →`, `Articles →`, `CardChrome harness`, `Palette & fonts`) was replaced with a short "Tap Atlas in the header for the full site map" nudge plus a muted dev-harness row keeping just `/test-card-chrome` and `/swatch`.
- Bag of Holding sigil: composite `BagOfHoldingSigil` component (`arcane/src/components/bag-of-holding-sigil.tsx`) — `PackageOpen` base with a smaller `Infinity` charm tucked into the bottom-right (rounded onyx socket, bronze edge). Both glyphs inherit `currentColor` so the active/planned palette drives both. Replaces an initial `Package` / `Backpack` pass that read as kitchenware.
- `atlas-sections.ts` icon field relaxed from `LucideIcon` to `ComponentType<{ className?: string }>` so composite sigils satisfy the contract alongside plain Lucide icons.
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — exit 0, zero output).
- ESLint clean (one pre-existing unused-import warning in `concept-drawer.tsx` unchanged).
- Deferred from Step 10: first-visit auto-open behavior (spec §3.6 "doubles as onboarding"), Aceternity Spotlight flourish on the main header (Step 13), Bag-badge count in the header (Step 5 extension), auth avatar (Step 11). Header is a minimal MVP by design so these slots attach without a refactor.

## Step 10 Files Changed

| File | Change |
|---|---|
| `arcane/src/lib/atlas-sections.ts` | NEW — single source of truth for the tile registry (4 active + 4 planned) |
| `arcane/src/components/atlas.tsx` | NEW — AtlasProvider context + responsive Sheet + grid of tiles |
| `arcane/src/components/site-header.tsx` | NEW — sticky site-wide header (wordmark + Atlas trigger) |
| `arcane/src/components/bag-of-holding-sigil.tsx` | NEW — composite PackageOpen + Infinity charm sigil |
| `arcane/src/app/layout.tsx` | EDITED — wired AtlasProvider + SiteHeader into the provider stack |
| `arcane/src/app/page.tsx` | EDITED — dropped redundant per-page nav; added Atlas nudge + muted dev-harness row |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 10 marked complete (this block) |

---

## Step 9b Verification Evidence

- `bouncer-api` Cloud Function redeployed with `/articles` route. Smoke test: `curl .../bouncer-api/articles?limit=3` returns both parallel-run rows (Bursar + Quartermaster) with full byline metadata (`author_name`, `author_beat`, `author_bio`, `council_version='v1'`). Legacy 3-persona rows excluded by the version filter.
- `deploy_bouncer.py` corrected: `FUNCTION_NAME` was `get_trend_data` (a legacy twin Cloud Function serving the old HTML frontend); changed to `bouncer-api` so future Bouncer edits actually reach the Arcane frontend. `get_trend_data` left deployed — see `project_get_trend_data_orphan.md` memory.
- `arcane/src/app/api/articles/route.ts` returns `{ articles, count }` wrapper at `http://localhost:3000/api/articles?limit=5` (HTTP 200, JSON verified).
- `/articles` Server Component renders HTTP 200 with 1-hr ISR. Two Council articles appear as CardChrome-wrapped cards in a 2-col grid on desktop (1280×800), single-column on mobile (375×812). Verified in Claude Preview.
- Article card byline row: Lucide sigil (Crown for Bursar, Anchor for Quartermaster) + author name (Spectral) + beat (Inter muted). Clicking the byline opens a shadcn Popover with the sigil, beat in ember mono-uppercase, and full bio — confirmed working on desktop click; same component also works on mobile tap.
- Hook renders as italicized Spectral with left ember border. Body rendered via `react-markdown` with prose overrides (headings, lists, strong, em, inline code). Key-stat chip renders as mono ember-bright in iron-backed pill.
- Confidence stubbed at 75 silver (metal-tier pip visible top-right) with `// STUB` comment — article-level confidence is a follow-up once we decide how to aggregate the Council member's cited sources.
- Sage system prompt extended with a COUNCIL CHAIR FRAMING block: 5-member roster with beats + luminary lineages injected inline. Sage remains "Sage" (she/her, no article) and may cite Council members by name without role-playing as them.
- Landing page (`/`) gains an "Articles →" nav link alongside "Overview lens →".
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — zero errors).
- ESLint clean (`pnpm lint` — zero errors; pre-existing unused-import warning in `concept-drawer.tsx` unchanged).
- Zero console errors/warnings on `/articles` (verified via Claude Preview).
- `react-markdown ^10.1.0` added; `@radix-ui/react-popover ^1.1.15` pinned (was referenced by `popover.tsx` since Step 6 but missing from `package.json`).

## Step 9b Files Changed

| File | Change |
|---|---|
| `bouncer/main.py` | EDITED — added `/articles` router branch + handler |
| `deploy_bouncer.py` | EDITED — `FUNCTION_NAME` from `get_trend_data` to `bouncer-api` |
| `arcane/src/lib/bouncer.ts` | EDITED — `Article` type, `CouncilAuthorName` union, `fetchArticles()` |
| `arcane/src/app/api/articles/route.ts` | NEW — thin Next proxy over Bouncer `/articles` |
| `arcane/src/components/article-card.tsx` | NEW — CardChrome-wrapped article card with sigil/byline/bio popover + markdown body |
| `arcane/src/app/articles/page.tsx` | NEW — Server Component grid with 1-hr ISR + empty state |
| `arcane/src/app/page.tsx` | EDITED — added `/articles` nav link |
| `arcane/src/app/api/sage/route.ts` | EDITED — Council Chair framing + naming convention (she/her, "Sage" no article, Council = "The X") |
| `arcane/package.json` + `pnpm-lock.yaml` | EDITED — added `react-markdown`, pinned `@radix-ui/react-popover` |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 9b marked complete (this block) |

---

## Step 9a Verification Evidence

- `gold_data.daily_articles` migrated: 4 new nullable columns (`author_name`, `author_beat`, `author_bio`, `council_version`) added via `setup_council_columns.py`. Legacy rows stay NULL.
- `gold_data.freight_index_daily` created (partitioned by date, clustered by lane_code) via `setup_freight_index_daily.py`. Columns: date, lane_code, lane_name, index_value, wow_delta_pct, source, raw_json.
- `dnd-daily-journalist` Cloud Function redeployed with Council refactor + `{"mode":"both"}` support for parallel-run. Memory bumped to 1 GiB, Vertex model = `gemini-2.5-flash`. Pandas removed from `fetch_context()` (OOM on cold start at 512 MiB).
- `freight-index-harvester` Cloud Function deployed. Extracts lane values from the embedded `window.frProductIntroTickerData` JSON on `fbx.freightos.com` (DOM scrape was wrong — initial selector was walking up to a shared container). Live smoke test inserted 3 rows for 2026-04-17: FBX ($1,877 +3.32%), FBX01 China→NAWC ($2,488 +2.79%), FBX03 China→NAEC ($3,678 +9.79%).
- Schedulers:
  - `trigger-daily-journalist`: body flipped to `{"mode":"both"}`, fires daily 04:30 America/Chicago. Parallel-run window: 2026-04-17 → ~2026-04-22 (3–5 days per plan).
  - `trigger-freight-index-harvester`: new job, `0 22 * * 6` America/Chicago (Sat 22:00 local, auto-DST).
- Smoke test: forced-Bursar Council article via `{"mode":"council","writer":"bursar"}` returned HTTP 200, wrote to BQ with `council_version='v1'` and all author_* columns populated. Voice check: "portfolio," "strategic stasis" — matches brief.

## Step 9a Files Changed

| File | Change |
|---|---|
| `cloud_functions/daily_journalist/council.py` | NEW — roster (Loremaster/Bursar/Quartermaster/Weaver/Architect), anomaly→writer routing, rotation guard via `gold_data.daily_articles`, prompt assembly |
| `cloud_functions/daily_journalist/main.py` | REWRITTEN — single-Council-article default, `?mode=legacy/both/council` paths, `?writer=<key>` override, no pandas, tolerant query helper |
| `cloud_functions/freight_index_harvester/main.py` | NEW — ticker JSON regex, 3 lane codes, inserts one row per lane |
| `cloud_functions/freight_index_harvester/requirements.txt` | NEW |
| `setup_council_columns.py` | NEW — idempotent ALTER TABLE for Council columns (EXECUTED) |
| `setup_freight_index_daily.py` | NEW — CREATE TABLE (EXECUTED) |
| `deploy_daily_journalist.py` | NEW — gcloud functions deploy wrapper, 1 GiB memory |
| `deploy_freight_index_harvester.py` | NEW |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 9a verification evidence (this block) |

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

## Step 6.5 Verification Evidence

- Sage panel streams response, shows "VERIFYING CLAIMS..." with pulsing Shield icon during grounding check
- Post-stream grounding check fires automatically via POST to `/api/sage/ground`
- Inline `(confidence%)[n]` superscript markers appear on factual claims (up to 5 per response)
- Footnotes section renders with per-claim: footnote number, confidence %, source name, and claim text or explanation
- GroundingByline shows below message: shield icon + headline score + data sources + algo version
- All 100% confidence on direct data matches (e.g. "Fighter (98)" when source data shows Fighter: 98)
- Graceful degradation: if grounding check fails, returns 75% silver fallback with `-fallback` algo version tag
- Robust Gemini parsing: handles `citations` vs `fact_checks` field name variance, nested `sources_available`, missing `explanation`
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — zero errors)

## Step 6.5 Files Changed

| File | Change |
|---|---|
| `arcane/src/lib/grounding.ts` | NEW — GroundingCitation/GroundingResult types, GROUNDING_SYSTEM_PROMPT, GROUNDING_RESPONSE_SCHEMA, findClaimPosition(), positionedCitations(), groundingTier() |
| `arcane/src/app/api/sage/ground/route.ts` | NEW — POST endpoint for async grounding check, robust Gemini response normalization, graceful fallback |
| `arcane/src/components/sage-panel.tsx` | EDITED — post-stream grounding trigger (useEffect), renderAnnotatedText(), GroundingByline, GroundingFootnotes, "Verifying claims..." indicator |
| `CONFIDENCE_METHODOLOGY.md` | EDITED — replaced §6.3 placeholder with full AI grounding implementation docs |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 6.5 marked complete, verification evidence added |

## Step 7 Verification Evidence

- Asked "What are the top monsters on Fandom right now?" → Sage called `getLeaderboard("fandom", "Monster")`, returned real data (Space Clown: 99, Tiefling: 95, Couatl: 92, etc.)
- Tool invocation chip renders: "LOADED LEADERBOARD DATA" with Database icon in compact format
- Asked "What are the top movers right now? Is our data fresh?" → Sage called BOTH `getTopMovers` AND `getSystemHealth` (multi-tool, two chips visible)
- Multi-step enabled: `stopWhen: stepCountIs(5)` allows up to 5 LLM round-trips
- Tool results fed to grounding check: claims from tool data score 100% grounding (not flagged as hallucinated)
- Grounding footnotes correctly cite tool results as source: `[1] 100% · getTopMovers tool result — "..."`
- Tool chips render independently from grounding annotations (fix applied for rendering path split)
- Graceful fallback: if a Bouncer endpoint fails, the tool returns an error object and the Sage tells the user plainly
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — zero errors)
- System prompt updated to instruct tool usage: "USE THE TOOLS to get real data — do not guess or make up numbers"

## Step 7 Files Changed

| File | Change |
|---|---|
| `arcane/src/lib/sage-tools.ts` | NEW — 8 tool definitions with Zod v4 schemas + Bouncer API execute functions |
| `arcane/src/app/api/sage/route.ts` | EDITED — added `tools: sageTools` + `stopWhen: stepCountIs(5)`, updated system prompt, increased maxDuration to 60s |
| `arcane/src/components/sage-panel.tsx` | EDITED — ToolInvocationChip component, extractToolContext() for grounding, tool part rendering split from text rendering |

## Step 8 Verification Evidence

- ConceptLink components render on Overview page (Top Classes + Top Opportunities leaderboards)
- Clicking a concept name opens the Concept Detail Drawer via `/api/concept?name=X` server-side aggregation
- Drawer direction is responsive: `side="bottom"` on mobile (`< 768px`), `side="right"` on desktop
- Mobile drawer: 85vh height, rounded top corners, drag-handle bar visual affordance
- Desktop drawer: full-height, max-width 512px, slides from right
- Three dismissal methods: click overlay backdrop, X button (top-right), Escape key (all via Radix Dialog)
- Confidence section shows score/tier with metal-tier color, methodology breakdown (streams, families, agreement, velocity, binding constraint)
- Cross-source appearances table shows per-source score + rank with trend indicators (TrendingUp/Minus/TrendingDown)
- "Ask the Sage" button builds rich context (concept name, category, confidence, source list) and opens Sage panel
- "Stow" button saves concept summary to Bag of Holding with toggle behavior
- Server-side aggregation route fires 10 parallel requests via `Promise.allSettled` (confidence + search + 8 leaderboard sources)
- Absent sources listed at bottom of appearances section
- TypeScript type-checks clean (`pnpm exec tsc --noEmit` — zero errors)

## Step 8 Files Changed

| File | Change |
|---|---|
| `arcane/src/components/concept-drawer.tsx` | NEW — ConceptDrawerProvider (context + Sheet), ConceptLink (tappable wrapper), ConfidenceSection, AppearancesSection, ActionsSection |
| `arcane/src/components/ui/sheet.tsx` | NEW — shadcn Sheet primitive (Radix Dialog-based, side variants) |
| `arcane/src/app/api/concept/route.ts` | NEW — Server-side aggregation endpoint, 10 parallel Bouncer API calls |
| `arcane/src/app/layout.tsx` | EDITED — wrapped children in ConceptDrawerProvider |
| `arcane/src/app/overview/page.tsx` | EDITED — wrapped concept names in ConceptLink |
| `FRONTEND_BUILD_STATUS.md` | EDITED — Step 8 marked complete |
