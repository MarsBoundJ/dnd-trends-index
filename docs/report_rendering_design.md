# Report Rendering — design

**Status:** proposed, Sep 2, 2026. No code written.
**Supersedes:** `FRONTEND_DESIGN_SPEC.md` §407 PDF-export choice, for reports
only — argued in "PDF" below.

---

## The problem is not "reports should be dynamic"

That framing understates it. Three concrete things are wrong, and only the second
is about freshness.

### 1. Published artifacts are not reproducible from the repo

> **Corrected Sep 2, 2026.** This section first claimed the published page could
> not be regenerated from the repo, citing text counts that "do not reconcile".
> That was wrong twice over, and the way it was wrong is worth keeping.
>
> The counts came from `grep -c`, which counts *matching lines* — and markdown
> paragraphs are single long lines while pandoc wraps its output. Comparing that
> number across two formats measures line-wrapping, not content. Counting actual
> occurrences, the two files agree to within the three commits the HTML is
> behind. **The artifact is fully reproducible.**
>
> Which is the same error the reports themselves made: a number that was real,
> attached to a claim it did not support.

`arcane/public/reports/ip_deep_dive.html` is pandoc output — the file says so
(`<meta name="generator" content="pandoc" />`) — built from
`pitch/report/trusight_breakdowns_scratch.md` by `make_html.ps1`, which passes
`--metadata 'pagetitle=IP Deep Dive: 19 Licensing Candidates'`, matching the
HTML `<title>` exactly.

The real defect is narrower and entirely fixable:

1. The script wrote `trusight_breakdowns.html`; the site serves
   `ip_deep_dive.html`. **The rename was a manual step recorded nowhere.**
2. So the published page is whatever someone last copied by hand — a May build
   (#73), while the source was corrected in September (#103).
3. Nothing in the artifact said which source or which commit it came from, so
   nobody could tell it was stale by looking at it.

**The pipeline was reproducible; the recipe was not written down.** The wrong
prose is still live not because it cannot be rebuilt, but because rebuilding
required knowledge that existed only in someone's memory.

Both halves are now closed: `make_html.ps1` publishes to
`arcane/public/reports/ip_deep_dive.html` directly, and stamps a provenance
footer (`docs/provenance_convention.md`) naming source, commit SHA, build time
and rebuild command. `make_html.bat` is now a thin wrapper rather than a second
copy of the same build.

Three pipelines remain, and consolidating them is still the goal:

| Pipeline | Input | Output |
|---|---|---|
| `pitch/report/build_*.js` (`docx` lib) | values hardcoded in JS | `.docx` → PDF |
| `pitch/report/make_html.ps1` (pandoc) | `*_scratch.md` + `pdf_style.css` | HTML → published |
| the other six PDFs | unestablished | `arcane/public/reports/*` |

### 2. Data is embedded in generator source

`build_ip_licensing_report.js` contains the literals `0.13%` and `0.99%`. The
report is not a document containing numbers; it is a *program* containing
numbers. Correcting the data cannot correct the report.

### 3. Claims are typed, not computed

Of the six errors found in the published deep dive (work item I in
`data_capture_hardening_plan.md`), **four were prose**:

- three IPs each asserting "Lowest of any IP measured"
- Demon Slayer called "third-largest after One Piece and FFXIV" when it is larger
  than FFXIV
- a published AO3 zero presented as an audience finding
- the "ship-fic / **naval**" homophone collapse, generalised across three IPs

And a demonstration of why patching prose does not hold: **PR #103's correction
was insertion-only — 16 insertions, 0 deletions.** It appended a note next to the
wrong sentence rather than replacing it. Both still stand in the markdown; the
original still stands, uncorrected, on the live site.

---

## What this design buys — three separable properties

Worth separating, because they have very different costs and you may not want
all three at once.

| Property | Means | Cost |
|---|---|---|
| **Reproducibility** | any published artifact rebuilds from repo + data | moderate |
| **Computed claims** | unsupported superlatives become *unrenderable* | moderate |
| **Pinned freshness** | corrections propagate; sent artifacts stay stable | small, if built in early |

Reproducibility is the one that fixes the root problem. Computed claims is the
one that prevents recurrence. Pinning is cheap now and expensive to retrofit.

---

## Non-goals

- Rewriting the reports' voice or argument. This is plumbing, not editing.
- Migrating all seven reports at once.
- Removing the `docx` path. If WotC wants a Word file, that is a real
  requirement and this design does not serve it.
- Making prose correct. **It cannot.** See "The honest limit" below.

---

## Architecture

### Data layer — already exists, unchanged

`arcane/src/lib/bouncer.ts`: *"All reads are server-side (Next 16 Server
Components) with 1-hour ISR revalidation."* `/matrix/universes-beyond`,
`/overview` and `/articles` already use it. Reports are the only part of the site
outside this pipeline. **This is a port, not a new system.**

Reports read the same gold views everything else does — `fanfic_crossover_rate`,
`fanfic_crossover_proxy`, `concept_confidence`.

### Route shape

```
/reports/[slug]            latest render, ISR
/reports/[slug]?v=<n>      a pinned version
/reports                   index (replaces the hardcoded list on the homepage)
```

Server Components throughout. No client-side data fetching — a PDF must not
depend on JS having finished.

### The claim component contract

The core rule:

> **If a sentence contains a number, a rank, or a superlative, it is a component,
> not text.**

```tsx
<Metric   ip="One Piece" field="crossover_rate" />        // 0.052%
<Rank     ip="One Piece" field="crossover_rate" />        // 11th of 25
<Superlative field="crossover_rate" dir="min" />          // resolves to ONE IP
<Comparison a="House of the Dragon" b="Mistborn" field="crossover_rate" />
```

`<Superlative>` is the load-bearing one. It makes three IPs claiming "lowest"
*impossible* rather than merely unnoticed — the component computes the answer, so
there is exactly one.

Every component records what it rendered, producing a **claims inventory** per
report: each computed claim, its value, its source view, and its `as_of`. That
inventory is what makes a report reviewable without re-reading it.

### Guard gate

```tsx
<ReportGuard ip="Sea of Thieves">…</ReportGuard>
```

Reads `gold_data.fanfic_capture_guard`. A CRITICAL row (zero count, metatag
inflation, extreme outlier) renders a visible banner in draft and **blocks
publish**. This is exactly how the Sea of Thieves zero would have been caught
before it went out.

### Prose stays prose — the honest limit

Components cover claims. They do not cover reasoning. Mapping the six item-I
findings:

| Finding | Prevented? |
|---|---|
| HotD 44 vs 15 numerator | ✅ binds to the view |
| FFXIV numerator drift | ✅ |
| Three IPs "lowest" | ✅ `<Superlative>` |
| Demon Slayer "third-largest" | ✅ `<Rank>` |
| Sea of Thieves zero | ⚠️ caught by the guard gate |
| **The ship-fic / naval pun** | ❌ **not addressed** |

The pun is the worst of the six and this design does nothing about it. The number
was right; the sentence was wrong. **Data binding fixes numbers, not reasoning.**
Prose review stays human. Anyone selling this design as "reports can't be wrong
any more" is overselling it.

One mitigation worth noting: the claims inventory makes the *reviewable surface*
much smaller. Reviewing 40 computed claims plus prose is tractable; re-reading
3,000 lines of markdown for arithmetic is not.

---

## PDF

### Not `@react-pdf/renderer`, for reports

`FRONTEND_DESIGN_SPEC.md:407` specifies it. For reports specifically I would
revisit that: react-pdf is a **separate layout engine** — its own StyleSheet, no
Tailwind, no shared CSS — so the HTML report and the PDF report become two
implementations of one document, and they drift.

Given that this entire design exists because two representations of the same
report already drifted, adopting a second renderer would reintroduce the failure
mode at a different layer. It remains a reasonable choice for generated briefs
that have no HTML equivalent.

### The ladder — and rung 1 already exists

`pitch/report/pdf_style.css` is 208 lines and already handles the hard part:

```css
*, *::before, *::after { print-color-adjust: exact !important; }
```

That is the print stylesheet. It should be ported into the app as the print
layer rather than rewritten.

1. **Print CSS + `window.print()`** — reuse `pdf_style.css`. Cheapest. Limit:
   no page numbers, because browsers do not implement CSS Paged Media counters.
2. **Add Paged.js** — real pagination, page numbers, running headers, in-browser.
   Same CSS.
3. **Headless Chrome on Cloud Run** — true download button, `displayHeaderFooter`.
   Renders *the same print CSS from rung 1*. Cloud Run and deploy tooling already
   exist here.

Each rung reuses the previous one's work, so rung 1 is never throwaway. Start
there and stop if it is good enough.

---

## Versioning — pin at publish

A report is a claim made *at a time*. If it renders live, numbers change under
readers: you email someone citing 0.13%, they open the link next month and see
0.042%, and you can no longer reconstruct what you sent.

```
report_versions
  slug, version, rendered_at, data_as_of, claims_inventory, html_snapshot
```

- Internal view: live, ISR, always current.
- Anything sent outward: a **pinned version**, frozen, with `data_as_of` shown
  on the page.
- An explicit "refresh to current" action creates a new version rather than
  mutating the old one.

Outreach is paused, which makes now the cheap moment to build this in. Retrofitting
version pinning after links are in circulation is materially harder.

---

## Migration order

**IP Deep Dive first.** It is the only report with an HTML version, the only one
audited, the largest, and the one whose errors are already catalogued — so
correctness of the port is checkable against a known list.

Then reassess. Six more reports is a lot of prose migration and the second one
will tell us the real per-report cost.

---

## Open questions

1. ~~Where does `ip_deep_dive.html` come from?~~ **Answered Sep 2, 2026** —
   pandoc, from `trusight_breakdowns_scratch.md`, via `make_html.ps1`, with a
   hand-copy step that is now automated. The markdown is the source of truth,
   so the port starts there. Provenance of the other six reports is still
   unestablished.
2. **Is the `.docx` path still required?** If WotC wants Word files, that is a
   separate output and this design does not replace it.
3. **Auth.** Reports are currently public static files; the app uses next-auth.
   Do pinned report links stay public?
4. **Does the prose live in markdown or in the JS builders?** Both, currently.
   The port needs one source.

---

## Risks

- **Scope.** Seven reports of dense prose. The claim components are the small
  part; moving and re-checking the prose is the large one.
- **Rebuilding will surface discrepancies.** Numbers on the live site may not
  reproduce from current data. Expect that, and treat each as a finding to
  record rather than a bug to silence — several will be real corrections, as
  work item I already showed.
- **Over-trust.** The largest risk is cultural: believing a computed report
  cannot be wrong. Findings 1 of item I is the standing counter-example and
  should be cited in the report template itself.
