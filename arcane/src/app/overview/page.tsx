/*
 * Arcane Analytics — Overview lens (Step 3 + Step 6 confidence wiring).
 *
 * Server Component. Fetches from the Bouncer API (1-hr revalidation) and
 * renders three CardChrome cards with real D&D trend data:
 *
 *   1. Top Classes — leaderboard by Google Trends score
 *   2. Category Heat — bar chart across all 18 categories
 *   3. Top Opportunities — leaderboard by opportunity_index
 *
 * Step 6 (§5.1): per-card confidence is now the MIN data_confidence
 * across the card's rendered concepts, looked up via the Bouncer
 * /confidence endpoint. Cards whose items aren't in the concept library
 * (e.g. Category Heat — rows are categories, not concepts) fall back to
 * the silver stub. AI grounding is not applied here (pure data cards).
 */

import Link from "next/link"

import { CardChrome } from "@/components/card-chrome"
import { OverviewBarChart } from "@/components/overview-bar-chart"
import { BagLink } from "@/components/bag-link"
import { ConceptLink } from "@/components/concept-drawer"
import {
  fetchBouncerData,
  fetchLeaderboards,
  fetchUBMatrix,
  fetchConfidence,
  cardConfidence,
  findCategory,
  deduplicateItems,
  flattenTopItems,
  topOpportunities,
  topCategoriesByHeat,
  UB_MEDIUM_LABEL,
} from "@/lib/bouncer"

export default async function OverviewPage() {
  // Parallel fetch across multiple Bouncer endpoints — all are independent
  // and 1-hr ISR cached, so fanning out keeps the page snappy even with
  // 6+ cards' worth of data sources.
  const [
    data,
    redditLeaderboard,
    youtubeLeaderboard,
    ubMatrix,
  ] = await Promise.all([
    fetchBouncerData(),
    fetchLeaderboards("reddit"),
    fetchLeaderboards("youtube"),
    fetchUBMatrix(5),
  ])

  // ── Card 1: Top Classes ─────────────────────────────────────────────────
  const classCategory = findCategory(data, "Class")
  const topClasses = classCategory
    ? deduplicateItems(classCategory.items)
        .sort((a, b) => b.score - a.score)
        .slice(0, 5)
    : []

  // ── Card 2: Category Heat ───────────────────────────────────────────────
  const heatData = topCategoriesByHeat(data, 10)

  // ── Card 3: Top Opportunities ───────────────────────────────────────────
  const opportunities = topOpportunities(data, 5)

  // ── Card 4: Reddit Top Concepts (new) ───────────────────────────────────
  const redditTop = flattenTopItems(redditLeaderboard, 5)

  // ── Card 5: YouTube Creator Consensus (new) ─────────────────────────────
  const youtubeTop = flattenTopItems(youtubeLeaderboard, 5)

  // ── Card 6: Universes Beyond Top-5 (new, cross-links to /matrix) ────────
  const ubTop = ubMatrix.candidates.slice(0, 5)

  // ── Step 6: batch-fetch data confidence for every concept we're about
  // to render, then compute one aggregate score per card (min across
  // looked-up concepts). Category Heat's "items" are category names, not
  // concepts, so they won't resolve and the card falls back to silver 75
  // — honest treatment until category-level confidence lands. ──────────
  const allNames = [
    ...topClasses.map((c) => c.name),
    ...opportunities.map((o) => o.name),
    ...heatData.map((h) => h.name), // probably no hits; fallback path
  ]
  const confidenceMap = await fetchConfidence(allNames)

  const topClassesConf = cardConfidence(
    topClasses.map((c) => c.name),
    confidenceMap,
  )
  const heatConf = cardConfidence(
    heatData.map((h) => h.name),
    confidenceMap,
  )
  const opportunitiesConf = cardConfidence(
    opportunities.map((o) => o.name),
    confidenceMap,
  )

  // ── Sage context snapshots ──────────────────────────────────────────────
  // Plain-text summaries of each card, handed to the Sage route handler on
  // Explain-click so the model can ground its answer in the numbers the user
  // is actually looking at. Kept terse — these go into the system prompt.
  const topClassesContext =
    `Card: "Top Classes" (Overview lens)\n` +
    `Source: Google Trends (7-day), vs. "Dungeons & Dragons" control.\n` +
    `Top 5:\n` +
    topClasses
      .map((c, i) => `  ${i + 1}. ${c.name} — score ${Math.round(c.score)}`)
      .join("\n")

  const heatContext =
    `Card: "Category Heat" (Overview lens)\n` +
    `Source: Google Trends, average heat per category (top 10).\n` +
    heatData
      .map((c, i) => `  ${i + 1}. ${c.name} — heat ${c.heat}`)
      .join("\n")

  const opportunitiesContext =
    `Card: "Top Opportunities" (Overview lens)\n` +
    `Source: opportunity_index (demand vs. supply gap; higher = bigger opening).\n` +
    `Top 5:\n` +
    opportunities
      .map(
        (o, i) =>
          `  ${i + 1}. ${o.name} — index ${o.opportunity_index.toFixed(0)}`
      )
      .join("\n")

  const redditContext =
    `Card: "Top on Reddit" (Overview lens)\n` +
    `Source: Reddit community chatter, scored by mention + sentiment volume.\n` +
    `Top 5:\n` +
    redditTop
      .map((r, i) => `  ${i + 1}. ${r.name} — score ${Math.round(r.score)}`)
      .join("\n")

  const youtubeContext =
    `Card: "YouTube Creator Consensus" (Overview lens)\n` +
    `Source: consensus_score across tracked D&D-content creators.\n` +
    `Top 5:\n` +
    youtubeTop
      .map((y, i) => `  ${i + 1}. ${y.name} — score ${Math.round(y.score)}`)
      .join("\n")

  const ubContext =
    `Card: "Universes Beyond Top 5" (Overview lens — preview of /matrix/universes-beyond)\n` +
    `Source: license_fit_score across 142 non-TTRPG IPs; overlays Fandom + Steam velocity on a calibrated 5-dimension rubric.\n` +
    `Top 5:\n` +
    ubTop
      .map(
        (c, i) =>
          `  ${i + 1}. ${c.ip_name} (${UB_MEDIUM_LABEL[c.medium]}) — fit ${((c.license_fit_score ?? 0) * 100).toFixed(0)}`,
      )
      .join("\n")

  return (
    <main className="mx-auto max-w-6xl px-6 py-12 space-y-10">

      {/* Page header */}
      <header className="space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Arcane Analytics · Overview
        </p>
        <h1 className="font-display text-4xl font-semibold tracking-tight text-parchment">
          State of the D&amp;D Multiverse
        </h1>
        <p className="font-sans text-sm text-ash max-w-2xl">
          Real-time trend data from across the D&amp;D ecosystem.{" "}
          <span className="text-parchment/60">
            Each card&rsquo;s confidence pip (top-right) is the min{" "}
            <span className="font-mono text-xs text-ember">data_confidence</span>{" "}
            across its rendered concepts — tap for methodology (
            <span className="font-mono text-xs text-ember">§5.1</span>).
          </span>
        </p>
      </header>

      {/* Card grid */}
      <section className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">

        {/* 1 — Top Classes leaderboard */}
        <CardChrome
          title="Top Classes"
          subtitle="overview · google trends · 7-day"
          lens="overview"
          cardType="leaderboard"
          confidence={topClassesConf.score}
          confidenceExplanation={topClassesConf.binding?.explanation ?? null}
          confidenceBindingName={topClassesConf.binding?.concept_name}
          confidenceHitCount={topClassesConf.hitCount}
          confidenceTotalCount={topClassesConf.totalCount}
          cardId="overview:top-classes"
          sageContext={topClassesContext}
        >
          <ol className="space-y-2 py-1">
            {topClasses.map((item, i) => (
              <li key={item.name} className="flex items-center gap-3">
                <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                  {i + 1}
                </span>
                <span className="font-sans text-sm text-parchment flex-1">
                  <ConceptLink name={item.name} />
                </span>
                <span className="font-mono text-xs text-ember-bright tabular-nums">
                  {Math.round(item.score)}
                </span>
              </li>
            ))}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            Score vs. &ldquo;Dungeons &amp; Dragons&rdquo; control
          </p>
        </CardChrome>

        {/* 2 — Category Heat bar chart (spans 2 cols on sm+) */}
        <div className="sm:col-span-1 lg:col-span-2">
          <CardChrome
            title="Category Heat"
            subtitle="overview · google trends · all categories"
            lens="overview"
            cardType="chart"
            confidence={heatConf.score}
            confidenceExplanation={heatConf.binding?.explanation ?? null}
            confidenceBindingName={heatConf.binding?.concept_name}
            confidenceHitCount={heatConf.hitCount}
            confidenceTotalCount={heatConf.totalCount}
            cardId="overview:category-heat"
            sageContext={heatContext}
          >
            <OverviewBarChart data={heatData} />
            <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-1">
              Average heat score per category · top 10
            </p>
          </CardChrome>
        </div>

        {/* 3 — Top Opportunities leaderboard */}
        <CardChrome
          title="Top Opportunities"
          subtitle="overview · opportunity index · all categories"
          lens="overview"
          cardType="leaderboard"
          confidence={opportunitiesConf.score}
          confidenceExplanation={opportunitiesConf.binding?.explanation ?? null}
          confidenceBindingName={opportunitiesConf.binding?.concept_name}
          confidenceHitCount={opportunitiesConf.hitCount}
          confidenceTotalCount={opportunitiesConf.totalCount}
          cardId="overview:top-opportunities"
          sageContext={opportunitiesContext}
        >
          <ol className="space-y-2 py-1">
            {opportunities.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              opportunities.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    <ConceptLink name={item.name} />
                  </span>
                  <span className="font-mono text-xs text-druid tabular-nums">
                    {item.opportunity_index.toFixed(0)}
                  </span>
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            Demand vs. supply gap · higher = bigger opening
          </p>
        </CardChrome>

        {/* 4 — Top on Reddit (new) */}
        <CardChrome
          title="Top on Reddit"
          subtitle="overview · community chatter · 7-day"
          lens="overview"
          cardType="leaderboard"
          confidence={75}
          cardId="overview:top-reddit"
          sageContext={redditContext}
        >
          <ol className="space-y-2 py-1">
            {redditTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              redditTop.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    <ConceptLink name={item.name} />
                  </span>
                  <span className="font-mono text-xs text-ember-bright tabular-nums">
                    {Math.round(item.score)}
                  </span>
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            Mention + sentiment volume across 25 subreddits
          </p>
        </CardChrome>

        {/* 5 — YouTube Creator Consensus (new) */}
        <CardChrome
          title="YouTube Creator Consensus"
          subtitle="overview · tracked creators · consensus score"
          lens="overview"
          cardType="leaderboard"
          confidence={75}
          cardId="overview:top-youtube"
          sageContext={youtubeContext}
        >
          <ol className="space-y-2 py-1">
            {youtubeTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              youtubeTop.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    <ConceptLink name={item.name} />
                  </span>
                  <span className="font-mono text-xs text-ember-bright tabular-nums">
                    {Math.round(item.score)}
                  </span>
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            Consensus score across tracked creators
          </p>
        </CardChrome>

        {/* 6 — Universes Beyond Top-5 preview (new, cross-links to /matrix) */}
        <CardChrome
          title="Universes Beyond Top 5"
          subtitle="overview · license-fit score · 142 IPs ranked"
          lens="overview"
          cardType="leaderboard"
          confidence={85}
          cardId="overview:ub-top-5"
          sageContext={ubContext}
        >
          <ol className="space-y-2 py-1">
            {ubTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              ubTop.map((c, i) => (
                <li key={c.ip_name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    {c.ip_name}
                    <span className="ml-2 font-mono text-[10px] uppercase tracking-widest text-ash/60">
                      {UB_MEDIUM_LABEL[c.medium]}
                    </span>
                  </span>
                  <span className="font-mono text-xs text-ember-bright tabular-nums">
                    {c.license_fit_score != null
                      ? (c.license_fit_score * 100).toFixed(0)
                      : "—"}
                  </span>
                </li>
              ))
            )}
          </ol>
          <div className="flex items-center justify-between mt-3">
            <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest">
              Rubric × market signal · 0-100 scale
            </p>
            <Link
              href="/matrix/universes-beyond"
              className="font-mono text-[10px] uppercase tracking-widest text-ember-bright hover:text-ember transition-colors"
            >
              Full matrix →
            </Link>
          </div>
        </CardChrome>

      </section>

      <footer className="border-t border-bronze pt-6 flex items-center justify-end">
        <BagLink />
      </footer>

    </main>
  )
}
