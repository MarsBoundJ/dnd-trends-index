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
import {
  OpportunityScatter,
  type OpportunityScatterPoint,
} from "@/components/opportunity-scatter"
import {
  SignalSourceDonut,
  type SignalSourceSlice,
} from "@/components/signal-source-donut"
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
  filterYouTubeConsensus,
  filterFandomTTRPG,
  topOpportunities,
  topCategoriesByHeat,
  UB_MEDIUM_LABEL,
} from "@/lib/bouncer"

export default async function OverviewPage() {
  // Parallel fetch across multiple Bouncer endpoints — all are independent
  // and 1-hr ISR cached, so fanning out keeps the page snappy even with
  // 10+ cards' worth of data sources.
  const [
    data,
    redditLeaderboard,
    youtubeLeaderboardRaw,
    fandomLeaderboardRaw,
    bggLeaderboard,
    amazonLeaderboard,
    ubMatrix,
  ] = await Promise.all([
    fetchBouncerData(),
    fetchLeaderboards("reddit"),
    fetchLeaderboards("youtube"),
    fetchLeaderboards("fandom"),
    fetchLeaderboards("bgg"),
    fetchLeaderboards("amazon"),
    fetchUBMatrix(5),
  ])

  // Apply source-specific cleaners before flattening — YouTube needs
  // multi-creator filtering to avoid the "everyone's at 100" normalization
  // artifact; Fandom needs TTRPG-source filtering to avoid surfacing
  // Stranger Things / Severance characters on a D&D-centric card.
  const youtubeLeaderboard = filterYouTubeConsensus(youtubeLeaderboardRaw, 2)
  const fandomLeaderboard = filterFandomTTRPG(fandomLeaderboardRaw)

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

  // ── Card 5: YouTube Creator Consensus (filtered to >=2 creators) ────────
  // Sort by creator_count desc (with score as tiebreaker) so the "most-
  // agreed-upon" concepts surface first rather than single-creator outliers.
  const youtubeTop = flattenTopItems(youtubeLeaderboard, 20) // Take more then re-sort
    .sort((a, b) => {
      const c = (b.creator_count ?? 0) - (a.creator_count ?? 0)
      return c !== 0 ? c : b.score - a.score
    })
    .slice(0, 5)

  // ── Card 6: Universes Beyond Top-5 (new, cross-links to /matrix) ────────
  const ubTop = ubMatrix.candidates.slice(0, 5)

  // ── Card 7: Top on Fandom (new, TTRPG-filtered) ─────────────────────────
  const fandomTop = flattenTopItems(fandomLeaderboard, 5)

  // ── Card 8: BGG Hotness (new, TTRPG rulebooks & adventures) ─────────────
  const bggTop = flattenTopItems(bggLeaderboard, 5)

  // ── Card 9: Amazon Bestsellers (new, with price) ────────────────────────
  const amazonTop = flattenTopItems(amazonLeaderboard, 5)

  // ── Card 10: Opportunity Landscape (scatter chart, new visualization) ───
  // Gather every concept with a meaningful interest × opportunity pair.
  // Drop zeros on either axis (they'd pile up at origin). Limit to 80 dots
  // to keep the chart legible — sorted by opportunity descending so the
  // "top-right story" (high interest + high opportunity) surfaces reliably.
  const scatterPoints: OpportunityScatterPoint[] = data
    .flatMap((cat) =>
      cat.items
        .filter((i) => i.score > 0 && i.opportunity_index > 0)
        .map((i) => ({
          name: i.name,
          category: cat.category,
          score: i.score,
          opportunityIndex: i.opportunity_index,
        })),
    )
    // Dedupe by name — the same concept can appear under multiple category
    // rollups. Keep the highest-opportunity instance.
    .reduce<OpportunityScatterPoint[]>((acc, p) => {
      const existing = acc.find((x) => x.name === p.name)
      if (!existing) {
        acc.push(p)
      } else if (p.opportunityIndex > existing.opportunityIndex) {
        existing.opportunityIndex = p.opportunityIndex
        existing.score = p.score
      }
      return acc
    }, [])
    .sort((a, b) => b.opportunityIndex - a.opportunityIndex)
    .slice(0, 80)

  // ── Card 11: Signal Source Mix (donut chart, new visualization) ─────────
  // Sum heat across all categories for each source, then split the donut
  // by source. Tells the cross-stream story in a single glance.
  function sumHeat(cats: typeof data): number {
    return cats.reduce((sum, cat) => sum + (cat.heat ?? 0), 0)
  }
  const sourceMix: SignalSourceSlice[] = [
    { source: "google", label: "Google Trends", total: sumHeat(data), color: "#e87722" },
    { source: "reddit", label: "Reddit", total: sumHeat(redditLeaderboard), color: "#5fc9e7" },
    { source: "youtube", label: "YouTube", total: sumHeat(youtubeLeaderboard), color: "#8b5cf6" },
    { source: "fandom", label: "Fandom", total: sumHeat(fandomLeaderboard), color: "#6baa75" },
    { source: "bgg", label: "BGG", total: sumHeat(bggLeaderboard), color: "#d4a94a" },
    { source: "amazon", label: "Amazon", total: sumHeat(amazonLeaderboard), color: "#7ab8e0" },
  ].filter((s) => s.total > 0)

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
    `Source: consensus_score across tracked D&D-content creators; filtered to concepts mentioned by >=2 creators to avoid per-category normalization artifacts.\n` +
    `Top 5 by creator count:\n` +
    youtubeTop
      .map(
        (y, i) =>
          `  ${i + 1}. ${y.name} — ${y.creator_count ?? 0} creators, score ${Math.round(y.score)}`,
      )
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

  const fandomContext =
    `Card: "Top on Fandom" (Overview lens)\n` +
    `Source: Fandom wiki page-view hype, filtered to D&D / Pathfinder / 5e / Critical Role wikis. Cross-medium IPs surfaced separately on the Universes Beyond Matrix.\n` +
    `Top 5:\n` +
    fandomTop
      .map((f, i) => `  ${i + 1}. ${f.name} — hype ${Math.round(f.score)}`)
      .join("\n")

  const bggContext =
    `Card: "BGG Hotness" (Overview lens)\n` +
    `Source: BoardGameGeek rank + owned counts for TTRPG rulebooks and adventures.\n` +
    `Top 5:\n` +
    bggTop
      .map(
        (b, i) =>
          `  ${i + 1}. ${b.name} — rank ${Math.round(b.score)}, owned by ${b.owned ?? "?"}`,
      )
      .join("\n")

  const amazonContext =
    `Card: "Amazon Bestsellers" (Overview lens)\n` +
    `Source: Amazon sales rank for D&D books, adventures, DM screens. Lower sales_rank = higher demand.\n` +
    `Top 5:\n` +
    amazonTop
      .map(
        (a, i) =>
          `  ${i + 1}. ${a.name} — sales_rank ${a.sales_rank ?? "?"}, price ${a.formatted_price ?? "?"}`,
      )
      .join("\n")

  const scatterContext =
    `Card: "Opportunity Landscape" (Overview lens — scatter chart)\n` +
    `X-axis: interest score (Google Trends, 0-100). Y-axis: opportunity_index (demand vs. supply gap; higher = bigger opening). Dots colored by category.\n` +
    `Top-right quadrant = concepts with high player interest but weak product coverage — i.e., the money opening.\n` +
    `Total concepts plotted: ${scatterPoints.length}.\n` +
    `Top 5 by opportunity:\n` +
    scatterPoints
      .slice(0, 5)
      .map(
        (p, i) =>
          `  ${i + 1}. ${p.name} (${p.category}) — interest ${Math.round(p.score)}, opportunity ${Math.round(p.opportunityIndex)}`,
      )
      .join("\n")

  const donutContext =
    `Card: "Signal Source Mix" (Overview lens — donut chart)\n` +
    `Shows proportion of total signal heat coming from each tracked data source. Arcane aggregates across ${sourceMix.length} streams.\n` +
    sourceMix
      .map(
        (s) =>
          `  ${s.label}: ${((s.total / sourceMix.reduce((t, x) => t + x.total, 0)) * 100).toFixed(1)}% (raw heat ${Math.round(s.total)})`,
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

        {/* 5 — YouTube Creator Consensus (filtered to multi-creator items) */}
        <CardChrome
          title="YouTube Creator Consensus"
          subtitle="overview · tracked creators · multi-creator agreement"
          lens="overview"
          cardType="leaderboard"
          confidence={75}
          cardId="overview:top-youtube"
          sageContext={youtubeContext}
        >
          <ol className="space-y-2 py-1">
            {youtubeTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">
                No multi-creator consensus yet.
              </li>
            ) : (
              youtubeTop.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    <ConceptLink name={item.name} />
                  </span>
                  <span className="font-mono text-[10px] uppercase tracking-widest text-ash/70 tabular-nums">
                    {item.creator_count ?? 0}× creators
                  </span>
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            Ranked by number of creators agreeing · {youtubeTop.length} shown
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

        {/* 7 — Top on Fandom (TTRPG-filtered) */}
        <CardChrome
          title="Top on Fandom"
          subtitle="overview · ttrpg wikis · hype score"
          lens="overview"
          cardType="leaderboard"
          confidence={75}
          cardId="overview:top-fandom"
          sageContext={fandomContext}
        >
          <ol className="space-y-2 py-1">
            {fandomTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              fandomTop.map((item, i) => (
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
            Page-view hype across D&amp;D / Pathfinder / 5e wikis
          </p>
        </CardChrome>

        {/* 8 — BGG Hotness */}
        <CardChrome
          title="BGG Hotness"
          subtitle="overview · boardgamegeek · rulebooks & adventures"
          lens="overview"
          cardType="leaderboard"
          confidence={80}
          cardId="overview:top-bgg"
          sageContext={bggContext}
        >
          <ol className="space-y-2 py-1">
            {bggTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              bggTop.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1">
                    <ConceptLink name={item.name} />
                  </span>
                  {item.owned !== undefined && item.owned > 0 && (
                    <span className="font-mono text-[10px] uppercase tracking-widest text-ash/70 tabular-nums">
                      {item.owned.toLocaleString()} owned
                    </span>
                  )}
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            BoardGameGeek rank × ownership count
          </p>
        </CardChrome>

        {/* 9 — Amazon Bestsellers */}
        <CardChrome
          title="Amazon Bestsellers"
          subtitle="overview · amazon sales rank · d&d books"
          lens="overview"
          cardType="leaderboard"
          confidence={80}
          cardId="overview:top-amazon"
          sageContext={amazonContext}
        >
          <ol className="space-y-2 py-1">
            {amazonTop.length === 0 ? (
              <li className="font-sans text-sm text-ash">No data available.</li>
            ) : (
              amazonTop.map((item, i) => (
                <li key={item.name} className="flex items-center gap-3">
                  <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
                    {i + 1}
                  </span>
                  <span className="font-sans text-sm text-parchment flex-1 truncate">
                    <ConceptLink name={item.name} />
                  </span>
                  {item.formatted_price && (
                    <span className="font-mono text-xs text-ember-bright tabular-nums">
                      {item.formatted_price}
                    </span>
                  )}
                </li>
              ))
            )}
          </ol>
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            By Amazon sales rank · price shown
          </p>
        </CardChrome>

        {/* 10 — Opportunity Landscape scatter chart (spans 2 cols on lg+) */}
        <div className="sm:col-span-1 lg:col-span-2">
          <CardChrome
            title="Opportunity Landscape"
            subtitle="overview · interest × opportunity · all concepts"
            lens="overview"
            cardType="chart"
            confidence={75}
            cardId="overview:opportunity-scatter"
            sageContext={scatterContext}
          >
            <OpportunityScatter data={scatterPoints} />
            <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-1">
              Top-right = high interest + underexploited · {scatterPoints.length} concepts
            </p>
          </CardChrome>
        </div>

        {/* 11 — Signal Source Mix donut */}
        <CardChrome
          title="Signal Source Mix"
          subtitle="overview · cross-stream aggregation"
          lens="overview"
          cardType="chart"
          confidence={90}
          cardId="overview:source-mix"
          sageContext={donutContext}
        >
          <SignalSourceDonut data={sourceMix} />
          <p className="font-mono text-[10px] text-ash/70 uppercase tracking-widest mt-3">
            % of total signal heat per source
          </p>
        </CardChrome>

      </section>

      <footer className="border-t border-bronze pt-6 flex items-center justify-end">
        <BagLink />
      </footer>

    </main>
  )
}
