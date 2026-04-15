/*
 * Arcane Analytics — Step 2 CardChrome verification harness.
 *
 * Renders CardChrome with five wildly different content types to prove
 * the container is truly universal (§4.4 — Strict Chrome, Loose Content).
 * One card per metal tier to verify all five confidence pip colors and
 * hover border transitions.
 *
 * No real data — all content is dummy. Real data wiring is Step 3.
 */

import { CardChrome } from "@/components/card-chrome"

// ─── Dummy content types ──────────────────────────────────────────────────

/** 1. Plain text — article / AI summary card */
function ArticleContent() {
  return (
    <div className="space-y-2">
      <p className="font-sans text-sm text-parchment/90 leading-relaxed">
        Low-level martial classes are showing a surprising uptick in Reddit discussion
        relative to their actual play rates — a gap that historically precedes product
        launches by 6–10 weeks. This is an exploratory signal; data coverage is thin.
      </p>
      <p className="font-mono text-xs text-ash">Source: reddit · 3 subreddits · 7-day window</p>
    </div>
  )
}

/** 2. Stat block — single-number hero card */
function StatContent() {
  return (
    <div className="flex items-end gap-3 py-2">
      <span className="font-mono text-5xl font-bold text-parchment leading-none">82</span>
      <div className="pb-1 space-y-0.5">
        <p className="font-sans text-xs text-ash uppercase tracking-widest">Trend Score</p>
        <p className="font-mono text-xs text-druid">↑ +4.2 vs last week</p>
      </div>
    </div>
  )
}

/** 3. Leaderboard list */
function LeaderboardContent() {
  const items = [
    { rank: 1, name: "Paladin",   score: "94" },
    { rank: 2, name: "Ranger",    score: "88" },
    { rank: 3, name: "Warlock",   score: "83" },
    { rank: 4, name: "Barbarian", score: "79" },
    { rank: 5, name: "Rogue",     score: "74" },
  ]
  return (
    <ol className="space-y-1.5">
      {items.map((item) => (
        <li key={item.rank} className="flex items-center gap-3">
          <span className="font-mono text-xs text-ash w-4 text-right shrink-0">
            {item.rank}
          </span>
          <span className="font-sans text-sm text-parchment flex-1">{item.name}</span>
          <span className="font-mono text-xs text-ember-bright">{item.score}</span>
        </li>
      ))}
    </ol>
  )
}

/** 4. AI insight / Sage quote */
function SageContent() {
  return (
    <figure className="space-y-3">
      <blockquote className="border-l-2 border-ember pl-3">
        <p className="font-display text-sm italic text-parchment/90 leading-relaxed">
          &ldquo;The Ranger renaissance is not driven by mechanical interest alone —
          the BG3 storyline activated latent nostalgia for exploration-fantasy that
          predates 5e. The creator economy is amplifying it now.&rdquo;
        </p>
      </blockquote>
      <figcaption className="font-mono text-xs text-ash pl-3">
        The Sage · Strategist voice · composite_concept_index
      </figcaption>
    </figure>
  )
}

/** 5. Chart stub — placeholder for a real Tremor chart (Step 3) */
function ChartContent() {
  // Fake sparkline bars to hint at the chart shape without real data
  const bars = [30, 45, 38, 52, 60, 55, 71, 68, 75, 82, 78, 88]
  const max = Math.max(...bars)
  return (
    <div className="space-y-3">
      <div className="flex items-end gap-1 h-16" aria-hidden>
        {bars.map((h, i) => (
          <div
            key={i}
            className="flex-1 rounded-sm bg-arcane/50"
            style={{ height: `${(h / max) * 100}%` }}
          />
        ))}
      </div>
      <div className="flex justify-between">
        <span className="font-mono text-xs text-ash">Jan 15</span>
        <span className="font-mono text-xs text-ash">Apr 15</span>
      </div>
      <p className="font-mono text-[10px] text-ash uppercase tracking-widest">
        Tremor chart · Step 3 · dummy data
      </p>
    </div>
  )
}

// ─── Page ─────────────────────────────────────────────────────────────────

export default function TestCardChromePage() {
  return (
    <main className="mx-auto max-w-6xl px-6 py-16 space-y-12">

      {/* Header */}
      <header className="space-y-4">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Step 2 of 16 · CardChrome · verification harness
        </p>
        <h1 className="font-display text-4xl font-semibold tracking-tight text-parchment">
          CardChrome
        </h1>
        <p className="font-sans text-base text-ash max-w-2xl">
          The universal card container. Five content types × five confidence tiers.
          Hover over any card on desktop to see the border transition from bronze to
          the card&rsquo;s metal tier. Hover over the pip (top-right dot, in the
          metadata cluster) to see the confidence tooltip.
        </p>
        <p className="font-sans text-sm text-ash">
          <span className="text-parchment font-medium">All content is dummy.</span>{" "}
          Real data wiring is Step 3. Aceternity glow halo is Step 13.
        </p>
      </header>

      {/* Card grid — one card per confidence tier */}
      <section className="space-y-6">
        <h2 className="font-display text-2xl font-semibold text-parchment">
          Five Content Types &times; Five Confidence Tiers
        </h2>
        <p className="font-sans text-sm text-ash">
          Each card uses a different confidence value to exercise all five metal tiers
          (copper → silver → gold → platinum → mithral). The pip color and hover border
          color should match.
        </p>

        <div className="grid grid-cols-1 gap-5 sm:grid-cols-2 lg:grid-cols-3">

          {/* Copper — exploratory, plain text */}
          <CardChrome
            title="The Gnome Problem: A Theory"
            subtitle="exploratory · reddit · 7d"
            lens="deep-dive"
            cardType="article"
            confidence={55}
          >
            <ArticleContent />
          </CardChrome>

          {/* Silver — stat block */}
          <CardChrome
            title="Paladin Trend Score"
            subtitle="overview · composite_concept_index"
            lens="overview"
            cardType="stat"
            confidence={74}
          >
            <StatContent />
          </CardChrome>

          {/* Gold — leaderboard */}
          <CardChrome
            title="Top Classes by Reddit Mentions"
            subtitle="game-dev · 30-day window"
            lens="game-dev"
            cardType="leaderboard"
            confidence={87}
          >
            <LeaderboardContent />
          </CardChrome>

          {/* Platinum — Sage quote */}
          <CardChrome
            title="Ranger Renaissance: A Sage Insight"
            subtitle="creator · cross_pollination_v2"
            lens="creator"
            cardType="article"
            confidence={93}
          >
            <SageContent />
          </CardChrome>

          {/* Mithral — chart stub */}
          <CardChrome
            title="Ranger vs. Paladin: 90-Day Trend"
            subtitle="marketing · blue_ocean · high confidence"
            lens="marketing"
            cardType="chart"
            confidence={97}
          >
            <ChartContent />
          </CardChrome>

        </div>
      </section>

      {/* Checklist — things to verify visually */}
      <section className="space-y-4 border-t border-bronze pt-10">
        <h2 className="font-display text-xl font-semibold text-parchment">Step 2 Visual Checklist</h2>
        <ul className="space-y-1.5 font-sans text-sm text-ash list-none">
          {[
            "Bronze border at rest on every card",
            "Hover over each card → border transitions to the card's tier color",
            "Pip colors: copper (warm brown), silver (grey), gold (warm gold), platinum (pale), mithral (blue)",
            "Hover over each pip → tooltip shows confidence % and tier label",
            "All five content types render coherently inside the same chrome",
            "Stow and Explain buttons present and consistently positioned on every card",
            "Confidence pip + two empty icon-slot placeholders in top-right cluster",
          ].map((item) => (
            <li key={item} className="flex items-start gap-2">
              <span className="text-bronze mt-0.5 shrink-0">□</span>
              <span>{item}</span>
            </li>
          ))}
        </ul>
      </section>

      <footer className="border-t border-bronze pt-6 flex items-center justify-between">
        <p className="font-mono text-xs text-ash">
          FRONTEND_DESIGN_SPEC.md · §4.4 Strict Chrome, Loose Content
        </p>
        <a
          href="/swatch"
          className="font-mono text-xs text-ash hover:text-ember transition-colors"
        >
          ← palette &amp; fonts
        </a>
      </footer>
    </main>
  )
}
