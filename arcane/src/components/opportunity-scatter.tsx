/*
 * Arcane Analytics — Opportunity Landscape scatter chart.
 *
 * Client component (Recharts requires browser DOM). Plots concepts on a
 * 2-D plane where X = interest (Google Trends score) and Y = opportunity
 * index. The top-right quadrant is the demo story: high-interest concepts
 * that are underexploited relative to that interest.
 *
 * Dots are colored by category using a curated palette pulled from the
 * Obsidian & Ember + rarity tokens in globals.css. Categories beyond the
 * palette size fall through to a neutral ember-bronze.
 *
 * Tooltip on hover shows concept name, category, and both axis values.
 */

"use client"

import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  ZAxis,
  Tooltip,
  CartesianGrid,
  ResponsiveContainer,
  Cell,
} from "recharts"

export interface OpportunityScatterPoint {
  name: string
  category: string
  score: number            // X axis — interest
  opportunityIndex: number // Y axis — demand/supply gap
}

interface OpportunityScatterProps {
  data: OpportunityScatterPoint[]
}

// Curated palette — pulled from globals.css Obsidian & Ember + rarity
// tokens. Designed for dot legibility on obsidian background; each color
// is distinct enough to tell categories apart at a glance.
const CATEGORY_COLORS: string[] = [
  "#e87722", // ember-bright
  "#5fc9e7", // arcane (frost)
  "#6baa75", // druid (green)
  "#8b5cf6", // void (purple)
  "#d4a94a", // rarity-gold
  "#7ab8e0", // rarity-mithral
  "#8c6239", // rarity-copper
  "#b8692a", // ember (fallback warm)
]

function colorForCategory(
  category: string,
  categoryOrder: string[],
): string {
  const idx = categoryOrder.indexOf(category)
  if (idx === -1) return CATEGORY_COLORS[CATEGORY_COLORS.length - 1]
  return CATEGORY_COLORS[idx % CATEGORY_COLORS.length]
}

export function OpportunityScatter({ data }: OpportunityScatterProps) {
  // Establish a stable category-to-color mapping — use the order categories
  // first appear in the data. Keeps the legend deterministic across rerenders.
  const categoryOrder = Array.from(
    new Set(data.map((p) => p.category)),
  ).slice(0, CATEGORY_COLORS.length)

  // Clamp outliers on Y so the chart's scale stays readable. A handful of
  // concepts have opportunity_index in the thousands (see Fighter at ~1200);
  // without clamping, every other point gets squished to the bottom.
  const yCeiling = 400
  const clampedData = data.map((p) => ({
    ...p,
    opportunityIndexClamped: Math.min(p.opportunityIndex, yCeiling),
    wasClamped: p.opportunityIndex > yCeiling,
  }))

  return (
    <ResponsiveContainer width="100%" height={320}>
      <ScatterChart
        margin={{ top: 10, right: 16, left: 8, bottom: 44 }}
      >
        <CartesianGrid stroke="#3A2E1F" strokeDasharray="2 4" vertical={true} />
        <XAxis
          type="number"
          dataKey="score"
          name="Interest"
          domain={[0, 100]}
          ticks={[0, 25, 50, 75, 100]}
          tick={{
            fill: "#8A8578",
            fontSize: 10,
            fontFamily: "var(--font-jetbrains-mono)",
          }}
          tickLine={false}
          axisLine={{ stroke: "#3A2E1F" }}
          label={{
            value: "Interest score →",
            position: "insideBottom",
            offset: -30,
            fill: "#8A8578",
            fontSize: 10,
            fontFamily: "var(--font-jetbrains-mono)",
            letterSpacing: "0.15em",
          }}
        />
        <YAxis
          type="number"
          dataKey="opportunityIndexClamped"
          name="Opportunity"
          domain={[0, yCeiling]}
          tick={{
            fill: "#8A8578",
            fontSize: 10,
            fontFamily: "var(--font-jetbrains-mono)",
          }}
          tickLine={false}
          axisLine={false}
          width={44}
          label={{
            value: "Opportunity →",
            angle: -90,
            position: "insideLeft",
            offset: 10,
            fill: "#8A8578",
            fontSize: 10,
            fontFamily: "var(--font-jetbrains-mono)",
            letterSpacing: "0.15em",
          }}
        />
        <ZAxis range={[40, 40]} />
        <Tooltip
          cursor={{ strokeDasharray: "3 3", stroke: "#B8692A" }}
          content={<ScatterTooltip />}
        />
        <Scatter data={clampedData} fillOpacity={0.85}>
          {clampedData.map((point, i) => (
            <Cell
              key={`${point.name}-${i}`}
              fill={colorForCategory(point.category, categoryOrder)}
            />
          ))}
        </Scatter>
      </ScatterChart>
    </ResponsiveContainer>
  )
}

// Custom tooltip — matches the app's Obsidian & Ember styling. Displays
// the concept name, its category, and both axis values. If the Y value
// was clamped for display, shows the original in parens.
interface TooltipPayloadItem {
  payload?: {
    name?: string
    category?: string
    score?: number
    opportunityIndex?: number
    wasClamped?: boolean
  }
}

function ScatterTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: TooltipPayloadItem[]
}) {
  if (!active || !payload || payload.length === 0) return null
  const p = payload[0].payload
  if (!p) return null

  return (
    <div
      className="rounded-md border border-bronze bg-iron/95 px-3 py-2 font-sans text-xs text-parchment shadow-lg"
      style={{ minWidth: 160 }}
    >
      <p className="font-display text-sm font-semibold text-parchment">
        {p.name}
      </p>
      <p className="mt-0.5 font-mono text-[10px] uppercase tracking-widest text-ember-bright">
        {p.category}
      </p>
      <div className="mt-2 space-y-0.5 font-mono text-[11px] tabular-nums text-ash">
        <p>
          Interest: <span className="text-parchment">{p.score?.toFixed(0)}</span>
        </p>
        <p>
          Opportunity:{" "}
          <span className="text-parchment">
            {p.opportunityIndex?.toFixed(0)}
          </span>
          {p.wasClamped && (
            <span className="ml-1 text-ember-bright text-[10px]">(off-chart)</span>
          )}
        </p>
      </div>
    </div>
  )
}
