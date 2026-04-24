/*
 * Arcane Analytics — Signal Source Mix donut chart.
 *
 * Client component (Recharts in browser DOM). Renders a donut showing
 * what proportion of total signal volume comes from each data source
 * (Google Trends, Reddit, YouTube, Fandom, BGG, Amazon). The legend
 * sits beside the donut rather than below, because space is tighter
 * on a single-column card and a side legend reads cleaner.
 *
 * Values are percentages of total summed "heat" across all sources.
 * Source-ordering is deterministic (by config) so the donut visual
 * stays consistent across rerenders.
 */

"use client"

import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
} from "recharts"

export interface SignalSourceSlice {
  source: string
  label: string
  total: number
  color: string
}

interface SignalSourceDonutProps {
  data: SignalSourceSlice[]
}

export function SignalSourceDonut({ data }: SignalSourceDonutProps) {
  const grandTotal = data.reduce((sum, slice) => sum + slice.total, 0)

  // Add computed percentages so the tooltip and legend share them.
  const enrichedData = data.map((slice) => ({
    ...slice,
    percent: grandTotal > 0 ? (slice.total / grandTotal) * 100 : 0,
  }))

  return (
    <div className="flex items-center gap-4 flex-wrap sm:flex-nowrap">
      {/* Donut — compact, minimal */}
      <div className="relative shrink-0 w-[160px] h-[160px]">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={enrichedData}
              dataKey="total"
              cx="50%"
              cy="50%"
              innerRadius={48}
              outerRadius={72}
              paddingAngle={1.5}
              startAngle={90}
              endAngle={-270}
              strokeWidth={0}
            >
              {enrichedData.map((slice) => (
                <Cell key={slice.source} fill={slice.color} />
              ))}
            </Pie>
            <Tooltip content={<DonutTooltip />} />
          </PieChart>
        </ResponsiveContainer>
        {/* Center label — total number of sources */}
        <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
          <p className="font-display text-2xl font-semibold text-parchment tabular-nums">
            {enrichedData.length}
          </p>
          <p className="font-mono text-[9px] uppercase tracking-widest text-ash">
            sources
          </p>
        </div>
      </div>

      {/* Legend — source name, color swatch, %-of-total */}
      <ul className="flex-1 space-y-1.5 min-w-[140px]">
        {enrichedData.map((slice) => (
          <li
            key={slice.source}
            className="flex items-center gap-2 font-sans text-xs text-parchment"
          >
            <span
              className="h-2.5 w-2.5 rounded-sm shrink-0"
              style={{ backgroundColor: slice.color }}
              aria-hidden
            />
            <span className="flex-1 truncate">{slice.label}</span>
            <span className="font-mono text-[11px] text-ash tabular-nums">
              {slice.percent.toFixed(0)}%
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
}

// Custom tooltip — shows source name, raw total, and %.
interface TooltipPayloadItem {
  payload?: SignalSourceSlice & { percent?: number }
}

function DonutTooltip({
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
    <div className="rounded-md border border-bronze bg-iron/95 px-3 py-2 font-sans text-xs text-parchment shadow-lg">
      <p className="font-display text-sm font-semibold">{p.label}</p>
      <div className="mt-1 font-mono text-[11px] tabular-nums text-ash">
        <p>
          <span className="text-parchment">
            {p.percent !== undefined ? p.percent.toFixed(1) : "0"}%
          </span>{" "}
          of total signal
        </p>
        <p className="text-[10px] opacity-80">
          raw heat: {Math.round(p.total).toLocaleString()}
        </p>
      </div>
    </div>
  )
}
