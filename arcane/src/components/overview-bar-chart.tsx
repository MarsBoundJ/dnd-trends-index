/*
 * Arcane Analytics — Category Heat bar chart.
 *
 * Client component (Recharts requires the browser DOM). Renders a horizontal
 * bar chart of D&D categories sorted by Google Trends heat score.
 *
 * Colors are hardcoded to Obsidian & Ember palette hex values — Tailwind
 * class names can't reach inside SVG rendered by Recharts.
 */

"use client"

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Cell,
  ResponsiveContainer,
} from "recharts"

interface OverviewBarChartProps {
  data: { name: string; heat: number }[]
}

export function OverviewBarChart({ data }: OverviewBarChartProps) {
  return (
    <ResponsiveContainer width="100%" height={180}>
      <BarChart
        data={data}
        margin={{ top: 4, right: 4, left: -16, bottom: 48 }}
        barCategoryGap="22%"
      >
        <XAxis
          dataKey="name"
          tick={{ fill: "#8A8578", fontSize: 10, fontFamily: "var(--font-jetbrains-mono)" }}
          angle={-40}
          textAnchor="end"
          interval={0}
          tickLine={false}
          axisLine={{ stroke: "#3A2E1F" }}
        />
        <YAxis
          domain={[0, 100]}
          tick={{ fill: "#8A8578", fontSize: 10, fontFamily: "var(--font-jetbrains-mono)" }}
          tickLine={false}
          axisLine={false}
          width={28}
        />
        <Bar dataKey="heat" radius={[3, 3, 0, 0]} maxBarSize={32}>
          {data.map((_, i) => (
            <Cell
              key={i}
              fill={i === 0 ? "#E87722" : "#B8692A"}
              opacity={1 - i * 0.05}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}
