/*
 * Trusight — Category Heat bar chart.
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
    <ResponsiveContainer width="100%" height={200}>
      <BarChart
        data={data}
        margin={{ top: 4, right: 4, left: 4, bottom: 64 }}
        barCategoryGap="22%"
      >
        <XAxis
          dataKey="name"
          tick={{ fill: "#8A8578", fontSize: 10, fontFamily: "var(--font-jetbrains-mono)" }}
          angle={-45}
          textAnchor="end"
          interval={0}
          tickLine={false}
          axisLine={{ stroke: "#3A2E1F" }}
          height={72}
        />
        <YAxis
          domain={[0, 100]}
          ticks={[0, 25, 50, 75, 100]}
          tick={{ fill: "#8A8578", fontSize: 10, fontFamily: "var(--font-jetbrains-mono)" }}
          tickLine={false}
          axisLine={false}
          width={36}
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
