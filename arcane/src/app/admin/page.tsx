/*
 * Arcane Analytics — Admin landing (Step 12).
 *
 * Tile grid of the available back-office surfaces. Two active tiles
 * (Harvesting Cockpit, BackerKit Console) + two planned tiles
 * (Library Clerk, Scrying Chamber) tracked for Step 12.5. The
 * planned tiles render greyed with a short note so the operator can
 * see the full admin roadmap at a glance.
 */

import Link from "next/link"
import {
  Hand,
  TerminalSquare,
  Library,
  Crosshair,
  type LucideIcon,
} from "lucide-react"

import { AdminShell } from "@/components/admin/admin-shell"
import { cn } from "@/lib/utils"

interface AdminTile {
  id: string
  title: string
  description: string
  icon: LucideIcon
  href?: string
  status: "active" | "planned"
  note?: string
}

const TILES: AdminTile[] = [
  {
    id: "harvest",
    title: "Harvesting Cockpit",
    description: "Drag-install bookmarklets for manual harvests (Amazon, Kickstarter).",
    icon: Hand,
    href: "/admin/harvest",
    status: "active",
  },
  {
    id: "backerkit",
    title: "BackerKit Console",
    description: "Fire the BackerKit scraper and watch recent runs.",
    icon: TerminalSquare,
    href: "/admin/backerkit",
    status: "active",
  },
  {
    id: "library-clerk",
    title: "Library Clerk",
    description: "Concept recategorization, approval / archiving, CSV ingests.",
    icon: Library,
    status: "planned",
    note: "Step 12.5",
  },
  {
    id: "scrying-chamber",
    title: "Scrying Chamber",
    description: "Data-stream health — last-scraped timestamps, term counts, errors.",
    icon: Crosshair,
    status: "planned",
    note: "Step 12.5",
  },
]

export const metadata = {
  title: "Admin · Arcane Analytics",
}

export default function AdminIndexPage() {
  const active = TILES.filter((t) => t.status === "active")
  const planned = TILES.filter((t) => t.status === "planned")

  return (
    <AdminShell
      title="Back Office"
      description="Admin-only surfaces. Harvests, data-quality monitors, and the odd one-off the Council doesn't need to see."
    >
      <section aria-label="Available admin sections">
        <h2 className="font-mono uppercase tracking-widest text-[10px] text-ash/80 mb-3">
          Available
        </h2>
        <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {active.map((t) => (
            <AdminTileCard key={t.id} tile={t} />
          ))}
        </ul>
      </section>

      <section aria-label="Planned admin sections">
        <h2 className="font-mono uppercase tracking-widest text-[10px] text-ash/80 mb-3">
          Planned (Step 12.5)
        </h2>
        <ul className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {planned.map((t) => (
            <AdminTileCard key={t.id} tile={t} />
          ))}
        </ul>
      </section>
    </AdminShell>
  )
}

function AdminTileCard({ tile }: { tile: AdminTile }) {
  const Icon = tile.icon
  const body = (
    <div
      className={cn(
        "h-full rounded-lg border p-4 flex flex-col gap-1.5 transition-colors",
        tile.status === "active"
          ? "border-bronze bg-onyx hover:border-ember hover:bg-iron"
          : "border-bronze/40 bg-onyx/50 cursor-not-allowed opacity-70",
      )}
    >
      <div className="flex items-center gap-2">
        <Icon
          className={cn(
            "h-4 w-4 shrink-0",
            tile.status === "active" ? "text-ember-bright" : "text-ash/60",
          )}
          aria-hidden
        />
        <span
          className={cn(
            "font-display text-sm font-semibold",
            tile.status === "active" ? "text-parchment" : "text-parchment/70",
          )}
        >
          {tile.title}
        </span>
      </div>
      <p
        className={cn(
          "text-xs leading-relaxed",
          tile.status === "active" ? "text-ash" : "text-ash/70",
        )}
      >
        {tile.description}
      </p>
      {tile.status === "planned" && tile.note && (
        <p className="font-mono uppercase tracking-widest text-[10px] text-ember/70 mt-1">
          {tile.note}
        </p>
      )}
    </div>
  )

  if (tile.status === "active" && tile.href) {
    return (
      <li>
        <Link
          href={tile.href}
          className="block h-full rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian"
        >
          {body}
        </Link>
      </li>
    )
  }

  return (
    <li title={tile.note ?? "Coming soon"}>{body}</li>
  )
}
