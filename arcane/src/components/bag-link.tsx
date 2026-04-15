"use client"

/*
 * Bag of Holding link with live badge count.
 *
 * Small client component so the badge count updates reactively as the user
 * stows things. Gates the numeric count on `useBagHasHydrated()` to avoid
 * SSR/CSR mismatch — before hydration, the link still renders but with no
 * count, so the page doesn't layout-shift. A proper bottom-nav icon with
 * the bouncing badge (§2, §4.2) lands in Step 10 with Atlas.
 */

import Link from "next/link"
import { Bookmark } from "lucide-react"
import { useBagStore, useBagHasHydrated } from "@/lib/bag-store"
import { cn } from "@/lib/utils"

export function BagLink({ className }: { className?: string }) {
  const hydrated = useBagHasHydrated()
  const count = useBagStore((s) => s.items.length)

  return (
    <Link
      href="/collection"
      className={cn(
        "inline-flex items-center gap-1.5 font-mono text-xs text-ash hover:text-ember transition-colors",
        className
      )}
      aria-label={
        hydrated
          ? `Bag of Holding (${count} ${count === 1 ? "item" : "items"})`
          : "Bag of Holding"
      }
    >
      <Bookmark className="h-3.5 w-3.5" />
      <span>bag of holding</span>
      {hydrated && count > 0 && (
        <span className="rounded-full bg-ember px-1.5 py-[1px] font-mono text-[10px] font-semibold text-onyx">
          {count}
        </span>
      )}
    </Link>
  )
}
