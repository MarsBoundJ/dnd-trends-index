/*
 * Arcane Analytics — Bag of Holding sigil (Step 10).
 *
 * Composite icon: a `PackageOpen` base with a small `Infinity` charm
 * tucked into the bottom-right corner. The infinity glyph connotes the
 * bag's bottomless / extradimensional nature, which is what makes the
 * Bag of Holding the Bag of Holding rather than a plain backpack.
 *
 * Renders in a `relative` wrapper so the absolute-positioned charm
 * anchors off the base icon. Both icons inherit color from the parent
 * via `currentColor`, so the Atlas tile's existing
 * text-ember-bright / text-ash styling continues to apply uniformly.
 *
 * Note: lucide-react's `Infinity` export shadows the JS builtin if
 * imported bare — aliased to `InfinityIcon` to avoid the confusion.
 */

import { PackageOpen, Infinity as InfinityIcon } from "lucide-react"
import { cn } from "@/lib/utils"

export interface BagOfHoldingSigilProps {
  className?: string
}

export function BagOfHoldingSigil({ className }: BagOfHoldingSigilProps) {
  return (
    <span
      aria-hidden
      className={cn(
        "relative inline-flex items-center justify-center shrink-0",
        className,
      )}
    >
      <PackageOpen className="h-full w-full" strokeWidth={1.75} />
      {/*
       * Infinity charm. Sits just off the bottom-right of the base icon
       * (so it reads as a "socket" or "rune" attached to the bag, not
       * as part of the bag's own outline). Background matches the tile
       * surface (onyx) with a thin bronze edge so the charm separates
       * cleanly against both the active and the planned tile colors.
       */}
      <span
        className={cn(
          "absolute -bottom-1 -right-1.5",
          "flex items-center justify-center",
          "rounded-full bg-onyx border border-bronze/70",
          "p-[1px]",
        )}
      >
        <InfinityIcon className="h-2 w-2" strokeWidth={2.5} />
      </span>
    </span>
  )
}
