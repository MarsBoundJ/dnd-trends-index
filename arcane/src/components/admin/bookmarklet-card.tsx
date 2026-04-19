"use client"

/*
 * Arcane Analytics — Bookmarklet install card (Step 12).
 *
 * React strips `href="javascript:..."` from JSX in dev warnings and
 * future versions may strip it entirely as a security hardening. The
 * browser, however, NEEDS the anchor to carry the literal javascript
 * URL so drag-to-bookmark installs it as an executable bookmark.
 *
 * Workaround: render the anchor with `href="#"` in JSX, then set the
 * real `javascript:...` href via a ref in useEffect. That keeps React
 * happy, keeps SSR deterministic, and preserves the drag-install UX.
 *
 * Click-to-run is intentionally disabled — clicking the pill here
 * would execute the script in this admin page's context, where the
 * Amazon / Kickstarter DOM selectors would all no-op and confuse the
 * operator. The only legitimate interaction is "drag it to your
 * bookmarks bar, then click it from the target site."
 */

import React from "react"
import { GripVertical, ArrowUpRight } from "lucide-react"

import { cn } from "@/lib/utils"

export interface BookmarkletCardProps {
  id: string
  name: string
  targetHost: string
  description: string
  usage: string
  /** The raw `javascript:...` URL. */
  href: string
}

export function BookmarkletCard({
  id,
  name,
  targetHost,
  description,
  usage,
  href,
}: BookmarkletCardProps) {
  const anchorRef = React.useRef<HTMLAnchorElement>(null)

  React.useEffect(() => {
    if (anchorRef.current) {
      anchorRef.current.setAttribute("href", href)
    }
  }, [href])

  return (
    <article
      className={cn(
        "rounded-xl border border-bronze bg-onyx p-5 space-y-4",
        "transition-colors hover:border-ember/70",
      )}
    >
      <header className="flex items-start justify-between gap-3">
        <div className="space-y-0.5">
          <h3 className="font-display text-base font-semibold text-parchment">
            {name}
          </h3>
          <p className="font-mono text-[11px] uppercase tracking-widest text-ember/80">
            {targetHost}
          </p>
        </div>
      </header>

      <p className="text-sm text-ash leading-relaxed">{description}</p>

      {/* The install pill. */}
      <div className="space-y-2">
        <p className="font-mono uppercase tracking-widest text-[10px] text-ash/60">
          Drag to bookmarks bar ↓
        </p>
        <a
          ref={anchorRef}
          href="#"
          draggable
          onClick={(e) => {
            // Clicking here would execute the bookmarklet in *this* page.
            // The operator wants it on their bookmarks bar, where it'll
            // run against the target site.
            e.preventDefault()
          }}
          data-bookmarklet-id={id}
          className={cn(
            "inline-flex items-center gap-2 rounded-md px-3 py-2",
            "border border-ember/70 bg-iron",
            "text-sm text-parchment font-display",
            "cursor-grab active:cursor-grabbing select-none",
            "hover:border-ember hover:bg-iron/80",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ember focus-visible:ring-offset-2 focus-visible:ring-offset-obsidian",
          )}
        >
          <GripVertical className="h-4 w-4 text-ember-bright" aria-hidden />
          <span>{name}</span>
        </a>
      </div>

      <footer className="border-t border-bronze/40 pt-3">
        <p className="text-xs text-ash/90 leading-relaxed flex items-start gap-1.5">
          <ArrowUpRight
            className="h-3.5 w-3.5 text-ember-bright shrink-0 mt-0.5"
            aria-hidden
          />
          <span>{usage}</span>
        </p>
      </footer>
    </article>
  )
}
