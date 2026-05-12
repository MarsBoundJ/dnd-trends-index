/*
 * Trusight — Admin shell wrapper (Step 12).
 *
 * Thin framing component every /admin/* page mounts inside of. Adds:
 *   - A narrow uppercase "ADMIN" breadcrumb strip under the main site
 *     header so the operator always knows they're in the back office.
 *   - A page-title + optional description + breadcrumbs row.
 *   - Max-width container around the children.
 *
 * Visually distinct from public pages via (a) the bronze top rule and
 * (b) the mono-uppercase "ADMIN / Trusight" label — no change
 * to the Obsidian & Ember palette itself.
 */

import Link from "next/link"
import { ChevronRight } from "lucide-react"

import { cn } from "@/lib/utils"

export interface AdminCrumb {
  label: string
  href?: string
}

export interface AdminShellProps {
  title: string
  description?: string
  crumbs?: AdminCrumb[]
  children: React.ReactNode
}

export function AdminShell({
  title,
  description,
  crumbs,
  children,
}: AdminShellProps) {
  return (
    <main className="min-h-[calc(100vh-3.5rem)] bg-obsidian">
      {/* Bronze top band — visual marker that we're in the back office. */}
      <div className="border-b border-bronze/70 bg-iron/60">
        <div className="mx-auto max-w-6xl px-4 md:px-6 py-2.5 flex items-center gap-2 text-[11px] uppercase tracking-widest font-mono text-ember/90">
          <Link href="/admin" className="hover:text-ember-bright transition-colors">
            Admin
          </Link>
          <span className="text-ash/40">·</span>
          <span className="text-ash/80">Trusight</span>

          {crumbs && crumbs.length > 0 && (
            <>
              {crumbs.map((c, i) => (
                <span key={`${c.label}-${i}`} className="flex items-center gap-2">
                  <ChevronRight className="h-3 w-3 text-ash/40" aria-hidden />
                  {c.href ? (
                    <Link
                      href={c.href}
                      className="text-ash/80 hover:text-ember-bright transition-colors"
                    >
                      {c.label}
                    </Link>
                  ) : (
                    <span className="text-ash/80">{c.label}</span>
                  )}
                </span>
              ))}
            </>
          )}
        </div>
      </div>

      <div className="mx-auto max-w-6xl px-4 md:px-6 py-10 md:py-14 space-y-8">
        <header className="space-y-2">
          <h1
            className={cn(
              "font-display text-3xl md:text-4xl font-semibold text-parchment",
            )}
          >
            {title}
          </h1>
          {description && (
            <p className="font-sans text-sm text-ash max-w-2xl leading-relaxed">
              {description}
            </p>
          )}
        </header>

        {children}
      </div>
    </main>
  )
}
