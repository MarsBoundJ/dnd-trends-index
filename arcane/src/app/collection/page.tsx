"use client"

/*
 * Trusight — Bag of Holding viewer (Step 5 MVP).
 *
 * Client Component (all state lives in localStorage via Zustand). Renders
 * three sections:
 *
 *   1. Cards          — stowed data cards from any lens
 *   2. Sage Answers   — single Q+A pairs pinned from the Sage panel
 *   3. Sage Threads   — whole conversations pinned from the Sage panel
 *
 * Each stowed item has an "Unstow" button. No drag-reorder (Step 16 or
 * Atlas cleanup), no export (Step 15), no Firestore sync (Step 11). The
 * URL slug is `/collection` per §3.5 — "Bag of Holding" is the UI chrome
 * name, `/collection/` is the URL name, matching Apple Music's pattern.
 */

import Link from "next/link"
import { Bookmark, Trash2, Sparkles, MessageSquare } from "lucide-react"
import {
  useBagStore,
  useBagHasHydrated,
  type StowedCard,
  type StowedSageAnswer,
  type StowedSageThread,
} from "@/lib/bag-store"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { GenerateReportForm } from "@/components/generate-report-form"

// Format a stow timestamp as a short relative string. Intentionally simple —
// the polish pass in Step 16 can upgrade to Intl.RelativeTimeFormat.
function formatStowedAt(ts: number): string {
  const diffMs = Date.now() - ts
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 1) return "just now"
  if (mins < 60) return `${mins}m ago`
  const hours = Math.floor(mins / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export default function CollectionPage() {
  const hydrated = useBagHasHydrated()
  const items = useBagStore((s) => s.items)
  const unstow = useBagStore((s) => s.unstow)
  const clear = useBagStore((s) => s.clear)

  // Split by kind — sorted newest-first within each section.
  const cards = items
    .filter((i): i is StowedCard => i.kind === "card")
    .sort((a, b) => b.stowedAt - a.stowedAt)
  const answers = items
    .filter((i): i is StowedSageAnswer => i.kind === "sage-answer")
    .sort((a, b) => b.stowedAt - a.stowedAt)
  const threads = items
    .filter((i): i is StowedSageThread => i.kind === "sage-thread")
    .sort((a, b) => b.stowedAt - a.stowedAt)

  const totalCount = cards.length + answers.length + threads.length

  return (
    <main className="mx-auto max-w-4xl px-6 py-12 space-y-10">
      <header className="space-y-3">
        <p className="font-mono text-xs uppercase tracking-widest text-ember-bright">
          Trusight · Bag of Holding
        </p>
        <div className="flex items-end justify-between gap-4 flex-wrap">
          <h1 className="font-display text-4xl font-semibold tracking-tight text-parchment">
            Your stowed keepsakes
          </h1>
          {hydrated && totalCount > 0 && (
            <Button
              variant="ghost"
              size="sm"
              onClick={() => {
                if (confirm(`Empty the Bag of Holding? (${totalCount} items)`)) {
                  clear()
                }
              }}
              className="h-7 px-3 text-xs text-ash hover:text-rarity-copper hover:bg-iron"
            >
              Empty the bag
            </Button>
          )}
        </div>
        <p className="font-sans text-sm text-ash max-w-2xl">
          Cards and Sage answers you&rsquo;ve pinned for later. Everything here
          lives in your browser only for now — sign-in and cloud sync land in
          a later step.
        </p>
      </header>

      {/* Hydration gate: render nothing until Zustand has rehydrated so the
          SSR output and first client render agree. */}
      {!hydrated ? (
        <p className="font-mono text-xs text-ash">Opening the bag…</p>
      ) : totalCount === 0 ? (
        <EmptyState />
      ) : (
        <div className="space-y-12">
          {/*
           * Generate Report surface. Positioned above the stowed-item
           * sections so it's the first action-affordance users see once
           * they have items in the bag. Hidden when bag is empty (handled
           * by the outer ternary).
           */}
          <GenerateReportForm />

          {cards.length > 0 && (
            <Section
              title="Cards"
              icon={<Bookmark className="h-4 w-4" />}
              count={cards.length}
            >
              {cards.map((c) => (
                <CardRow key={c.id} item={c} onUnstow={() => unstow(c.id)} />
              ))}
            </Section>
          )}

          {answers.length > 0 && (
            <Section
              title="Sage Answers"
              icon={<Sparkles className="h-4 w-4" />}
              count={answers.length}
            >
              {answers.map((a) => (
                <SageAnswerRow
                  key={a.id}
                  item={a}
                  onUnstow={() => unstow(a.id)}
                />
              ))}
            </Section>
          )}

          {threads.length > 0 && (
            <Section
              title="Sage Conversations"
              icon={<MessageSquare className="h-4 w-4" />}
              count={threads.length}
            >
              {threads.map((t) => (
                <SageThreadRow
                  key={t.id}
                  item={t}
                  onUnstow={() => unstow(t.id)}
                />
              ))}
            </Section>
          )}
        </div>
      )}

      <footer className="border-t border-bronze pt-6 flex items-center justify-between flex-wrap gap-3">
        <p className="font-mono text-xs text-ash">
          FRONTEND_DESIGN_SPEC.md · §3.5 Bag of Holding · Step 5 of 16
        </p>
        <div className="flex gap-4">
          <Link
            href="/overview"
            className="font-mono text-xs text-ash hover:text-ember transition-colors"
          >
            ← overview
          </Link>
        </div>
      </footer>
    </main>
  )
}

// ─── Section wrapper ─────────────────────────────────────────────────────────

function Section({
  title,
  icon,
  count,
  children,
}: {
  title: string
  icon: React.ReactNode
  count: number
  children: React.ReactNode
}) {
  return (
    <section className="space-y-4">
      <div className="flex items-center gap-2 border-b border-bronze/40 pb-2">
        <span className="text-ember-bright">{icon}</span>
        <h2 className="font-display text-lg font-semibold text-parchment">
          {title}
        </h2>
        <span className="font-mono text-xs text-ash">{count}</span>
      </div>
      <div className="space-y-3">{children}</div>
    </section>
  )
}

// ─── Row components ──────────────────────────────────────────────────────────

function RowShell({
  children,
  onUnstow,
  stowedAt,
}: {
  children: React.ReactNode
  onUnstow: () => void
  stowedAt: number
}) {
  return (
    <article
      className={cn(
        "rounded-lg border border-bronze bg-onyx p-4",
        "transition-colors hover:border-ember/60"
      )}
    >
      <div className="flex items-start gap-4">
        <div className="flex-1 min-w-0 space-y-2">{children}</div>
        <div className="flex flex-col items-end gap-2 shrink-0">
          <span className="font-mono text-[10px] uppercase tracking-widest text-ash/60">
            {formatStowedAt(stowedAt)}
          </span>
          <Button
            variant="ghost"
            size="sm"
            onClick={onUnstow}
            aria-label="Remove from Bag of Holding"
            className="h-7 w-7 p-0 text-ash hover:text-rarity-copper hover:bg-iron"
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </article>
  )
}

function CardRow({
  item,
  onUnstow,
}: {
  item: StowedCard
  onUnstow: () => void
}) {
  return (
    <RowShell onUnstow={onUnstow} stowedAt={item.stowedAt}>
      <div>
        <h3 className="font-display text-base font-semibold text-parchment leading-tight">
          {item.title}
        </h3>
        {item.subtitle && (
          <p className="font-sans text-xs text-ash mt-0.5">{item.subtitle}</p>
        )}
      </div>
      {item.snapshot && (
        <pre className="font-mono text-[11px] leading-snug text-ash whitespace-pre-wrap">
          {item.snapshot}
        </pre>
      )}
      <p className="font-mono text-[10px] uppercase tracking-widest text-ash/60">
        confidence {item.confidence}% · {item.lens ?? "—"} · {item.cardType ?? "—"}
      </p>
    </RowShell>
  )
}

function SageAnswerRow({
  item,
  onUnstow,
}: {
  item: StowedSageAnswer
  onUnstow: () => void
}) {
  return (
    <RowShell onUnstow={onUnstow} stowedAt={item.stowedAt}>
      {item.question && (
        <div>
          <p className="font-mono text-[10px] uppercase tracking-widest text-ash/60">
            You asked
          </p>
          <p className="font-sans text-sm text-parchment mt-0.5 whitespace-pre-wrap">
            {item.question}
          </p>
        </div>
      )}
      <div>
        <p className="font-mono text-[10px] uppercase tracking-widest text-ember-bright">
          The Sage
        </p>
        <p className="font-sans text-sm text-parchment mt-0.5 whitespace-pre-wrap">
          {item.answer}
        </p>
      </div>
      {item.pageContext && (
        <p className="font-mono text-[10px] text-ash/50 truncate">
          from: {item.pageContext.split("\n")[0]}
        </p>
      )}
    </RowShell>
  )
}

function SageThreadRow({
  item,
  onUnstow,
}: {
  item: StowedSageThread
  onUnstow: () => void
}) {
  return (
    <RowShell onUnstow={onUnstow} stowedAt={item.stowedAt}>
      <div className="flex items-center gap-2">
        <p className="font-display text-sm font-semibold text-parchment">
          Conversation ({item.messages.length} messages)
        </p>
      </div>
      {item.pageContext && (
        <p className="font-mono text-[10px] text-ash/60 truncate">
          from: {item.pageContext.split("\n")[0]}
        </p>
      )}
      <details className="group">
        <summary className="font-mono text-[10px] uppercase tracking-widest text-ash/60 cursor-pointer hover:text-ember-bright list-none">
          <span className="group-open:hidden">Expand transcript ▾</span>
          <span className="hidden group-open:inline">Collapse transcript ▴</span>
        </summary>
        <div className="mt-3 space-y-3 border-l border-bronze/40 pl-3">
          {item.messages.map((m, i) => (
            <div key={i}>
              <p
                className={cn(
                  "font-mono text-[10px] uppercase tracking-widest",
                  m.role === "user" ? "text-ash/60" : "text-ember-bright"
                )}
              >
                {m.role === "user" ? "You" : "Sage"}
              </p>
              <p className="font-sans text-sm text-parchment mt-0.5 whitespace-pre-wrap">
                {m.text}
              </p>
            </div>
          ))}
        </div>
      </details>
    </RowShell>
  )
}

// ─── Empty state ─────────────────────────────────────────────────────────────

function EmptyState() {
  return (
    <div className="rounded-lg border border-dashed border-bronze bg-onyx/60 p-10 text-center space-y-3">
      <Bookmark className="h-8 w-8 text-ash/40 mx-auto" />
      <h2 className="font-display text-lg font-semibold text-parchment">
        Your bag is empty
      </h2>
      <p className="font-sans text-sm text-ash max-w-md mx-auto">
        Click <span className="font-mono text-ember-bright">Stow</span> on any
        card — or ask The Sage something and stow the answer — and it&rsquo;ll
        live here across sessions.
      </p>
      <div className="pt-2">
        <Link
          href="/overview"
          className="font-mono text-xs uppercase tracking-widest text-ember-bright hover:text-parchment transition-colors"
        >
          Go to the Overview lens →
        </Link>
      </div>
    </div>
  )
}
